use crate::sync_manager::config::LogSyncConfig;
use crate::sync_manager::data_cache::DataCache;
use crate::sync_manager::log_entry_fetcher::{LogEntryFetcher, LogFetchProgress};
use anyhow::{anyhow, bail, Result};
use ethereum_types::H256;
use ethers::{prelude::Middleware, types::BlockNumber};
use futures::FutureExt;
use jsonrpsee::tracing::{debug, error, trace, warn};
use shared_types::{ChunkArray, Transaction};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use storage::log_store::{tx_store::BlockHashAndSubmissionIndex, Store};
use task_executor::{ShutdownReason, TaskExecutor};
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{oneshot, RwLock};

const RETRY_WAIT_MS: u64 = 500;
const BROADCAST_CHANNEL_CAPACITY: usize = 16;
const CATCH_UP_END_GAP: u64 = 100;

#[derive(Clone, Debug)]
pub enum LogSyncEvent {
    /// Chain reorg detected without any operation yet.
    ReorgDetected { tx_seq: u64 },
    /// Transaction reverted in storage.
    Reverted { tx_seq: u64 },
    /// Synced a transaction from blockchain
    TxSynced { tx: Transaction },
}

pub struct LogSyncManager {
    config: LogSyncConfig,
    log_fetcher: LogEntryFetcher,
    store: Arc<dyn Store>,
    data_cache: DataCache,

    next_tx_seq: u64,

    /// To broadcast events to handle in advance.
    event_send: broadcast::Sender<LogSyncEvent>,

    block_hash_cache: Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
}

impl LogSyncManager {
    pub async fn spawn(
        config: LogSyncConfig,
        executor: TaskExecutor,
        store: Arc<dyn Store>,
    ) -> Result<(broadcast::Sender<LogSyncEvent>, oneshot::Receiver<()>)> {
        let next_tx_seq = store.next_tx_seq();

        let executor_clone = executor.clone();
        let mut shutdown_sender = executor.shutdown_sender();

        let (event_send, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        let event_send_cloned = event_send.clone();
        let (catch_up_end_sender, catch_up_end_receiver) = oneshot::channel();

        // Spawn the task to sync log entries from the blockchain.
        executor.spawn(
            run_and_log(
                move || {
                    shutdown_sender
                        .try_send(ShutdownReason::Failure("log sync failure"))
                        .expect("shutdown send error")
                },
                async move {
                    let log_fetcher = LogEntryFetcher::new(
                        &config.rpc_endpoint_url,
                        config.contract_address,
                        config.log_page_size,
                        config.confirmation_block_count,
                        config.rate_limit_retries,
                        config.timeout_retries,
                        config.initial_backoff,
                    )
                    .await?;
                    let data_cache = DataCache::new(config.cache_config.clone());

                    let block_hash_cache = Arc::new(RwLock::new(
                        store
                            .get_block_hashes()?
                            .into_iter()
                            .map(|(x, y)| (x, Some(y)))
                            .collect::<BTreeMap<_, _>>(),
                    ));
                    let mut log_sync_manager = Self {
                        config,
                        log_fetcher,
                        next_tx_seq,
                        store,
                        data_cache,
                        event_send,
                        block_hash_cache,
                    };

                    let (mut start_block_number, mut start_block_hash) =
                        match log_sync_manager.store.get_sync_progress()? {
                            // No previous progress, so just use config.
                            None => {
                                let block_number = log_sync_manager.config.start_block_number;
                                let block_hash =
                                    log_sync_manager.get_block(block_number.into()).await?.1;
                                (block_number, block_hash)
                            }
                            Some((block_number, block_hash)) => (block_number, block_hash),
                        };

                    let (mut finalized_block_number, mut finalized_block_hash) =
                        match log_sync_manager.get_block(BlockNumber::Finalized).await {
                            Ok(finalized) => finalized,
                            Err(e) => {
                                warn!(?e, "unable to get finalized block");
                                log_sync_manager.get_block(0.into()).await?
                            }
                        };

                    // Load previous progress from db and check if chain reorg happens after restart.
                    let mut need_handle_reorg = false;
                    if start_block_number <= finalized_block_number {
                        let expect_block_hash = log_sync_manager
                            .get_block(start_block_number.into())
                            .await?
                            .1;
                        if expect_block_hash != start_block_hash {
                            need_handle_reorg = true;
                        }
                    }
                    debug!(
                        "current start block number {}, block hash {}, finalized block number {}",
                        start_block_number, start_block_hash, finalized_block_number
                    );

                    if need_handle_reorg {
                        let reorg_rx = log_sync_manager.log_fetcher.handle_reorg(
                            start_block_number,
                            start_block_hash,
                            &executor_clone,
                            log_sync_manager.block_hash_cache.clone(),
                        );
                        log_sync_manager.handle_data(reorg_rx).await?;
                        if let Some((block_number, block_hash)) =
                            log_sync_manager.store.get_sync_progress()?
                        {
                            start_block_number = block_number;
                            start_block_hash = block_hash;
                        } else {
                            bail!("get log sync progress error");
                        }
                    }

                    // TODO(zz): Rate limit to avoid OOM during recovery.
                    if start_block_number >= finalized_block_number {
                        let block = log_sync_manager
                            .block_hash_cache
                            .read()
                            .await
                            .get(&start_block_number)
                            .cloned();
                        if let Some(b) = block {
                            // special case avoid reorg
                            if let Some(submission_idx) = b.as_ref().unwrap().first_submission_index
                            {
                                log_sync_manager.process_reverted(submission_idx).await;
                            }
                        }
                    }

                    let parent_block_hash = if start_block_number >= finalized_block_number {
                        // No need to catch up data.
                        if start_block_number > 0 {
                            let parent_block_number = start_block_number.saturating_sub(1);
                            match log_sync_manager
                                .block_hash_cache
                                .read()
                                .await
                                .get(&parent_block_number)
                            {
                                Some(b) => b.as_ref().unwrap().block_hash,
                                _ => {
                                    log_sync_manager
                                        .get_block(parent_block_number.into())
                                        .await?
                                        .1
                                }
                            }
                        } else {
                            start_block_hash
                        }
                    } else {
                        // Keep catching-up data until we are close to the latest height.
                        loop {
                            log_sync_manager
                                .catch_up_data(
                                    executor_clone.clone(),
                                    start_block_number,
                                    finalized_block_number,
                                )
                                .await?;
                            start_block_number = finalized_block_number.saturating_add(1);

                            let new_finalized_block =
                                log_sync_manager.get_block(BlockNumber::Finalized).await?;
                            if new_finalized_block.0.saturating_sub(finalized_block_number)
                                <= CATCH_UP_END_GAP
                            {
                                break finalized_block_hash;
                            }
                            finalized_block_number = new_finalized_block.0;
                            finalized_block_hash = new_finalized_block.1;
                        }
                    };

                    catch_up_end_sender
                        .send(())
                        .map_err(|e| anyhow!("catch up end send err, e={:?}", e))?;

                    let watch_rx = log_sync_manager.log_fetcher.start_watch(
                        start_block_number,
                        parent_block_hash,
                        &executor_clone,
                        log_sync_manager.block_hash_cache.clone(),
                        log_sync_manager.config.watch_loop_wait_time_ms,
                    );
                    // Syncing `watch_rx` is supposed to block forever.
                    log_sync_manager.handle_data(watch_rx).await?;
                    Ok::<(), anyhow::Error>(())
                },
            )
            .map(|_| ()),
            "log_sync",
        );
        Ok((event_send_cloned, catch_up_end_receiver))
    }

    async fn put_tx(&mut self, tx: Transaction) -> bool {
        // We call this after process chain reorg, so the sequence number should match.
        match tx.seq.cmp(&self.next_tx_seq) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => {
                debug!("log entry sync get entry: {:?}", tx);
                self.put_tx_inner(tx).await
            }
            std::cmp::Ordering::Greater => {
                error!(
                    "Unexpected transaction seq: next={} get={}",
                    self.next_tx_seq, tx.seq
                );
                false
            }
        }
    }

    /// `tx_seq` is the first reverted tx seq.
    async fn process_reverted(&mut self, tx_seq: u64) {
        warn!("revert for chain reorg: seq={}", tx_seq);
        {
            let store = self.store.clone();
            for seq in tx_seq..self.next_tx_seq {
                if matches!(store.check_tx_completed(seq), Ok(true)) {
                    if let Ok(Some(tx)) = store.get_tx_by_seq_number(seq) {
                        // TODO(zz): Skip reading the rear padding data?
                        if let Ok(Some(data)) =
                            store.get_chunks_by_tx_and_index_range(seq, 0, tx.num_entries())
                        {
                            if !self
                                .data_cache
                                .add_data(tx.data_merkle_root, seq, data.data)
                            {
                                // TODO(zz): Data too large. Save to disk?
                                warn!("large reverted data dropped for tx={:?}", tx);
                            }
                        }
                    }
                }
            }
        }

        let _ = self.event_send.send(LogSyncEvent::ReorgDetected { tx_seq });

        // TODO(zz): `wrapping_sub` here is a hack to handle the case of tx_seq=0.
        if let Err(e) = self.store.revert_to(tx_seq.wrapping_sub(1)) {
            error!("revert_to fails: e={:?}", e);
            return;
        }
        self.next_tx_seq = tx_seq;

        let _ = self.event_send.send(LogSyncEvent::Reverted { tx_seq });
    }

    async fn handle_data(&mut self, mut rx: UnboundedReceiver<LogFetchProgress>) -> Result<()> {
        while let Some(data) = rx.recv().await {
            trace!("handle_data: data={:?}", data);
            match data {
                LogFetchProgress::SyncedBlock((
                    block_number,
                    block_hash,
                    first_submission_index,
                )) => {
                    if first_submission_index.is_some() {
                        self.block_hash_cache.write().await.insert(
                            block_number,
                            Some(BlockHashAndSubmissionIndex {
                                block_hash,
                                first_submission_index: first_submission_index.unwrap(),
                            }),
                        );
                    }

                    self.store.put_sync_progress((
                        block_number,
                        block_hash,
                        first_submission_index,
                    ))?;

                    match self.log_fetcher.provider().get_block(block_number).await {
                        Ok(Some(b)) => {
                            if b.number != Some(block_number.into()) {
                                error!(
                                    "block number not match, reorg possible happened, block number {:?}, received {}", b.number, block_number 
                                );
                            } else if b.hash != Some(block_hash) {
                                error!("block hash not match, reorg possible happened, block hash {:?}, received {}", b.hash, block_hash);
                            }
                        }
                        e => {
                            error!("log put progress check rpc fails, e={:?}", e);
                        }
                    }
                }
                LogFetchProgress::Transaction(tx) => {
                    if !self.put_tx(tx.clone()).await {
                        // Unexpected error.
                        error!("log sync write error");
                        break;
                    }
                    if let Err(e) = self.event_send.send(LogSyncEvent::TxSynced { tx }) {
                        // TODO: Do we need to wait until all receivers are initialized?
                        // Auto-sync and txpool may need this event, but it's possible that
                        // no receivers will be created.
                        warn!("log sync broadcast error, error={:?}", e);
                    }
                }
                LogFetchProgress::Reverted(reverted) => {
                    self.process_reverted(reverted).await;
                }
            }
        }
        Ok(())
    }

    async fn put_tx_inner(&mut self, tx: Transaction) -> bool {
        if let Err(e) = self.store.put_tx(tx.clone()) {
            error!("put_tx error: e={:?}", e);
            false
        } else {
            if let Some(data) = self.data_cache.pop_data(&tx.data_merkle_root) {
                let store = self.store.clone();
                // We are holding a mutable reference of LogSyncManager, so no chain reorg is
                // possible after put_tx.
                if let Err(e) = store
                    .put_chunks_with_tx_hash(
                        tx.seq,
                        tx.hash(),
                        ChunkArray {
                            data,
                            start_index: 0,
                        },
                        None,
                    )
                    .and_then(|_| store.finalize_tx_with_hash(tx.seq, tx.hash()))
                {
                    error!("put_tx data error: e={:?}", e);
                    return false;
                }
            }
            self.data_cache.garbage_collect(self.next_tx_seq);
            self.next_tx_seq += 1;
            true
        }
    }

    async fn get_block(&self, block_number: BlockNumber) -> Result<(u64, H256)> {
        let block = match self.log_fetcher.provider().get_block(block_number).await {
            Ok(Some(block)) => block,
            Ok(None) => {
                bail!("None for block {}", block_number);
            }
            e => {
                bail!("unable to get block: {:?}", e);
            }
        };
        Ok((
            block
                .number
                .ok_or_else(|| anyhow!("None block number for finalized block"))?
                .as_u64(),
            block
                .hash
                .ok_or_else(|| anyhow!("None block hash for finalized block"))?,
        ))
    }

    /// Return the ending block number and the parent block hash.
    async fn catch_up_data(
        &mut self,
        executor_clone: TaskExecutor,
        start_block_number: u64,
        finalized_block_number: u64,
    ) -> Result<()> {
        if start_block_number < finalized_block_number {
            let recover_rx = self.log_fetcher.start_recover(
                start_block_number,
                finalized_block_number,
                &executor_clone,
                Duration::from_millis(self.config.recover_query_delay),
            );
            self.handle_data(recover_rx).await?;
        }

        self.log_fetcher.start_remove_finalized_block_task(
            &executor_clone,
            self.store.clone(),
            self.block_hash_cache.clone(),
            self.config.default_finalized_block_count,
            self.config.remove_finalized_block_interval_minutes,
        );
        Ok(())
    }
}

async fn run_and_log<R, E>(
    mut on_error: impl FnMut(),
    f: impl Future<Output = std::result::Result<R, E>> + Send,
) -> Option<R>
where
    E: Debug,
{
    match f.await {
        Err(e) => {
            error!("log sync failure: e={:?}", e);
            on_error();
            None
        }
        Ok(r) => Some(r),
    }
}

pub(crate) mod config;
mod data_cache;
mod log_entry_fetcher;
mod log_query;
