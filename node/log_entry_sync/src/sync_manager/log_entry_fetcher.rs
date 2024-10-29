use crate::sync_manager::log_query::LogQuery;
use crate::sync_manager::{metrics, RETRY_WAIT_MS};
use crate::{ContractAddress, LogSyncConfig};
use anyhow::{anyhow, bail, Result};
use append_merkle::{Algorithm, Sha3Algorithm};
use contract_interface::{SubmissionNode, SubmitFilter, ZgsFlow};
use ethers::abi::RawLog;
use ethers::prelude::{BlockNumber, EthLogDecode, Http, Middleware, Provider};
use ethers::providers::{HttpRateLimitRetryPolicy, RetryClient, RetryClientBuilder};
use ethers::types::{Block, Log, H256};
use futures::StreamExt;
use jsonrpsee::tracing::{debug, error, info, warn};
use shared_types::{DataRoot, Transaction};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::log_store::{tx_store::BlockHashAndSubmissionIndex, Store};
use task_executor::TaskExecutor;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    RwLock,
};

pub struct LogEntryFetcher {
    contract_address: ContractAddress,
    log_page_size: u64,
    provider: Arc<Provider<RetryClient<Http>>>,

    confirmation_delay: u64,
}

impl LogEntryFetcher {
    pub async fn new(config: &LogSyncConfig) -> Result<Self> {
        let provider = Arc::new(Provider::new(
            RetryClientBuilder::default()
                .rate_limit_retries(config.rate_limit_retries)
                .timeout_retries(config.timeout_retries)
                .initial_backoff(Duration::from_millis(config.initial_backoff))
                .build(
                    Http::new_with_client(
                        url::Url::parse(&config.rpc_endpoint_url)?,
                        reqwest::Client::builder()
                            .timeout(config.blockchain_rpc_timeout)
                            .connect_timeout(config.blockchain_rpc_timeout)
                            .build()?,
                    ),
                    Box::new(HttpRateLimitRetryPolicy),
                ),
        ));
        // TODO: `error` types are removed from the ABI json file.
        Ok(Self {
            contract_address: config.contract_address,
            provider,
            log_page_size: config.log_page_size,
            confirmation_delay: config.confirmation_block_count,
        })
    }

    pub fn handle_reorg(
        &self,
        block_number: u64,
        block_hash: H256,
        executor: &TaskExecutor,
        block_hash_cache: Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let (reorg_tx, reorg_rx) = tokio::sync::mpsc::unbounded_channel();
        let provider = self.provider.clone();

        executor.spawn(
            async move {
                let mut block_number = block_number;
                let mut block_hash = block_hash;

                debug!(
                    "handle_reorg starts, block number={} hash={}",
                    block_number, block_hash
                );

                loop {
                    match provider.get_block(block_number).await {
                        Ok(Some(b)) => {
                            if b.hash == Some(block_hash) {
                                break;
                            } else {
                                warn!(
                                    "log sync reorg check hash fails, \
                                            block_number={:?} expect={:?} get={:?}",
                                    block_number, block_hash, b.hash
                                );

                                match revert_one_block(
                                    block_hash,
                                    block_number,
                                    &reorg_tx,
                                    &block_hash_cache,
                                    provider.as_ref(),
                                )
                                .await
                                {
                                    Ok((parent_block_number, parent_block_hash)) => {
                                        block_number = parent_block_number;
                                        block_hash = parent_block_hash;
                                    }
                                    Err(e) => {
                                        error!("revert block fails, e={:?}", e);
                                    }
                                }
                            }
                        }
                        e => {
                            error!("handle reorg fails, e={:?}", e);
                        }
                    };
                }
            },
            "handle reorg",
        );

        reorg_rx
    }

    pub fn start_remove_finalized_block_task(
        &self,
        executor: &TaskExecutor,
        store: Arc<dyn Store>,
        block_hash_cache: Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
        default_finalized_block_count: u64,
        remove_finalized_block_interval_minutes: u64,
    ) {
        let provider = self.provider.clone();
        executor.spawn(
            async move {
                loop {
                    debug!("processing finalized block");

                    let processed_block_number = match store.get_sync_progress() {
                        Ok(Some((processed_block_number, _))) => Some(processed_block_number),
                        Ok(None) => None,
                        Err(e) => {
                            error!("get sync progress error: e={:?}", e);
                            None
                        }
                    };

                    let log_latest_block_number = match store.get_log_latest_block_number() {
                        Ok(Some(b)) => b,
                        Ok(None) => 0,
                        Err(e) => {
                            error!("get log latest block number error: e={:?}", e);
                            0
                        }
                    };

                    if let Some(processed_block_number) = processed_block_number {
                        let finalized_block_number =
                            match provider.get_block(BlockNumber::Finalized).await {
                                Ok(block) => match block {
                                    Some(b) => match b.number {
                                        Some(f) => Some(f.as_u64()),
                                        None => {
                                            error!("block number is none for finalized block");
                                            None
                                        }
                                    },
                                    None => {
                                        error!("finalized block is none");
                                        None
                                    }
                                },
                                Err(e) => {
                                    error!("get finalized block number: e={:?}", e);
                                    Some(processed_block_number - default_finalized_block_count)
                                }
                            };

                        if let Some(finalized_block_number) = finalized_block_number {
                            let safe_block_number = std::cmp::min(
                                std::cmp::min(
                                    log_latest_block_number.saturating_sub(1),
                                    finalized_block_number,
                                ),
                                processed_block_number,
                            );
                            let mut pending_keys = vec![];
                            for (key, _) in block_hash_cache.read().await.iter() {
                                if *key < safe_block_number {
                                    pending_keys.push(*key);
                                } else {
                                    break;
                                }
                            }

                            for key in pending_keys.into_iter() {
                                if let Err(e) = store.delete_block_hash_by_number(key) {
                                    error!("remove block tx for number {} error: e={:?}", key, e);
                                } else {
                                    block_hash_cache.write().await.remove(&key);
                                }
                            }
                        }
                    }

                    tokio::time::sleep(Duration::from_secs(
                        60 * remove_finalized_block_interval_minutes,
                    ))
                    .await;
                }
            },
            "handle reorg",
        );
    }

    pub fn start_recover(
        &self,
        start_block_number: u64,
        end_block_number: u64,
        executor: &TaskExecutor,
        log_query_delay: Duration,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let provider = self.provider.clone();
        let (recover_tx, recover_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = self.flow_contract();
        let log_page_size = self.log_page_size;

        executor.spawn(
            async move {
                let mut progress = start_block_number;
                let mut filter = contract
                    .submit_filter()
                    .from_block(progress)
                    .to_block(end_block_number)
                    .address(contract.address().into())
                    .filter;
                let mut stream = LogQuery::new(&provider, &filter, log_query_delay)
                    .with_page_size(log_page_size);
                info!(
                    "start_recover starts, start={} end={}",
                    start_block_number, end_block_number
                );
                let (mut block_hash_sent, mut block_number_sent) = (None, None);
                while let Some(maybe_log) = stream.next().await {
                    let start_time = Instant::now();
                    match maybe_log {
                        Ok(log) => {
                            let sync_progress =
                                if log.block_hash.is_some() && log.block_number.is_some() {
                                    if block_hash_sent != log.block_hash
                                        || block_number_sent != log.block_number
                                    {
                                        let synced_block = LogFetchProgress::SyncedBlock((
                                            log.block_number.unwrap().as_u64(),
                                            log.block_hash.unwrap(),
                                            None,
                                        ));
                                        progress = log.block_number.unwrap().as_u64();
                                        Some(synced_block)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                            debug!("recover: progress={:?}", sync_progress);

                            match SubmitFilter::decode_log(&RawLog {
                                topics: log.topics,
                                data: log.data.to_vec(),
                            }) {
                                Ok(event) => {
                                    if let Err(e) = recover_tx
                                        .send(submission_event_to_transaction(
                                            event,
                                            log.block_number.expect("block number exist").as_u64(),
                                        ))
                                        .and_then(|_| match sync_progress {
                                            Some(b) => {
                                                recover_tx.send(b)?;
                                                block_hash_sent = log.block_hash;
                                                block_number_sent = log.block_number;
                                                Ok(())
                                            }
                                            None => Ok(()),
                                        })
                                    {
                                        error!("send error: e={:?}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("log decode error: e={:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("log query error: e={:?}", e);
                            filter = filter.from_block(progress).address(contract.address());
                            stream = LogQuery::new(&provider, &filter, log_query_delay)
                                .with_page_size(log_page_size);
                            tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                        }
                    }
                    metrics::RECOVER_LOG.update_since(start_time);
                }

                info!("log recover end");
            },
            "log recover",
        );
        recover_rx
    }

    pub fn start_watch(
        &self,
        start_block_number: u64,
        parent_block_hash: H256,
        executor: &TaskExecutor,
        block_hash_cache: Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
        watch_loop_wait_time_ms: u64,
        mut watch_progress_rx: UnboundedReceiver<u64>,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let (watch_tx, watch_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = self.flow_contract();
        let provider = self.provider.clone();
        let confirmation_delay = self.confirmation_delay;
        let log_page_size = self.log_page_size;
        let mut progress_reset_history = BTreeMap::new();
        executor.spawn(
            async move {
                debug!("start_watch starts, start={}", start_block_number);
                let mut progress = start_block_number;
                let mut parent_block_hash = parent_block_hash;

                loop {
                    check_watch_process(
                        &mut watch_progress_rx,
                        &mut progress,
                        &mut parent_block_hash,
                        &mut progress_reset_history,
                        watch_loop_wait_time_ms,
                        &block_hash_cache,
                        provider.as_ref(),
                    )
                    .await;

                    match Self::watch_loop(
                        provider.as_ref(),
                        progress,
                        parent_block_hash,
                        &watch_tx,
                        confirmation_delay,
                        &contract,
                        &block_hash_cache,
                        log_page_size,
                    )
                    .await
                    {
                        Err(e) => {
                            error!("log sync watch error: e={:?}", e);
                        }
                        Ok(Some((p, h, _))) => {
                            progress = p.saturating_add(1);
                            parent_block_hash = h;
                            info!("log sync to block number {:?}", progress);
                        }
                        Ok(None) => {
                            debug!(
                                "log sync gets entries without progress? old_progress={}",
                                progress
                            )
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(watch_loop_wait_time_ms)).await;
                }
            },
            "log watch",
        );
        watch_rx
    }

    #[allow(clippy::too_many_arguments)]
    async fn watch_loop(
        provider: &Provider<RetryClient<Http>>,
        from_block_number: u64,
        parent_block_hash: H256,
        watch_tx: &UnboundedSender<LogFetchProgress>,
        confirmation_delay: u64,
        contract: &ZgsFlow<Provider<RetryClient<Http>>>,
        block_hash_cache: &Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
        log_page_size: u64,
    ) -> Result<Option<(u64, H256, Option<Option<u64>>)>> {
        let latest_block_number = provider.get_block_number().await?.as_u64();
        debug!(
            "from block number {}, latest block number {}, confirmation delay {}",
            from_block_number, latest_block_number, confirmation_delay
        );
        let to_block_number = latest_block_number.saturating_sub(confirmation_delay);
        if from_block_number > to_block_number {
            return Ok(None);
        }

        let block = provider
            .get_block_with_txs(from_block_number)
            .await?
            .ok_or_else(|| anyhow!("None for block {}", from_block_number))?;
        if Some(from_block_number.into()) != block.number {
            bail!(
                "block number mismatch, expected {}, actual {:?}",
                from_block_number,
                block.number
            );
        }

        if block.logs_bloom.is_none() {
            bail!("block {:?} logs bloom is none", block.number);
        }

        if from_block_number > 0 && block.parent_hash != parent_block_hash {
            // reorg happened
            let (parent_block_number, block_hash) = revert_one_block(
                parent_block_hash,
                from_block_number.saturating_sub(1),
                watch_tx,
                block_hash_cache,
                provider,
            )
            .await?;
            return Ok(Some((parent_block_number, block_hash, None)));
        }

        let mut blocks: HashMap<u64, Block<ethers::types::Transaction>> = Default::default();
        let mut parent_block_hash = block.hash;
        blocks.insert(from_block_number, block);
        for block_number in from_block_number + 1..to_block_number + 1 {
            let block = provider
                .get_block_with_txs(block_number)
                .await?
                .ok_or_else(|| anyhow!("None for block {}", block_number))?;
            if Some(block_number.into()) != block.number {
                bail!(
                    "block number mismatch, expected {}, actual {:?}",
                    block_number,
                    block.number
                );
            }
            if parent_block_hash.is_none() || Some(block.parent_hash) != parent_block_hash {
                bail!(
                    "parent block hash mismatch, expected {:?}, actual {}",
                    parent_block_hash,
                    block.parent_hash
                );
            }

            if block_number == to_block_number && block.hash.is_none() {
                bail!("block {:?} hash is none", block.number);
            }

            if block.logs_bloom.is_none() {
                bail!("block {:?} logs bloom is none", block.number);
            }

            parent_block_hash = block.hash;
            blocks.insert(block_number, block);
        }

        let filter = contract
            .submit_filter()
            .from_block(from_block_number)
            .to_block(to_block_number)
            .address(contract.address().into())
            .filter;
        let mut stream = LogQuery::new(provider, &filter, Duration::from_millis(10))
            .with_page_size(log_page_size);
        let mut block_logs: BTreeMap<u64, Vec<Log>> = BTreeMap::new();
        while let Some(maybe_log) = stream.next().await {
            let log = maybe_log?;
            let block_number = log
                .block_number
                .ok_or_else(|| anyhow!("block number missing"))?
                .as_u64();
            block_logs.entry(block_number).or_default().push(log);
        }

        let mut progress = None;
        for block_number in from_block_number..to_block_number + 1 {
            if let Some(block) = blocks.remove(&block_number) {
                let txs_hm = block
                    .transactions
                    .iter()
                    .map(|tx| (tx.transaction_index, tx))
                    .collect::<HashMap<_, _>>();
                let mut log_events = vec![];
                let mut first_submission_index = None;

                if let Some(logs) = block_logs.remove(&block_number) {
                    for log in logs.into_iter() {
                        if log.block_hash != block.hash {
                            warn!(
                                "log block hash mismatch, log block hash {:?}, block hash {:?}",
                                log.block_hash, block.hash
                            );
                            return Ok(progress);
                        }
                        if log.block_number != block.number {
                            warn!(
                                "log block num mismatch, log block number {:?}, block number {:?}",
                                log.block_number, block.number
                            );
                            return Ok(progress);
                        }

                        let tx = txs_hm[&log.transaction_index];
                        if log.transaction_hash.is_none() || log.transaction_hash != Some(tx.hash) {
                            warn!(
                            "log tx hash mismatch, log transaction {:?}, block transaction {:?}",
                            log.transaction_hash,
                            tx.hash
                        );
                            return Ok(progress);
                        }
                        if log.transaction_index.is_none()
                            || log.transaction_index != tx.transaction_index
                        {
                            warn!(
                            "log tx index mismatch, log tx index {:?}, block transaction index {:?}",
                            log.transaction_index,
                            tx.transaction_index
                        );
                            return Ok(progress);
                        }

                        let submit_filter = match SubmitFilter::decode_log(&RawLog {
                            topics: log.topics,
                            data: log.data.to_vec(),
                        }) {
                            Ok(v) => v,
                            Err(e) => {
                                return {
                                    warn!("decode log failed: {:?}", e);
                                    Ok(progress)
                                }
                            }
                        };

                        if first_submission_index.is_none()
                            || first_submission_index
                                > Some(submit_filter.submission_index.as_u64())
                        {
                            first_submission_index = Some(submit_filter.submission_index.as_u64());
                        }

                        log_events
                            .push(submission_event_to_transaction(submit_filter, block_number));
                    }

                    info!("synced {} events", log_events.len());
                }

                let new_progress = if block.hash.is_some() && block.number.is_some() {
                    Some((
                        block.number.unwrap().as_u64(),
                        block.hash.unwrap(),
                        Some(first_submission_index),
                    ))
                } else {
                    None
                };
                for log in log_events.into_iter() {
                    if let Err(e) = watch_tx.send(log) {
                        warn!("send LogFetchProgress::Transaction failed: {:?}", e);
                        return Ok(progress);
                    }
                }

                if let Some(p) = &new_progress {
                    if let Err(e) = watch_tx.send(LogFetchProgress::SyncedBlock(*p)) {
                        warn!("send LogFetchProgress::SyncedBlock failed: {:?}", e);
                        return Ok(progress);
                    } else {
                        let mut cache = block_hash_cache.write().await;
                        match cache.get(&p.0) {
                            Some(Some(v))
                                if v.block_hash == p.1
                                    && v.first_submission_index == p.2.unwrap() => {}
                            _ => {
                                cache.insert(p.0, None);
                            }
                        }
                    }
                }
                progress = new_progress;
            }
        }

        Ok(progress)
    }

    pub fn provider(&self) -> &Provider<RetryClient<Http>> {
        self.provider.as_ref()
    }

    pub fn flow_contract(&self) -> ZgsFlow<Provider<RetryClient<Http>>> {
        ZgsFlow::new(self.contract_address, self.provider.clone())
    }
}

async fn check_watch_process(
    watch_progress_rx: &mut UnboundedReceiver<u64>,
    progress: &mut u64,
    parent_block_hash: &mut H256,
    progress_reset_history: &mut BTreeMap<u64, (Instant, usize)>,
    watch_loop_wait_time_ms: u64,
    block_hash_cache: &Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
    provider: &Provider<RetryClient<Http>>,
) {
    let mut min_received_progress = None;
    while let Ok(v) = watch_progress_rx.try_recv() {
        min_received_progress = match min_received_progress {
            Some(min) if min > v => Some(v),
            None => Some(v),
            _ => min_received_progress,
        };
    }

    let mut reset = false;
    if let Some(v) = min_received_progress {
        if *progress <= v {
            error!(
                "received unexpected progress, current {}, received {}",
                *progress, v
            );
            return;
        }

        let now = Instant::now();
        match progress_reset_history.get_mut(&v) {
            Some((last_update, counter)) => {
                if *counter >= 3 {
                    error!("maximum reset attempts have been reached.");
                    watch_progress_rx.close();
                    return;
                }

                if now.duration_since(*last_update)
                    >= Duration::from_millis(watch_loop_wait_time_ms * 30)
                {
                    info!("reset to progress from {} to {}", *progress, v);
                    *progress = v;
                    *last_update = now;
                    *counter += 1;
                    reset = true;
                }
            }
            None => {
                info!("reset to progress from {} to {}", *progress, v);
                *progress = v;
                progress_reset_history.insert(v, (now, 1usize));
                reset = true;
            }
        }
    }

    if reset {
        *parent_block_hash = loop {
            if let Some(block) = block_hash_cache.read().await.get(&(*progress - 1)) {
                if let Some(v) = block {
                    break v.block_hash;
                } else {
                    debug!(
                        "block_hash_cache wait for SyncedBlock processed for {}",
                        *progress - 1
                    );
                    tokio::time::sleep(Duration::from_secs(RETRY_WAIT_MS)).await;
                }
            } else {
                warn!(
                    "get block hash for block {} from RPC, assume there is no org",
                    *progress - 1
                );
                let hash = loop {
                    match provider.get_block(*progress - 1).await {
                        Ok(Some(v)) => {
                            break v.hash.expect("parent block hash expect exist");
                        }
                        Ok(None) => {
                            panic!("parent block {} expect exist", *progress - 1);
                        }
                        Err(e) => {
                            if e.to_string().contains("server is too busy") {
                                warn!("server busy, wait for parent block {}", *progress - 1);
                            } else {
                                panic!("parent block {} expect exist, error {}", *progress - 1, e);
                            }
                        }
                    }
                };
                break hash;
            }
        };
    }

    progress_reset_history.retain(|k, _| k + 1000 >= *progress);
}

async fn revert_one_block(
    block_hash: H256,
    block_number: u64,
    watch_tx: &UnboundedSender<LogFetchProgress>,
    block_hash_cache: &Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
    provider: &Provider<RetryClient<Http>>,
) -> Result<(u64, H256), anyhow::Error> {
    debug!("revert block {}, block hash {:?}", block_number, block_hash);
    let block = loop {
        if let Some(block) = block_hash_cache.read().await.get(&block_number) {
            if let Some(v) = block {
                break v.clone();
            } else {
                debug!(
                    "block_hash_cache wait for SyncedBlock processed for {}",
                    block_number
                );
                tokio::time::sleep(Duration::from_secs(RETRY_WAIT_MS)).await;
            }
        } else {
            return Err(anyhow!("None for block {}", block_number));
        }
    };

    assert!(block_hash == block.block_hash);
    if let Some(reverted) = block.first_submission_index {
        watch_tx.send(LogFetchProgress::Reverted(reverted))?;
    }

    let parent_block_number = block_number.saturating_sub(1);
    let parent_block_hash = match block_hash_cache.read().await.get(&parent_block_number) {
        Some(v) => v.clone().as_ref().unwrap().block_hash,
        _ => {
            debug!("assume parent block {} is not reorged", parent_block_number);
            provider
                .get_block(parent_block_number)
                .await?
                .ok_or_else(|| anyhow!("None for block {}", parent_block_number))?
                .hash
                .ok_or_else(|| anyhow!("None block hash for block {}", parent_block_number))?
        }
    };

    let synced_block =
        LogFetchProgress::SyncedBlock((parent_block_number, parent_block_hash, None));
    watch_tx.send(synced_block)?;

    Ok((parent_block_number, parent_block_hash))
}

#[derive(Debug)]
pub enum LogFetchProgress {
    SyncedBlock((u64, H256, Option<Option<u64>>)),
    Transaction((Transaction, u64)),
    Reverted(u64),
}

fn submission_event_to_transaction(e: SubmitFilter, block_number: u64) -> LogFetchProgress {
    LogFetchProgress::Transaction((
        Transaction {
            stream_ids: vec![],
            data: vec![],
            data_merkle_root: nodes_to_root(&e.submission.nodes),
            merkle_nodes: e
                .submission
                .nodes
                .iter()
                // the submission height is the height of the root node starting from height 0.
                .map(|SubmissionNode { root, height }| (height.as_usize() + 1, root.into()))
                .collect(),
            start_entry_index: e.start_pos.as_u64(),
            size: e.submission.length.as_u64(),
            seq: e.submission_index.as_u64(),
        },
        block_number,
    ))
}

fn nodes_to_root(node_list: &[SubmissionNode]) -> DataRoot {
    let mut root: DataRoot = node_list.last().expect("not empty").root.into();
    for next_node in node_list[..node_list.len() - 1].iter().rev() {
        root = Sha3Algorithm::parent(&next_node.root.into(), &root);
    }
    root
}
