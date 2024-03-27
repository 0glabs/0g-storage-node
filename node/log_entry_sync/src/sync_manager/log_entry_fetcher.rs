use crate::rpc_proxy::ContractAddress;
use crate::sync_manager::log_query::LogQuery;
use crate::sync_manager::RETRY_WAIT_MS;
use anyhow::{anyhow, bail, Result};
use append_merkle::{Algorithm, Sha3Algorithm};
use contract_interface::{SubmissionNode, SubmitFilter, ZgsFlow};
use ethers::abi::RawLog;
use ethers::prelude::{BlockNumber, EthLogDecode, Http, Middleware, Provider};
use ethers::providers::{HttpRateLimitRetryPolicy, RetryClient, RetryClientBuilder};
use ethers::types::H256;
use futures::StreamExt;
use jsonrpsee::tracing::{debug, error, info, warn};
use shared_types::{DataRoot, Transaction};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
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
    pub async fn new(
        url: &str,
        contract_address: ContractAddress,
        log_page_size: u64,
        confirmation_delay: u64,
        rate_limit_retries: u32,
        timeout_retries: u32,
        initial_backoff: u64,
    ) -> Result<Self> {
        let provider = Arc::new(Provider::new(
            RetryClientBuilder::default()
                .rate_limit_retries(rate_limit_retries)
                .timeout_retries(timeout_retries)
                .initial_backoff(Duration::from_millis(initial_backoff))
                .build(Http::from_str(url)?, Box::new(HttpRateLimitRetryPolicy)),
        ));
        // TODO: `error` types are removed from the ABI json file.
        Ok(Self {
            contract_address,
            provider,
            log_page_size,
            confirmation_delay,
        })
    }

    pub fn handle_reorg(
        &self,
        block_number: u64,
        block_hash: H256,
        executor: &TaskExecutor,
        block_hash_cache: Arc<RwLock<BTreeMap<u64, BlockHashAndSubmissionIndex>>>,
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
        store: Arc<RwLock<dyn Store>>,
        block_hash_cache: Arc<RwLock<BTreeMap<u64, BlockHashAndSubmissionIndex>>>,
        default_finalized_block_count: u64,
        remove_finalized_block_interval_minutes: u64,
    ) {
        let provider = self.provider.clone();
        executor.spawn(
            async move {
                loop {
                    debug!("processing finalized block");

                    let processed_block_number = match store.read().await.get_sync_progress() {
                        Ok(Some((processed_block_number, _))) => Some(processed_block_number),
                        Ok(None) => None,
                        Err(e) => {
                            error!("get sync progress error: e={:?}", e);
                            None
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
                            if processed_block_number >= finalized_block_number {
                                let mut pending_keys = vec![];
                                for (key, _) in block_hash_cache.read().await.iter() {
                                    if *key <= finalized_block_number {
                                        pending_keys.push(*key);
                                    } else {
                                        break;
                                    }
                                }

                                for key in pending_keys.into_iter() {
                                    if let Err(e) =
                                        store.write().await.delete_block_hash_by_number(key)
                                    {
                                        error!(
                                            "remove block tx for number {} error: e={:?}",
                                            key, e
                                        );
                                    } else {
                                        block_hash_cache.write().await.remove(&key);
                                    }
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
        let contract = ZgsFlow::new(self.contract_address, provider.clone());
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
                debug!(
                    "start_recover starts, start={} end={}",
                    start_block_number, end_block_number
                );
                while let Some(maybe_log) = stream.next().await {
                    match maybe_log {
                        Ok(log) => {
                            let sync_progress =
                                if log.block_hash.is_some() && log.block_number.is_some() {
                                    let synced_block = LogFetchProgress::SyncedBlock((
                                        log.block_number.unwrap().as_u64(),
                                        log.block_hash.unwrap(),
                                        None,
                                    ));
                                    progress = log.block_number.unwrap().as_u64();
                                    Some(synced_block)
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
                                        .send(submission_event_to_transaction(event))
                                        .and_then(|_| match sync_progress {
                                            Some(b) => recover_tx.send(b),
                                            None => Ok(()),
                                        })
                                    {
                                        error!("send error: e={:?}", e);
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
                }
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
        block_hash_cache: Arc<RwLock<BTreeMap<u64, BlockHashAndSubmissionIndex>>>,
        watch_loop_wait_time_ms: u64,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let (watch_tx, watch_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = ZgsFlow::new(self.contract_address, self.provider.clone());
        let provider = self.provider.clone();
        let confirmation_delay = self.confirmation_delay;
        executor.spawn(
            async move {
                debug!("start_watch starts, start={}", start_block_number);
                let mut progress = start_block_number;
                let mut parent_block_hash = parent_block_hash;

                loop {
                    match Self::watch_loop(
                        provider.as_ref(),
                        progress,
                        parent_block_hash,
                        &watch_tx,
                        confirmation_delay,
                        &contract,
                        &block_hash_cache,
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
                            error!(
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
        block_number: u64,
        parent_block_hash: H256,
        watch_tx: &UnboundedSender<LogFetchProgress>,
        confirmation_delay: u64,
        contract: &ZgsFlow<Provider<RetryClient<Http>>>,
        block_hash_cache: &Arc<RwLock<BTreeMap<u64, BlockHashAndSubmissionIndex>>>,
    ) -> Result<Option<(u64, H256, Option<Option<u64>>)>> {
        let latest_block_number = provider.get_block_number().await?.as_u64();
        debug!(
            "block number {}, latest block number {}, confirmation_delay {}",
            block_number, latest_block_number, confirmation_delay
        );
        if block_number > latest_block_number.saturating_sub(confirmation_delay) {
            return Ok(None);
        }

        let block = provider
            .get_block_with_txs(block_number)
            .await?
            .ok_or_else(|| anyhow!("None for block {}", block_number))?;
        if block_number > 0 && block.parent_hash != parent_block_hash {
            // reorg happened
            let (parent_block_number, block_hash) = revert_one_block(
                parent_block_hash,
                block_number.saturating_sub(1),
                watch_tx,
                block_hash_cache,
            )
            .await?;
            return Ok(Some((parent_block_number, block_hash, None)));
        }

        let txs_hm = block
            .transactions
            .iter()
            .map(|tx| (tx.transaction_index, tx))
            .collect::<HashMap<_, _>>();

        let filter = contract
            .submit_filter()
            .from_block(block_number)
            .to_block(block_number)
            .address(contract.address().into())
            .filter;
        let mut logs = vec![];
        let mut first_submission_index = None;
        for log in provider.get_logs(&filter).await? {
            if log.block_hash != block.hash {
                bail!(
                    "log block hash mismatch, log block hash {:?}, block hash {:?}",
                    log.block_hash,
                    block.hash
                );
            }
            if log.block_number != block.number {
                bail!(
                    "log block num mismatch, log block number {:?}, block number {:?}",
                    log.block_number,
                    block.number
                );
            }

            let tx = txs_hm[&log.transaction_index];
            if log.transaction_hash != Some(tx.hash) {
                bail!(
                    "log tx hash mismatch, log transaction {:?}, block transaction {:?}",
                    log.transaction_hash,
                    tx.hash
                );
            }
            if log.transaction_index != tx.transaction_index {
                bail!(
                    "log tx index mismatch, log tx index {:?}, block transaction index {:?}",
                    log.transaction_index,
                    tx.transaction_index
                );
            }

            let tx = SubmitFilter::decode_log(&RawLog {
                topics: log.topics,
                data: log.data.to_vec(),
            })?;

            if first_submission_index.is_none()
                || first_submission_index > Some(tx.submission_index.as_u64())
            {
                first_submission_index = Some(tx.submission_index.as_u64());
            }

            logs.push(submission_event_to_transaction(tx));
        }

        let progress = if block.hash.is_some() && block.number.is_some() {
            Some((
                block.number.unwrap().as_u64(),
                block.hash.unwrap(),
                Some(first_submission_index),
            ))
        } else {
            None
        };
        if let Some(p) = &progress {
            watch_tx.send(LogFetchProgress::SyncedBlock(*p))?;
        }

        for log in logs.into_iter() {
            watch_tx.send(log)?;
        }

        Ok(progress)
    }

    pub fn provider(&self) -> &Provider<RetryClient<Http>> {
        self.provider.as_ref()
    }
}

async fn revert_one_block(
    block_hash: H256,
    block_number: u64,
    watch_tx: &UnboundedSender<LogFetchProgress>,
    block_hash_cache: &Arc<RwLock<BTreeMap<u64, BlockHashAndSubmissionIndex>>>,
) -> Result<(u64, H256), anyhow::Error> {
    debug!("revert block {}, block hash {:?}", block_number, block_hash);
    let block = block_hash_cache
        .read()
        .await
        .get(&block_number)
        .ok_or_else(|| anyhow!("None for block {}", block_number))?
        .clone();

    assert!(block_hash == block.block_hash);
    if let Some(reverted) = block.first_submission_index {
        watch_tx.send(LogFetchProgress::Reverted(reverted))?;
    }

    let parent_block_number = block_number.saturating_sub(1);
    let parent_block_hash = block_hash_cache
        .read()
        .await
        .get(&parent_block_number)
        .ok_or_else(|| anyhow!("None for block {}", parent_block_number))?
        .clone()
        .block_hash;

    let synced_block =
        LogFetchProgress::SyncedBlock((parent_block_number, parent_block_hash, None));
    watch_tx.send(synced_block)?;

    Ok((parent_block_number, parent_block_hash))
}

#[derive(Debug)]
pub enum LogFetchProgress {
    SyncedBlock((u64, H256, Option<Option<u64>>)),
    Transaction(Transaction),
    Reverted(u64),
}

fn submission_event_to_transaction(e: SubmitFilter) -> LogFetchProgress {
    LogFetchProgress::Transaction(Transaction {
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
    })
}

fn nodes_to_root(node_list: &Vec<SubmissionNode>) -> DataRoot {
    let mut root: DataRoot = node_list.last().expect("not empty").root.into();
    for next_node in node_list[..node_list.len() - 1].iter().rev() {
        root = Sha3Algorithm::parent(&next_node.root.into(), &root);
    }
    root
}
