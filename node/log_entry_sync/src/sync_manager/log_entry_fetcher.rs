use crate::rpc_proxy::ContractAddress;
use crate::sync_manager::log_query::LogQuery;
use crate::sync_manager::{repeat_run_and_log, RETRY_WAIT_MS};
use anyhow::{anyhow, bail, Result};
use append_merkle::{Algorithm, Sha3Algorithm};
use contract_interface::{ZgsFlow, SubmissionNode, SubmitFilter};
use ethers::abi::RawLog;
use ethers::prelude::{BlockNumber, EthLogDecode, Http, Log, Middleware, Provider, U256};
use ethers::providers::{FilterKind, HttpRateLimitRetryPolicy, RetryClient, RetryClientBuilder};
use ethers::types::H256;
use futures::StreamExt;
use jsonrpsee::tracing::{debug, error, info};
use shared_types::{DataRoot, Transaction};
use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use task_executor::TaskExecutor;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

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
                            filter = filter.from_block(progress);
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
        executor: &TaskExecutor,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let (watch_tx, watch_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = ZgsFlow::new(self.contract_address, self.provider.clone());
        let provider = self.provider.clone();
        let mut log_confirmation_queue = LogConfirmationQueue::new(self.confirmation_delay);
        executor.spawn(
            async move {
                let mut filter = contract
                    .submit_filter()
                    .from_block(start_block_number)
                    .address(contract.address().into())
                    .filter;
                debug!("start_watch starts, start={}", start_block_number);
                let mut filter_id =
                    repeat_run_and_log(|| provider.new_filter(FilterKind::Logs(&filter))).await;
                let mut progress = start_block_number;

                loop {
                    match Self::watch_loop(
                        provider.as_ref(),
                        filter_id,
                        &watch_tx,
                        &mut log_confirmation_queue,
                    )
                    .await
                    {
                        Err(e) => {
                            error!("log sync watch error: e={:?}", e);
                            filter = filter.from_block(progress).address(contract.address());
                            filter_id = repeat_run_and_log(|| {
                                provider.new_filter(FilterKind::Logs(&filter))
                            })
                            .await;
                        }
                        Ok(Some(p)) => {
                            progress = p;
                            info!("log sync to block number {:?}", progress);
                        }
                        Ok(None) => {
                            error!(
                                "log sync gets entries without progress? old_progress={}",
                                progress
                            )
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
            },
            "log watch",
        );
        watch_rx
    }

    async fn watch_loop(
        provider: &Provider<RetryClient<Http>>,
        filter_id: U256,
        watch_tx: &UnboundedSender<LogFetchProgress>,
        log_confirmation_queue: &mut LogConfirmationQueue,
    ) -> Result<Option<u64>> {
        debug!("get block");
        let latest_block = provider
            .get_block(BlockNumber::Latest)
            .await?
            .ok_or_else(|| anyhow!("None for latest block"))?;
        debug!("get filter changes");
        let logs: Vec<Log> = provider.get_filter_changes(filter_id).await?;
        if let Some(reverted) = log_confirmation_queue.push(logs)? {
            watch_tx.send(LogFetchProgress::Reverted(reverted))?;
        }
        debug!("get filter end");
        for log in log_confirmation_queue.confirm_logs(latest_block.number.unwrap().as_u64()) {
            assert!(!log.removed.unwrap_or(false));
            // TODO(zz): Log parse error means logs might be lost here.
            let tx = SubmitFilter::decode_log(&RawLog {
                topics: log.topics,
                data: log.data.to_vec(),
            })?;
            watch_tx.send(submission_event_to_transaction(tx))?;
        }
        let progress = if latest_block.hash.is_some() && latest_block.number.is_some() {
            Some((
                latest_block.number.unwrap().as_u64(),
                latest_block.hash.unwrap(),
            ))
        } else {
            None
        };
        if let Some(p) = &progress {
            watch_tx.send(LogFetchProgress::SyncedBlock(*p))?;
        }
        Ok(progress.map(|p| p.0))
    }

    pub fn provider(&self) -> &Provider<RetryClient<Http>> {
        self.provider.as_ref()
    }
}

struct LogConfirmationQueue {
    /// Keep the unconfirmed new logs.
    /// The key is the block number and the value is the set of needed logs in that block.
    queue: VecDeque<(u64, Vec<Log>)>,

    latest_block_number: u64,
    confirmation_delay: u64,
}

impl LogConfirmationQueue {
    fn new(confirmation_delay: u64) -> Self {
        Self {
            queue: VecDeque::new(),
            latest_block_number: 0,
            confirmation_delay,
        }
    }
    /// Push a set of new logs.
    /// We assumes that these logs are in order, and removed logs are returned first.
    ///
    /// Return `Ok(Some(tx_seq))` of the first reverted tx_seq if chain reorg happens.
    /// `Err` is returned if assumptions are violated (like the log have missing fields).
    fn push(&mut self, logs: Vec<Log>) -> Result<Option<u64>> {
        let mut revert_to = None;
        // First merge logs according to the block number.
        let mut block_logs: BTreeMap<u64, Vec<Log>> = BTreeMap::new();
        let mut removed_block_logs = BTreeMap::new();
        for log in logs {
            let set = if log.removed.unwrap_or(false) {
                &mut removed_block_logs
            } else {
                &mut block_logs
            };
            let block_number = log
                .block_number
                .ok_or_else(|| anyhow!("block number missing"))?
                .as_u64();
            set.entry(block_number).or_default().push(log);
        }

        // Handle revert if it happens.
        for (block_number, removed_logs) in &removed_block_logs {
            if revert_to.is_none() {
                let reverted_index = match self.queue.binary_search_by_key(block_number, |e| e.0) {
                    Ok(x) => x,
                    Err(x) => x,
                };
                self.queue.truncate(reverted_index);
                let first = removed_logs.first().expect("not empty");
                let first_reverted_tx_seq = SubmitFilter::decode_log(&RawLog {
                    topics: first.topics.clone(),
                    data: first.data.to_vec(),
                })?
                .submission_index
                .as_u64();
                revert_to = Some(first_reverted_tx_seq);
            } else {
                // Other removed logs should have larger tx seq, so no need to process them.
                break;
            }
        }

        // Add new logs to the queue.
        for (block_number, new_logs) in block_logs {
            if block_number <= self.queue.back().map(|e| e.0).unwrap_or(0) {
                bail!("reverted without being notified");
            }
            self.queue.push_back((block_number, new_logs));
        }

        Ok(revert_to)
    }

    /// Pass in the latest block number and return the confirmed logs.
    fn confirm_logs(&mut self, latest_block_number: u64) -> Vec<Log> {
        self.latest_block_number = latest_block_number;
        let mut confirmed_logs = Vec::new();
        while let Some((block_number, _)) = self.queue.front() {
            if *block_number
                > self
                    .latest_block_number
                    .wrapping_sub(self.confirmation_delay)
            {
                break;
            }
            let (_, mut logs) = self.queue.pop_front().unwrap();
            confirmed_logs.append(&mut logs);
        }
        confirmed_logs
    }
}

#[derive(Debug)]
pub enum LogFetchProgress {
    SyncedBlock((u64, H256)),
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
