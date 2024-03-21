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
use std::collections::HashMap;
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

    pub fn handle_reorg(
        &self,
        block_number: u64,
        block_hash: H256,
        executor: &TaskExecutor,
        log_query_delay: Duration,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let (reorg_tx, reorg_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = ZgsFlow::new(self.contract_address, self.provider.clone());
        let provider = self.provider.clone();

        executor.spawn(
            async move {
                let mut block_number = block_number;
                let mut block_hash = block_hash;

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
                                    provider.as_ref(),
                                    block_hash,
                                    block_number,
                                    &contract,
                                    log_query_delay,
                                    &reorg_tx,
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
        start_block_hash: H256,
        executor: &TaskExecutor,
        log_query_delay: Duration,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let (watch_tx, watch_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = ZgsFlow::new(self.contract_address, self.provider.clone());
        let provider = self.provider.clone();
        let confirmation_delay = self.confirmation_delay;
        executor.spawn(
            async move {
                debug!("start_watch starts, start={}", start_block_number);
                let mut progress = start_block_number.saturating_add(1);
                let mut parent_block_hash = start_block_hash;

                loop {
                    match Self::watch_loop(
                        provider.as_ref(),
                        progress,
                        parent_block_hash,
                        &watch_tx,
                        confirmation_delay,
                        &contract,
                        log_query_delay,
                    )
                    .await
                    {
                        Err(e) => {
                            error!("log sync watch error: e={:?}", e);
                        }
                        Ok(Some((p, h))) => {
                            progress = p;
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
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
            },
            "log watch",
        );
        watch_rx
    }

    async fn watch_loop(
        provider: &Provider<RetryClient<Http>>,
        block_number: u64,
        parent_block_hash: H256,
        watch_tx: &UnboundedSender<LogFetchProgress>,
        confirmation_delay: u64,
        contract: &ZgsFlow<Provider<RetryClient<Http>>>,
        log_query_delay: Duration,
    ) -> Result<Option<(u64, H256)>> {
        debug!("get block");
        let latest_block = provider
            .get_block(BlockNumber::Latest)
            .await?
            .ok_or_else(|| anyhow!("None for latest block"))?;
        let latest_block_number = match latest_block.number {
            Some(b) => b.as_u64(),
            _ => return Ok(None),
        };

        if block_number > latest_block_number - confirmation_delay {
            return Ok(None);
        }

        let block = provider
            .get_block_with_txs(block_number)
            .await?
            .ok_or_else(|| anyhow!("None for block {}", block_number))?;
        if block.parent_hash != parent_block_hash {
            // reorg happened
            let (parent_block_number, block_hash) = revert_one_block(
                provider,
                parent_block_hash,
                block_number.saturating_sub(1),
                contract,
                log_query_delay,
                watch_tx,
            )
            .await?;
            return Ok(Some((parent_block_number.saturating_add(1), block_hash)));
        }

        debug!(
            "create log filter, start={} end={}",
            block_number, block_number
        );

        let txs_hm = block
            .transactions
            .iter()
            .map(|tx| (tx.transaction_index, tx))
            .collect::<HashMap<_, _>>();

        let mut filter = contract
            .submit_filter()
            .from_block(block_number)
            .to_block(block_number)
            .address(contract.address().into())
            .filter;
        let mut stream = LogQuery::new(&provider, &filter, log_query_delay);
        while let Some(maybe_log) = stream.next().await {
            match maybe_log {
                Ok(log) => {
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
                        bail!("log tx index mismatch, log tx index {:?}, block transaction index {:?}",log.transaction_index, tx.transaction_index );
                    }

                    let tx = SubmitFilter::decode_log(&RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    })?;
                    watch_tx.send(submission_event_to_transaction(tx))?;
                }
                Err(e) => {
                    error!("log query error: e={:?}", e);
                    filter = filter.from_block(block_number).address(contract.address());
                    stream = LogQuery::new(&provider, &filter, log_query_delay);
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
            }
        }

        let progress = if block.hash.is_some() && block.number.is_some() {
            Some((block.number.unwrap().as_u64(), block.hash.unwrap()))
        } else {
            None
        };
        if let Some(p) = &progress {
            watch_tx.send(LogFetchProgress::SyncedBlock(*p))?;
        }
        Ok(progress)
    }

    pub fn provider(&self) -> &Provider<RetryClient<Http>> {
        self.provider.as_ref()
    }
}

async fn revert_one_block(
    provider: &Provider<RetryClient<Http>>,
    block_hash: H256,
    block_number: u64,
    contract: &ZgsFlow<Provider<RetryClient<Http>>>,
    log_query_delay: Duration,
    watch_tx: &UnboundedSender<LogFetchProgress>,
) -> Result<(u64, H256), anyhow::Error> {
    let block = provider
        .get_block(block_hash)
        .await?
        .ok_or_else(|| anyhow!("None for block {}", block_hash))?;

    let filter = contract
        .submit_filter()
        .at_block_hash(block_hash)
        .address(contract.address().into())
        .filter;
    let mut stream = LogQuery::new(&provider, &filter, log_query_delay);
    let mut reverted = None;
    while let Some(maybe_log) = stream.next().await {
        match maybe_log {
            Ok(log) => {
                assert!(log.removed.unwrap_or(false));
                match SubmitFilter::decode_log(&RawLog {
                    topics: log.topics,
                    data: log.data.to_vec(),
                }) {
                    Ok(event) => {
                        if reverted.is_none() {
                            reverted = Some(event.submission_index.as_u64());
                            // break;
                        } else {
                            assert!(reverted < Some(event.submission_index.as_u64()));
                        }
                    }
                    Err(e) => {
                        error!("log decode error: e={:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("get log error: e={:?}", e);
            }
        }
    }
    if let Some(reverted) = reverted {
        watch_tx.send(LogFetchProgress::Reverted(reverted))?;
    }

    let synced_block =
        LogFetchProgress::SyncedBlock((block_number.saturating_sub(1), block.parent_hash));
    watch_tx.send(synced_block)?;

    Ok((block_number.saturating_sub(1), block.parent_hash))
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
