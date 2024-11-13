use anyhow::{bail, Result};
use contract_interface::ChunkLinearReward;
use ethereum_types::Address;
use ethers::prelude::{Http, Provider};
use ethers::providers::{HttpRateLimitRetryPolicy, RetryClient, RetryClientBuilder};
use miner::MinerMessage;
use rand::Rng;
use std::cmp::Ordering;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use storage::config::{ShardConfig, SHARD_CONFIG_KEY};
use storage::log_store::log_manager::{DATA_DB_KEY, PORA_CHUNK_SIZE};
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};
use zgs_spec::SECTORS_PER_PRICING;

// Start pruning when the db directory size exceeds 0.9 * limit.
const PRUNE_THRESHOLD: f32 = 0.9;

const FIRST_REWARDABLE_CHUNK_KEY: &str = "first_rewardable_chunk";

const CHUNKS_PER_PRICING: u64 = (SECTORS_PER_PRICING / PORA_CHUNK_SIZE) as u64;

#[derive(Debug)]
pub struct PrunerConfig {
    pub shard_config: ShardConfig,
    pub db_path: PathBuf,
    pub max_num_sectors: usize,
    pub check_time: Duration,
    pub batch_size: usize,
    pub batch_wait_time: Duration,

    pub rpc_endpoint_url: String,
    pub reward_address: Address,

    pub rate_limit_retries: u32,
    pub timeout_retries: u32,
    pub initial_backoff: u64,
}

impl PrunerConfig {
    fn start_prune_size(&self) -> u64 {
        (self.max_num_sectors as f32 * PRUNE_THRESHOLD) as u64
    }
}

pub struct Pruner {
    config: PrunerConfig,
    first_rewardable_chunk: u64,
    first_tx_seq: u64,

    store: Arc<Store>,

    sender: mpsc::UnboundedSender<PrunerMessage>,
    miner_sender: Option<broadcast::Sender<MinerMessage>>,

    reward_contract: ChunkLinearReward<Arc<Provider<RetryClient<Http>>>>,
}

impl Pruner {
    pub async fn spawn(
        executor: TaskExecutor,
        mut config: PrunerConfig,
        store: Arc<Store>,
        miner_sender: Option<broadcast::Sender<MinerMessage>>,
    ) -> Result<mpsc::UnboundedReceiver<PrunerMessage>> {
        if let Some(shard_config) = get_shard_config(store.as_ref()).await? {
            config.shard_config = shard_config;
        }
        let (first_rewardable_chunk, first_tx_seq) = get_first_rewardable_chunk(store.as_ref())
            .await?
            .unwrap_or((0, 0));

        let provider = Arc::new(Provider::new(
            RetryClientBuilder::default()
                .rate_limit_retries(config.rate_limit_retries)
                .timeout_retries(config.timeout_retries)
                .initial_backoff(Duration::from_millis(config.initial_backoff))
                .build(
                    Http::from_str(&config.rpc_endpoint_url)?,
                    Box::new(HttpRateLimitRetryPolicy),
                ),
        ));
        let reward_contract = ChunkLinearReward::new(config.reward_address, Arc::new(provider));
        let (tx, rx) = mpsc::unbounded_channel();
        let pruner = Pruner {
            config,
            first_rewardable_chunk,
            first_tx_seq,
            store,
            sender: tx,
            miner_sender,
            reward_contract,
        };
        pruner.put_shard_config().await?;
        executor.spawn(
            async move {
                pruner.start().await.expect("pruner error");
            },
            "pruner",
        );
        Ok(rx)
    }

    pub async fn start(mut self) -> Result<()> {
        loop {
            // Check shard config update and prune unneeded data.
            if let Some(delete_list) = self.maybe_update().await? {
                info!(new_config = ?self.config.shard_config, "new shard config");
                self.put_shard_config().await?;
                self.prune_in_batch(delete_list).await?;
            }

            // Check no reward chunks and prune.
            match self.reward_contract.first_rewardable_chunk().call().await {
                Ok(new_first_rewardable) => {
                    if let Some(no_reward_list) = self
                        .maybe_forward_first_rewardable(new_first_rewardable)
                        .await?
                    {
                        info!(
                            ?new_first_rewardable,
                            "first rewardable chunk moves forward, start pruning"
                        );
                        self.prune_tx(
                            self.first_rewardable_chunk * SECTORS_PER_PRICING as u64,
                            new_first_rewardable * SECTORS_PER_PRICING as u64,
                        )
                        .await?;
                        self.prune_in_batch(no_reward_list).await?;

                        self.first_rewardable_chunk = new_first_rewardable;
                        self.put_first_rewardable_chunk_index(
                            self.first_rewardable_chunk,
                            self.first_tx_seq,
                        )
                        .await?;
                    }
                }
                e => {
                    error!("handle reward contract read fails, e={:?}", e);
                }
            };
            tokio::time::sleep(self.config.check_time).await;
        }
    }

    async fn maybe_update(&mut self) -> Result<Option<Box<dyn Send + Iterator<Item = u64>>>> {
        let current_size = self.store.get_num_entries().await?;
        debug!(
            current_size = current_size,
            config = ?self.config.shard_config,
            "maybe_update"
        );
        if current_size < self.config.start_prune_size() {
            Ok(None)
        } else {
            // Update config and generate delete list should be done in a single lock to ensure
            // the list is complete.
            let config = &mut self.config.shard_config;
            let old_shard_id = config.shard_id;
            let old_num_shard = config.num_shard;

            // Update new config
            let rand_bit = {
                let mut rng = rand::thread_rng();
                rng.gen::<bool>()
            };
            config.shard_id = old_shard_id + rand_bit as usize * old_num_shard;
            config.num_shard *= 2;

            // Generate delete list
            let flow_len = self
                .store
                .get_context()
                .await?
                .1
                .div_ceil(PORA_CHUNK_SIZE as u64);
            let start_index = old_shard_id + (!rand_bit) as usize * old_num_shard;
            Ok(Some(Box::new(
                (start_index as u64..flow_len).step_by(config.num_shard),
            )))
        }
    }

    async fn maybe_forward_first_rewardable(
        &mut self,
        new_first_rewardable: u64,
    ) -> Result<Option<Box<dyn Send + Iterator<Item = u64>>>> {
        match self.first_rewardable_chunk.cmp(&new_first_rewardable) {
            Ordering::Less => Ok(Some(Box::new(
                self.first_rewardable_chunk * CHUNKS_PER_PRICING
                    ..new_first_rewardable * CHUNKS_PER_PRICING,
            ))),
            Ordering::Equal => Ok(None),
            Ordering::Greater => {
                error!(
                    "Unexpected first_rewardable_chunk revert: old={} new={}",
                    self.first_rewardable_chunk, new_first_rewardable
                );
                Ok(None)
            }
        }
    }

    async fn prune_in_batch(&self, to_prune: Box<dyn Send + Iterator<Item = u64>>) -> Result<()> {
        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut iter = to_prune.peekable();
        while let Some(index) = iter.next() {
            batch.push(index);
            if batch.len() == self.config.batch_size || iter.peek().is_none() {
                debug!(start = batch.first(), end = batch.last(), "prune batch");
                self.store.remove_chunks_batch(&batch).await?;
                batch = Vec::with_capacity(self.config.batch_size);
                tokio::time::sleep(self.config.batch_wait_time).await;
            }
        }
        Ok(())
    }

    async fn prune_tx(&mut self, start_sector: u64, end_sector: u64) -> Result<()> {
        loop {
            if let Some(tx) = self.store.get_tx_by_seq_number(self.first_tx_seq).await? {
                // If a part of the tx data is pruned, we mark the tx as pruned.
                if tx.start_entry_index() >= start_sector && tx.start_entry_index() < end_sector {
                    self.store.prune_tx(tx.seq).await?;
                } else if tx.start_entry_index() >= end_sector {
                    break;
                } else {
                    bail!(
                        "prune tx out of range: tx={:?}, start={} end={}",
                        tx,
                        start_sector,
                        end_sector
                    );
                }
                self.first_tx_seq += 1;
            } else {
                // Wait for `first_tx_seq` to be processed.
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
        Ok(())
    }

    async fn put_shard_config(&self) -> Result<()> {
        if let Some(sender) = &self.miner_sender {
            sender.send(MinerMessage::SetShardConfig(self.config.shard_config))?;
        }
        self.sender
            .send(PrunerMessage::ChangeShardConfig(self.config.shard_config))?;
        self.store
            .update_shard_config(self.config.shard_config)
            .await;
        self.store
            .set_config_encoded(&SHARD_CONFIG_KEY, &self.config.shard_config, DATA_DB_KEY)
            .await
    }

    async fn put_first_rewardable_chunk_index(
        &self,
        new_first_rewardable_chunk: u64,
        new_first_tx_seq: u64,
    ) -> Result<()> {
        self.store
            .set_config_encoded(
                &FIRST_REWARDABLE_CHUNK_KEY,
                &(new_first_rewardable_chunk, new_first_tx_seq),
                DATA_DB_KEY,
            )
            .await
    }
}

async fn get_shard_config(store: &Store) -> Result<Option<ShardConfig>> {
    store
        .get_config_decoded(&SHARD_CONFIG_KEY, DATA_DB_KEY)
        .await
}

async fn get_first_rewardable_chunk(store: &Store) -> Result<Option<(u64, u64)>> {
    store
        .get_config_decoded(&FIRST_REWARDABLE_CHUNK_KEY, DATA_DB_KEY)
        .await
}

#[derive(Debug)]
pub enum PrunerMessage {
    ChangeShardConfig(ShardConfig),
}
