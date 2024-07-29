use anyhow::Result;
use miner::MinerMessage;
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage::config::{ShardConfig, SHARD_CONFIG_KEY};
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info};

// Start pruning when the db directory size exceeds 0.9 * limit.
const PRUNE_THRESHOLD: f32 = 0.9;

#[derive(Debug)]
pub struct PrunerConfig {
    pub shard_config: ShardConfig,
    pub db_path: PathBuf,
    pub max_num_sectors: usize,
    pub check_time: Duration,
    pub batch_size: usize,
    pub batch_wait_time: Duration,
}

impl PrunerConfig {
    fn start_prune_size(&self) -> u64 {
        (self.max_num_sectors as f32 * PRUNE_THRESHOLD) as u64
    }
}

pub struct Pruner {
    config: PrunerConfig,
    store: Arc<Store>,

    sender: mpsc::UnboundedSender<PrunerMessage>,
    miner_sender: Option<broadcast::Sender<MinerMessage>>,
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
        let (tx, rx) = mpsc::unbounded_channel();
        let pruner = Pruner {
            config,
            store,
            sender: tx,
            miner_sender,
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
            if let Some(delete_list) = self.maybe_update().await? {
                info!(new_config = ?self.config.shard_config, "new shard config");
                self.put_shard_config().await?;
                let mut batch = Vec::with_capacity(self.config.batch_size);
                let mut iter = delete_list.peekable();
                while let Some(index) = iter.next() {
                    batch.push(index);
                    if batch.len() == self.config.batch_size || iter.peek().is_none() {
                        debug!(start = batch.first(), end = batch.last(), "prune batch");
                        self.store.remove_chunks_batch(&batch).await?;
                        batch = Vec::with_capacity(self.config.batch_size);
                        tokio::time::sleep(self.config.batch_wait_time).await;
                    }
                }
            }
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
        if current_size >= self.config.start_prune_size() {
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
            let flow_len = self.store.get_context().await?.1;
            let start_index = old_shard_id + (!rand_bit) as usize * old_num_shard;
            return Ok(Some(Box::new(
                (start_index as u64..flow_len).step_by(config.num_shard),
            )));
        }
        Ok(None)
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
            .set_config_encoded(&SHARD_CONFIG_KEY, &self.config.shard_config)
            .await
    }
}

async fn get_shard_config(store: &Store) -> Result<Option<ShardConfig>> {
    store.get_config_decoded(&SHARD_CONFIG_KEY).await
}

#[derive(Debug)]
pub enum PrunerMessage {
    ChangeShardConfig(ShardConfig),
}
