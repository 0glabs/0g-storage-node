use anyhow::Result;
use fs_extra::dir::get_size;
use miner::MinerMessage;
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage::config::{ShardConfig, SHARD_CONFIG_KEY};
use storage::log_store::config::ConfigurableExt;
use storage::log_store::Store;
use tokio::sync::{broadcast, RwLock};

// Start pruning when the db directory size exceeds 0.9 * limit.
const PRUNE_THRESHOLD: f32 = 0.9;

pub struct PrunerConfig {
    pub shard_config: ShardConfig,
    pub db_path: PathBuf,
    pub size_limit: usize,
    pub check_time: Duration,
    pub batch_size: usize,
    pub batch_wait_time: Duration,
}

impl PrunerConfig {
    fn start_prune_size(&self) -> u64 {
        (self.size_limit as f32 * PRUNE_THRESHOLD) as u64
    }
}

pub struct Pruner {
    config: PrunerConfig,
    store: Arc<RwLock<dyn Store>>,

    miner_sender: Option<broadcast::Sender<MinerMessage>>,
}

impl Pruner {
    pub async fn spawn(
        mut config: PrunerConfig,
        store: Arc<RwLock<dyn Store>>,
        miner_sender: Option<broadcast::Sender<MinerMessage>>,
    ) -> Result<()> {
        if let Some(shard_config) = get_shard_config(&store).await? {
            config.shard_config = shard_config;
        }
        let pruner = Pruner {
            config,
            store,
            miner_sender,
        };
        pruner.put_shard_config().await?;
        pruner.start().await
    }

    pub async fn start(mut self) -> Result<()> {
        loop {
            if let Some(delete_list) = self.maybe_update().await? {
                self.put_shard_config().await?;
                let mut batch = Vec::with_capacity(self.config.batch_size);
                let mut iter = delete_list.peekable();
                while let Some(index) = iter.next() {
                    batch.push(index);
                    if batch.len() == self.config.batch_size || iter.peek().is_some() {
                        self.store.write().await.remove_chunks_batch(&batch)?;
                        batch = Vec::with_capacity(self.config.batch_size);
                        tokio::time::sleep(self.config.batch_wait_time).await;
                    }
                }
            }
            tokio::time::sleep(self.config.check_time).await;
        }
    }

    async fn maybe_update(&mut self) -> Result<Option<Box<dyn Send + Iterator<Item = u64>>>> {
        let current_size = get_size(&self.config.db_path)
            .expect(&format!("db size error: db_path={:?}", self.config.db_path));
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
            let flow_len = self.store.read().await.get_context()?.1;
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
        let mut store = self.store.write().await;
        store
            .flow_mut()
            .update_shard_config(self.config.shard_config);
        store.set_config_encoded(&SHARD_CONFIG_KEY, &self.config.shard_config)
    }
}

async fn get_shard_config(store: &RwLock<dyn Store>) -> Result<Option<ShardConfig>> {
    store.read().await.get_config_decoded(&SHARD_CONFIG_KEY)
}
