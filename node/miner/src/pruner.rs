use crate::{MinerMessage, ShardConfig};
use anyhow::Result;
use fs_extra::dir::get_size;
use itertools::Itertools;
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage::log_store::Store;
use tokio::sync::{broadcast, RwLock};

// Start pruning when the db directory size exceeds 0.9 * limit.
const PRUNE_THRESHOLD: f32 = 0.9;

pub struct PrunerConfig {
    shard_config: ShardConfig,
    db_path: PathBuf,
    size_limit: usize,
    check_time: Duration,
    delete_batch_size: usize,
    delete_sleep_time: Duration,
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
        if let Some((num_shard, shard_id)) = store.read().await.get_shard_config()? {
            let shard_config = ShardConfig {
                shard_id,
                num_shard,
            };
            config.shard_config = shard_config;
            if let Some(sender) = &miner_sender {
                sender.send(MinerMessage::SetShardConfig(shard_config))?;
            }
        } else {
            let shard_config = config.shard_config;
            store
                .write()
                .await
                .put_shard_config(shard_config.num_shard, shard_config.shard_id)?;
        }
        let pruner = Pruner {
            config,
            store,
            miner_sender,
        };
        pruner.start().await
    }

    pub async fn start(mut self) -> Result<()> {
        loop {
            if let Some(delete_list) = self.maybe_update().await? {
                let shard_config = self.config.shard_config;
                self.store
                    .write()
                    .await
                    .put_shard_config(shard_config.num_shard, shard_config.shard_id)?;
                for batch_iter in &delete_list.chunks(self.config.delete_batch_size) {
                    let batch: Vec<u64> = batch_iter.collect();
                    self.store.write().await.remove_chunks_batch(batch)?;
                    tokio::time::sleep(self.config.delete_sleep_time).await;
                }
            }
            tokio::time::sleep(self.config.check_time).await;
        }
    }

    async fn maybe_update(&mut self) -> Result<Option<Box<dyn Iterator<Item = u64>>>> {
        let current_size = get_size(&self.config.db_path)
            .expect(&format!("db size error: db_path={:?}", self.config.db_path));
        if current_size >= self.config.start_prune_size() {
            // Update config and generate delete list should be done in a single lock to ensure
            // the list is complete.
            let config = &mut self.config.shard_config;
            let old_shard_id = config.shard_id;
            let old_num_shard = config.num_shard;

            // Update new config
            let mut rng = rand::thread_rng();
            let rand_bit = rng.gen::<bool>();
            config.shard_id = old_shard_id + rand_bit as usize * old_num_shard;
            config.num_shard *= 2;
            if let Some(sender) = &self.miner_sender {
                sender.send(MinerMessage::SetShardConfig(*config))?;
            }

            // Generate delete list
            let flow_len = self.store.read().await.get_context()?.1;
            let start_index = old_shard_id + (!rand_bit) as usize * old_num_shard;
            return Ok(Some(Box::new(
                (start_index as u64..flow_len).step_by(config.num_shard),
            )));
        }
        Ok(None)
    }
}
