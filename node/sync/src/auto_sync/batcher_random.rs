use super::{batcher::Batcher, sync_store::SyncStore};
use crate::{
    auto_sync::{batcher::SyncResult, INTERVAL_ERROR, INTERVAL_IDLE},
    Config, SyncSender,
};
use anyhow::Result;
use storage_async::Store;
use tokio::time::sleep;

#[derive(Debug)]
pub struct RandomBatcher {
    batcher: Batcher,
    sync_store: SyncStore,
}

impl RandomBatcher {
    pub fn new(config: Config, store: Store, sync_send: SyncSender) -> Self {
        let sync_store = SyncStore::new(store.clone());

        Self {
            // now, only 1 thread to sync file randomly
            batcher: Batcher::new(config, 1, store, sync_send),
            sync_store,
        }
    }

    pub async fn start(mut self) {
        info!("Start to sync files");

        loop {
            match self.sync_once().await {
                Ok(true) => {}
                Ok(false) => {
                    trace!(?self, "File sync still in progress or idle");
                    sleep(INTERVAL_IDLE).await;
                }
                Err(err) => {
                    warn!(%err, ?self, "Failed to sync file once");
                    sleep(INTERVAL_ERROR).await;
                }
            }
        }
    }

    async fn sync_once(&mut self) -> Result<bool> {
        if self.schedule().await? {
            return Ok(true);
        }

        // poll any completed file sync
        let (tx_seq, sync_result) = match self.batcher.poll().await? {
            Some(v) => v,
            None => return Ok(false),
        };

        debug!(%tx_seq, ?sync_result, ?self, "Completed to sync file");

        match sync_result {
            SyncResult::Completed => self.sync_store.remove_tx(tx_seq).await?,
            _ => self.sync_store.downgrade_tx_to_pending(tx_seq).await?,
        };

        Ok(true)
    }

    async fn schedule(&mut self) -> Result<bool> {
        if self.batcher.len() > 0 {
            return Ok(false);
        }

        let tx_seq = match self.sync_store.random_tx().await? {
            Some(v) => v,
            None => return Ok(false),
        };

        if !self.batcher.add(tx_seq).await? {
            return Ok(false);
        }

        debug!(?self, "Pick a file to sync");

        Ok(true)
    }
}
