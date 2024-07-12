use super::{batcher::Batcher, sync_store::SyncStore};
use crate::{
    auto_sync::{batcher::SyncResult, INTERVAL_ERROR, INTERVAL_IDLE},
    Config, SyncSender,
};
use anyhow::Result;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use storage_async::Store;
use tokio::time::sleep;

pub struct RandomBatcher {
    batcher: Batcher,
    sync_store: Arc<SyncStore>,
}

impl RandomBatcher {
    pub fn new(
        config: Config,
        store: Store,
        sync_send: SyncSender,
        sync_store: Arc<SyncStore>,
    ) -> Self {
        Self {
            // now, only 1 thread to sync file randomly
            batcher: Batcher::new(config, 1, store, sync_send),
            sync_store,
        }
    }

    pub async fn start(mut self, catched_up: Arc<AtomicBool>) {
        info!("Start to sync files");

        loop {
            // disable file sync until catched up
            if !catched_up.load(Ordering::Relaxed) {
                trace!("Cannot sync file in catch-up phase");
                sleep(INTERVAL_IDLE).await;
                continue;
            }

            match self.sync_once().await {
                Ok(true) => {}
                Ok(false) => {
                    trace!(
                        "File sync still in progress or idle, {:?}",
                        self.stat().await
                    );
                    sleep(INTERVAL_IDLE).await;
                }
                Err(err) => {
                    warn!(%err, "Failed to sync file once, {:?}", self.stat().await);
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

        debug!(%tx_seq, ?sync_result, "Completed to sync file, {:?}", self.stat().await);

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

        debug!("Pick a file to sync, {:?}", self.stat().await);

        Ok(true)
    }

    async fn stat(&self) -> String {
        match self.sync_store.stat().await {
            Ok((num_pending_txs, num_ready_txs)) => format!(
                "RandomBatcher {{ batcher = {:?}, pending_txs = {}, ready_txs = {}}}",
                self.batcher, num_pending_txs, num_ready_txs
            ),
            Err(err) => format!(
                "RandomBatcher {{ batcher = {:?}, pending_txs/ready_txs = Error({:?})}}",
                self.batcher, err
            ),
        }
    }
}
