use super::{
    batcher::{Batcher, SyncResult},
    sync_store::SyncStore,
};
use crate::{
    auto_sync::{INTERVAL_ERROR, INTERVAL_IDLE},
    Config, SyncSender,
};
use anyhow::Result;
use log_entry_sync::LogSyncEvent;
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use storage_async::Store;
use tokio::sync::oneshot;
use tokio::{
    sync::{broadcast::Receiver, mpsc::UnboundedReceiver},
    time::sleep,
};

/// Supports to sync files in sequence concurrently.
pub struct SerialBatcher {
    batcher: Batcher,

    /// Next tx seq to sync.
    next_tx_seq: u64,
    /// Maximum tx seq to sync.
    max_tx_seq: u64,

    /// Completed txs that pending to update sync db in sequence.
    pending_completed_txs: HashMap<u64, SyncResult>,
    /// Next tx seq to sync in db, so as to continue file sync from
    /// break point when program restarted.
    next_tx_seq_in_db: u64,

    sync_store: Arc<SyncStore>,
}

impl Debug for SerialBatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let max_tx_seq_desc = if self.max_tx_seq == u64::MAX {
            "N/A".into()
        } else {
            format!("{}", self.max_tx_seq)
        };

        let pendings_desc = if self.pending_completed_txs.len() <= 5 {
            format!("{:?}", self.pending_completed_txs)
        } else {
            format!("{}", self.pending_completed_txs.len())
        };

        f.debug_struct("SerialBatcher")
            .field("batcher", &self.batcher)
            .field("next", &self.next_tx_seq)
            .field("max", &max_tx_seq_desc)
            .field("pendings", &pendings_desc)
            .field("next_in_db", &self.next_tx_seq_in_db)
            .finish()
    }
}

impl SerialBatcher {
    pub async fn new(
        config: Config,
        store: Store,
        sync_send: SyncSender,
        sync_store: Arc<SyncStore>,
    ) -> Result<Self> {
        let capacity = config.max_sequential_workers;

        // continue file sync from break point in db
        let (next_tx_seq, max_tx_seq) = sync_store.get_tx_seq_range().await?;

        Ok(Self {
            batcher: Batcher::new(config, capacity, store, sync_send),
            next_tx_seq: next_tx_seq.unwrap_or(0),
            max_tx_seq: max_tx_seq.unwrap_or(u64::MAX),
            pending_completed_txs: Default::default(),
            next_tx_seq_in_db: next_tx_seq.unwrap_or(0),
            sync_store,
        })
    }

    pub async fn start(
        mut self,
        mut file_announcement_recv: UnboundedReceiver<u64>,
        mut log_sync_recv: Receiver<LogSyncEvent>,
        catch_up_end_recv: oneshot::Receiver<()>,
    ) {
        info!(?self, "Start to sync files");

        catch_up_end_recv.await.expect("log sync sender dropped");

        loop {
            // handle all pending file announcements
            if self
                .update_file_announcement(&mut file_announcement_recv)
                .await
            {
                continue;
            }

            // handle all reorg events
            if self.handle_reorg(&mut log_sync_recv).await {
                continue;
            }

            // sync files
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

    async fn update_file_announcement(&mut self, recv: &mut UnboundedReceiver<u64>) -> bool {
        let announced_tx_seq = match recv.try_recv() {
            Ok(v) => v,
            Err(_) => return false,
        };

        trace!(%announced_tx_seq, "Received file announcement");

        // new file announced
        if self.max_tx_seq == u64::MAX || announced_tx_seq > self.max_tx_seq {
            debug!(%announced_tx_seq, ?self, "Update for new file announcement");

            if let Err(err) = self.sync_store.set_max_tx_seq(announced_tx_seq).await {
                error!(%err, %announced_tx_seq, ?self, "Failed to set max_tx_seq in store");
            }

            self.max_tx_seq = announced_tx_seq;

            return true;
        }

        // already wait for sequential sync
        if announced_tx_seq >= self.next_tx_seq {
            return true;
        }

        // otherwise, mark tx as ready for sync
        if let Err(err) = self.sync_store.upgrade_tx_to_ready(announced_tx_seq).await {
            error!(%err, %announced_tx_seq, ?self, "Failed to promote announced tx to ready");
        }

        true
    }

    async fn handle_reorg(&mut self, recv: &mut Receiver<LogSyncEvent>) -> bool {
        let reverted_tx_seq = match recv.try_recv() {
            Ok(LogSyncEvent::Reverted { tx_seq }) => tx_seq,
            _ => return false,
        };

        debug!(%reverted_tx_seq, "Reorg detected");

        // reorg happened, but no impact on file sync
        if reverted_tx_seq >= self.next_tx_seq {
            return true;
        }

        info!(%reverted_tx_seq, ?self, "Handle reorg");

        // terminate all files in progress
        self.batcher
            .terminate_file_sync(reverted_tx_seq, true)
            .await;

        // update states
        self.batcher.reorg(reverted_tx_seq);
        self.next_tx_seq = reverted_tx_seq;
        self.pending_completed_txs
            .retain(|&k, _| k < reverted_tx_seq);
        if self.next_tx_seq_in_db > reverted_tx_seq {
            self.next_tx_seq_in_db = reverted_tx_seq;

            if let Err(err) = self
                .sync_store
                .set_next_tx_seq(self.next_tx_seq_in_db)
                .await
            {
                error!(%err, %reverted_tx_seq, ?self, "Failed to set next tx seq due to tx reverted");
            }
        }

        true
    }

    async fn sync_once(&mut self) -> Result<bool> {
        // try to trigger more file sync
        if self.schedule_next().await? {
            return Ok(true);
        }

        // poll any completed file sync
        let (tx_seq, sync_result) = match self.batcher.poll().await? {
            Some(v) => v,
            None => return Ok(false),
        };

        info!(%tx_seq, ?sync_result, ?self, "Completed to sync file");
        self.pending_completed_txs.insert(tx_seq, sync_result);

        // update sync db
        self.update_completed_txs_in_db().await?;

        Ok(true)
    }

    /// Schedule file sync in sequence.
    async fn schedule_next(&mut self) -> Result<bool> {
        if self.next_tx_seq > self.max_tx_seq {
            return Ok(false);
        }

        if !self.batcher.add(self.next_tx_seq).await? {
            return Ok(false);
        }

        self.next_tx_seq += 1;

        info!(?self, "Move forward");

        Ok(true)
    }

    /// Update file sync index in db.
    async fn update_completed_txs_in_db(&mut self) -> Result<()> {
        while let Some(sync_result) = self.pending_completed_txs.get(&self.next_tx_seq_in_db) {
            // downgrade to random sync if file sync failed or timeout
            if matches!(sync_result, SyncResult::Failed | SyncResult::Timeout) {
                self.sync_store
                    .add_pending_tx(self.next_tx_seq_in_db)
                    .await?;
            }

            // always move forward in db
            self.sync_store
                .set_next_tx_seq(self.next_tx_seq_in_db + 1)
                .await?;

            // update in memory after db updated
            self.pending_completed_txs.remove(&self.next_tx_seq_in_db);
            self.next_tx_seq_in_db += 1;
        }

        Ok(())
    }
}
