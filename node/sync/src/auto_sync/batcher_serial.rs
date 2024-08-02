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
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use storage_async::Store;
use tokio::{
    sync::{broadcast::Receiver, mpsc::UnboundedReceiver, RwLock},
    time::sleep,
};

/// Supports to sync files in sequence concurrently.
#[derive(Clone)]
pub struct SerialBatcher {
    batcher: Batcher,

    /// Next tx seq to sync.
    next_tx_seq: Arc<AtomicU64>,
    /// Maximum tx seq to sync.
    max_tx_seq: Arc<AtomicU64>,

    /// Completed txs that pending to update sync db in sequence.
    pending_completed_txs: Arc<RwLock<HashMap<u64, SyncResult>>>,
    /// Next tx seq to sync in db, so as to continue file sync from
    /// break point when program restarted.
    next_tx_seq_in_db: Arc<AtomicU64>,

    sync_store: Arc<SyncStore>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerialBatcherState {
    pub tasks: Vec<u64>,
    pub next: u64,
    pub max: u64,
    pub pendings: HashMap<u64, SyncResult>,
    pub next_in_db: u64,
}

impl Debug for SerialBatcherState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let max_tx_seq_desc = if self.max == u64::MAX {
            "N/A".into()
        } else {
            format!("{}", self.max)
        };

        let pendings_desc = if self.pendings.len() <= 5 {
            format!("{:?}", self.pendings)
        } else {
            format!("{}", self.pendings.len())
        };

        f.debug_struct("SerialBatcher")
            .field("tasks", &self.tasks)
            .field("next", &self.next)
            .field("max", &max_tx_seq_desc)
            .field("pendings", &pendings_desc)
            .field("next_in_db", &self.next_in_db)
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
            next_tx_seq: Arc::new(AtomicU64::new(next_tx_seq.unwrap_or(0))),
            max_tx_seq: Arc::new(AtomicU64::new(max_tx_seq.unwrap_or(u64::MAX))),
            pending_completed_txs: Default::default(),
            next_tx_seq_in_db: Arc::new(AtomicU64::new(next_tx_seq.unwrap_or(0))),
            sync_store,
        })
    }

    pub async fn get_state(&self) -> SerialBatcherState {
        SerialBatcherState {
            tasks: self.batcher.tasks().await,
            next: self.next_tx_seq.load(Ordering::Relaxed),
            max: self.max_tx_seq.load(Ordering::Relaxed),
            pendings: self
                .pending_completed_txs
                .read()
                .await
                .iter()
                .map(|(k, v)| (*k, *v))
                .collect(),
            next_in_db: self.next_tx_seq_in_db.load(Ordering::Relaxed),
        }
    }

    pub async fn start(
        mut self,
        mut file_announcement_recv: UnboundedReceiver<u64>,
        mut log_sync_recv: Receiver<LogSyncEvent>,
        catched_up: Arc<AtomicBool>,
    ) {
        info!("Start to sync files, state = {:?}", self.get_state().await);

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

            // disable file sync until catched up
            if !catched_up.load(Ordering::Relaxed) {
                trace!("Cannot sync file in catch-up phase");
                sleep(INTERVAL_IDLE).await;
                continue;
            }

            // sync files
            match self.sync_once().await {
                Ok(true) => {}
                Ok(false) => {
                    trace!(
                        "File sync still in progress or idle, state = {:?}",
                        self.get_state().await
                    );
                    sleep(INTERVAL_IDLE).await;
                }
                Err(err) => {
                    warn!(%err, "Failed to sync file once, state = {:?}", self.get_state().await);
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
        let max_tx_seq = self.max_tx_seq.load(Ordering::Relaxed);
        if max_tx_seq == u64::MAX || announced_tx_seq > max_tx_seq {
            debug!(%announced_tx_seq, "Update for new file announcement, state = {:?}", self.get_state().await);

            if let Err(err) = self.sync_store.set_max_tx_seq(announced_tx_seq).await {
                error!(%err, %announced_tx_seq, "Failed to set max_tx_seq in store, state = {:?}", self.get_state().await);
            }

            self.max_tx_seq.store(announced_tx_seq, Ordering::Relaxed);

            return true;
        }

        // already wait for sequential sync
        if announced_tx_seq >= self.next_tx_seq.load(Ordering::Relaxed) {
            return true;
        }

        // otherwise, mark tx as ready for sync
        if let Err(err) = self.sync_store.upgrade_tx_to_ready(announced_tx_seq).await {
            error!(%err, %announced_tx_seq, "Failed to promote announced tx to ready, state = {:?}", self.get_state().await);
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
        if reverted_tx_seq >= self.next_tx_seq.load(Ordering::Relaxed) {
            return true;
        }

        info!(%reverted_tx_seq, "Handle reorg started, state = {:?}", self.get_state().await);

        // terminate all files in progress
        self.batcher
            .terminate_file_sync(reverted_tx_seq, true)
            .await;

        // update states
        self.batcher.reorg(reverted_tx_seq).await;
        self.next_tx_seq.store(reverted_tx_seq, Ordering::Relaxed);
        self.pending_completed_txs
            .write()
            .await
            .retain(|&k, _| k < reverted_tx_seq);
        if self.next_tx_seq_in_db.load(Ordering::Relaxed) > reverted_tx_seq {
            self.next_tx_seq_in_db
                .store(reverted_tx_seq, Ordering::Relaxed);

            if let Err(err) = self.sync_store.set_next_tx_seq(reverted_tx_seq).await {
                error!(%err, %reverted_tx_seq, "Failed to set next tx seq due to tx reverted, state = {:?}", self.get_state().await);
            }
        }

        info!(%reverted_tx_seq, "Handle reorg ended, state = {:?}", self.get_state().await);

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

        info!(%tx_seq, ?sync_result, "Completed to sync file, state = {:?}", self.get_state().await);
        self.pending_completed_txs
            .write()
            .await
            .insert(tx_seq, sync_result);

        // update sync db
        self.update_completed_txs_in_db().await?;

        Ok(true)
    }

    /// Schedule file sync in sequence.
    async fn schedule_next(&mut self) -> Result<bool> {
        let next_tx_seq = self.next_tx_seq.load(Ordering::Relaxed);
        if next_tx_seq > self.max_tx_seq.load(Ordering::Relaxed) {
            return Ok(false);
        }

        if !self.batcher.add(next_tx_seq).await? {
            return Ok(false);
        }

        self.next_tx_seq.store(next_tx_seq + 1, Ordering::Relaxed);

        info!("Move forward, state = {:?}", self.get_state().await);

        Ok(true)
    }

    /// Update file sync index in db.
    async fn update_completed_txs_in_db(&mut self) -> Result<()> {
        let origin = self.next_tx_seq_in_db.load(Ordering::Relaxed);
        let mut current = origin;

        while let Some(&sync_result) = self.pending_completed_txs.read().await.get(&current) {
            // downgrade to random sync if file sync failed or timeout
            if matches!(sync_result, SyncResult::Failed | SyncResult::Timeout) {
                self.sync_store.add_pending_tx(current).await?;
            }

            // always move forward in db
            self.sync_store.set_next_tx_seq(current + 1).await?;

            // update in memory after db updated
            self.pending_completed_txs.write().await.remove(&current);
            current += 1;
            self.next_tx_seq_in_db.store(current, Ordering::Relaxed);
        }

        if current > origin {
            info!(%origin, %current, "Move forward in db");
        }

        Ok(())
    }
}
