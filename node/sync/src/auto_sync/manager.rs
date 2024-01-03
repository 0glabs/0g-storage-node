use super::sync_store::SyncStore;
use crate::{controllers::SyncState, Config, SyncRequest, SyncResponse, SyncSender};
use anyhow::{bail, Result};
use log_entry_sync::LogSyncEvent;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::broadcast::{error::RecvError, Receiver};
use tokio::time::sleep;

const INTERVAL_CATCHUP: Duration = Duration::from_millis(1);
const INTERVAL: Duration = Duration::from_secs(1);
const INTERVAL_ERROR: Duration = Duration::from_secs(10);

/// Manager to synchronize files among storage nodes automatically.
///
/// Generally, most files could be synchronized among storage nodes. However, a few
/// files may be unavailable on all storage nodes, e.g.
///
/// 1. File not uploaded by user in time.
/// 2. File removed due to blockchain reorg, and user do not upload again.
///
/// So, there are 2 workers to synchronize files in parallel:
///
/// 1. Synchronize announced files in sequence. If any file unavailable, store it into db.
/// 2. Synchronize the missed files in sequential synchronization.
#[derive(Clone)]
pub struct Manager {
    config: Config,

    /// The next `tx_seq` to sync in sequence.
    next_tx_seq: Arc<AtomicU64>,

    /// The maximum `tx_seq` to sync in sequence, `u64::MAX` means unlimited.
    /// Generally, it is updated when file announcement received.
    max_tx_seq: Arc<AtomicU64>,

    /// The last reverted transaction seq, `u64::MAX` means no tx reverted.
    /// Generally, it is updated when transaction reverted.
    reverted_tx_seq: Arc<AtomicU64>,

    store: Store,
    sync_store: SyncStore,

    /// Used to interact with sync service for the current file in sync.
    sync_send: SyncSender,
}

impl Manager {
    pub async fn new(store: Store, sync_send: SyncSender, config: Config) -> Result<Self> {
        let sync_store = SyncStore::new(store.clone());

        let (next_tx_seq, max_tx_seq) = sync_store.get_tx_seq_range().await?;
        let next_tx_seq = next_tx_seq.unwrap_or(0);
        let max_tx_seq = max_tx_seq.unwrap_or(u64::MAX);

        Ok(Self {
            config,
            next_tx_seq: Arc::new(AtomicU64::new(next_tx_seq)),
            max_tx_seq: Arc::new(AtomicU64::new(max_tx_seq)),
            reverted_tx_seq: Arc::new(AtomicU64::new(u64::MAX)),
            store,
            sync_store,
            sync_send,
        })
    }

    pub fn spwn(&self, executor: &TaskExecutor, receiver: Receiver<LogSyncEvent>) {
        executor.spawn(
            self.clone().monitor_reorg(receiver),
            "sync_manager_reorg_monitor",
        );

        executor.spawn(self.clone().start_sync(), "sync_manager_sequential_syncer");

        executor.spawn(
            self.clone().start_sync_pending_txs(),
            "sync_manager_pending_syncer",
        );
    }

    fn set_reverted(&self, tx_seq: u64) -> bool {
        if tx_seq >= self.reverted_tx_seq.load(Ordering::Relaxed) {
            return false;
        }

        self.reverted_tx_seq.store(tx_seq, Ordering::Relaxed);

        true
    }

    fn handle_on_reorg(&self) -> Option<u64> {
        let reverted_tx_seq = self.reverted_tx_seq.load(Ordering::Relaxed);

        // no reorg happened
        if reverted_tx_seq == u64::MAX {
            return None;
        }

        self.reverted_tx_seq.store(u64::MAX, Ordering::Relaxed);

        // reorg happened, but no impact on file sync
        let next_tx_seq = self.next_tx_seq.load(Ordering::Relaxed);
        if reverted_tx_seq > next_tx_seq {
            return None;
        }

        // handles on reorg
        info!(%reverted_tx_seq, %next_tx_seq, "Transaction reverted");

        // re-sync files from the reverted tx seq
        self.next_tx_seq.store(reverted_tx_seq, Ordering::Relaxed);

        Some(next_tx_seq)
    }

    pub async fn update_on_announcement(&self, announced_tx_seq: u64) {
        // new file announced
        let max_tx_seq = self.max_tx_seq.load(Ordering::Relaxed);
        if max_tx_seq == u64::MAX || announced_tx_seq > max_tx_seq {
            match self.sync_store.set_max_tx_seq(announced_tx_seq).await {
                Ok(()) => self.max_tx_seq.store(announced_tx_seq, Ordering::Relaxed),
                Err(e) => error!(%e, "Failed to set max_tx_seq in store"),
            };
            return;
        }

        // already wait for sequential sync
        if announced_tx_seq >= self.next_tx_seq.load(Ordering::Relaxed) {
            return;
        }

        // otherwise, mark tx as ready for sync
        if let Err(e) = self.sync_store.upgrade_tx_to_ready(announced_tx_seq).await {
            error!(%e, "Failed to promote announced tx to ready");
        }
    }

    async fn move_forward(&self, pending: bool) -> Result<bool> {
        let tx_seq = self.next_tx_seq.load(Ordering::Relaxed);
        if tx_seq > self.max_tx_seq.load(Ordering::Relaxed) {
            return Ok(false);
        }

        // put the tx into pending list
        if pending && self.sync_store.add_pending_tx(tx_seq).await? {
            debug!(%tx_seq, "Pending tx added");
        }

        let next_tx_seq = tx_seq + 1;
        self.sync_store.set_next_tx_seq(next_tx_seq).await?;
        self.next_tx_seq.store(next_tx_seq, Ordering::Relaxed);

        Ok(true)
    }

    /// Returns whether file sync in progress but no peers found
    async fn sync_tx(&self, tx_seq: u64) -> Result<bool> {
        // tx not available yet
        if self.store.get_tx_by_seq_number(tx_seq).await?.is_none() {
            return Ok(false);
        }

        // get sync state to handle in advance
        let state = match self
            .sync_send
            .request(SyncRequest::SyncStatus { tx_seq })
            .await?
        {
            SyncResponse::SyncStatus { status } => status,
            _ => bail!("Invalid sync response type"),
        };
        trace!(?tx_seq, ?state, "sync_tx tx status");

        // notify service to sync file if not started or failed
        if matches!(state, None | Some(SyncState::Failed { .. })) {
            match self
                .sync_send
                .request(SyncRequest::SyncFile { tx_seq })
                .await?
            {
                SyncResponse::SyncFile { err } if err.is_empty() => return Ok(false),
                SyncResponse::SyncFile { err } => bail!("Failed to sync file: {:?}", err),
                _ => bail!("Invalid sync response type"),
            }
        }

        if matches!(state, Some(SyncState::FindingPeers { since, .. }) if since.elapsed() > self.config.find_peer_timeout)
        {
            // no peers found for a long time
            self.terminate_file_sync(tx_seq, false).await;
            Ok(true)
        } else {
            // otherwise, continue to wait for file sync that already in progress
            Ok(false)
        }
    }

    async fn terminate_file_sync(&self, tx_seq: u64, is_reverted: bool) {
        if let Err(err) = self
            .sync_send
            .request(SyncRequest::TerminateFileSync {
                tx_seq,
                is_reverted,
            })
            .await
        {
            // just log and go ahead for any error, e.g. timeout
            error!(%err, %tx_seq, "Failed to terminate file sync");
        }
    }

    /// Starts to monitor reorg and handle on transaction reverted.
    async fn monitor_reorg(self, mut receiver: Receiver<LogSyncEvent>) {
        info!("Start to monitor reorg");

        loop {
            match receiver.recv().await {
                Ok(LogSyncEvent::ReorgDetected { .. }) => {}
                Ok(LogSyncEvent::Reverted { tx_seq }) => {
                    // requires to re-sync files since transaction and files removed in storage
                    self.set_reverted(tx_seq);
                }
                Ok(LogSyncEvent::TxSynced { .. }) => {} //No need to handle synced tx in reorg
                Err(RecvError::Closed) => {
                    // program terminated
                    info!("Completed to monitor reorg");
                    return;
                }
                Err(RecvError::Lagged(lagged)) => {
                    // Generally, such error should not happen since confirmed block
                    // reorg rarely happen, and the buffer size of broadcast channel
                    // is big enough.
                    error!(%lagged, "Failed to receive reverted tx (Lagged)");
                }
            }
        }
    }

    /// Starts to synchronize files in sequence.
    async fn start_sync(self) {
        info!(
            "Start to sync files periodically, next = {}, max = {}",
            self.next_tx_seq.load(Ordering::Relaxed),
            self.max_tx_seq.load(Ordering::Relaxed)
        );

        loop {
            // handles reorg before file sync
            if let Some(tx_seq) = self.handle_on_reorg() {
                // request sync service to terminate the file sync immediately
                self.terminate_file_sync(tx_seq, true).await;
            }

            // sync file
            let sync_result = self.sync_once().await;
            let next_tx_seq = self.next_tx_seq.load(Ordering::Relaxed);
            match sync_result {
                Ok(true) => {
                    debug!(%next_tx_seq, "Completed to sync file");
                    sleep(INTERVAL_CATCHUP).await;
                }
                Ok(false) => {
                    trace!(%next_tx_seq, "File in sync or log entry unavailable");
                    sleep(INTERVAL).await;
                }
                Err(err) => {
                    warn!(%err, %next_tx_seq, "Failed to sync file");
                    sleep(INTERVAL_ERROR).await;
                }
            }
        }
    }

    async fn sync_once(&self) -> Result<bool> {
        // already sync to the latest file
        let next_tx_seq = self.next_tx_seq.load(Ordering::Relaxed);
        if next_tx_seq > self.max_tx_seq.load(Ordering::Relaxed) {
            return Ok(false);
        }

        // already finalized
        if self.store.check_tx_completed(next_tx_seq).await? {
            self.move_forward(false).await?;
            return Ok(true);
        }

        // try sync tx
        let no_peer_timeout = self.sync_tx(next_tx_seq).await?;

        // put tx to pending list if no peers found for a long time
        if no_peer_timeout {
            self.move_forward(true).await?;
        }

        Ok(no_peer_timeout)
    }

    /// Starts to synchronize pending files that unavailable during sequential synchronization.
    async fn start_sync_pending_txs(self) {
        info!("Start to sync pending files");

        let mut tx_seq = 0;
        let mut next = true;

        loop {
            if next {
                match self.sync_store.random_tx().await {
                    Ok(Some(seq)) => tx_seq = seq,
                    Ok(None) => {
                        trace!("No pending file to sync");
                        sleep(INTERVAL).await;
                        continue;
                    }
                    Err(err) => {
                        warn!(%err, "Failed to pick pending file to sync");
                        sleep(INTERVAL_ERROR).await;
                        continue;
                    }
                }
            }

            match self.sync_pending_tx(tx_seq).await {
                Ok(true) => {
                    debug!(%tx_seq, "Completed to sync pending file");
                    sleep(INTERVAL_CATCHUP).await;
                    next = true;
                }
                Ok(false) => {
                    trace!(%tx_seq, "Pending file in sync or tx unavailable");
                    sleep(INTERVAL).await;
                    next = false;
                }
                Err(err) => {
                    warn!(%err, %tx_seq, "Failed to sync pending file");
                    sleep(INTERVAL_ERROR).await;
                    next = false;
                }
            }
        }
    }

    async fn sync_pending_tx(&self, tx_seq: u64) -> Result<bool> {
        // already finalized
        if self.store.check_tx_completed(tx_seq).await? {
            self.sync_store.remove_tx(tx_seq).await?;
            return Ok(true);
        }

        // try sync tx
        let no_peer_timeout = self.sync_tx(tx_seq).await?;

        // downgrade if no peers found for a long time
        if no_peer_timeout && self.sync_store.downgrade_tx_to_pending(tx_seq).await? {
            debug!(%tx_seq, "No peers found for pending file and downgraded");
        }

        Ok(no_peer_timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ops::Sub,
        sync::atomic::Ordering,
        time::{Duration, Instant},
    };

    use channel::{test_util::TestReceiver, Channel};
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::{
        auto_sync::sync_store::SyncStore,
        controllers::SyncState,
        test_util::{create_2_store, tests::TestStoreRuntime},
        Config, SyncMessage, SyncRequest, SyncResponse,
    };

    use super::Manager;

    async fn new_manager(
        runtime: &TestStoreRuntime,
        next_tx_seq: u64,
        max_tx_seq: u64,
    ) -> (
        Manager,
        TestReceiver<SyncMessage, SyncRequest, SyncResponse>,
    ) {
        let sync_store = SyncStore::new(runtime.store.clone());
        sync_store.set_next_tx_seq(next_tx_seq).await.unwrap();
        if max_tx_seq < u64::MAX {
            sync_store.set_max_tx_seq(max_tx_seq).await.unwrap();
        }

        let (sync_send, sync_recv) = Channel::unbounded();
        let manager = Manager::new(runtime.store.clone(), sync_send, Config::default())
            .await
            .unwrap();
        (manager, sync_recv.into())
    }

    #[tokio::test]
    async fn test_manager_init_values() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 4, 12).await;

        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 4);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 12);
        assert_eq!(manager.reverted_tx_seq.load(Ordering::Relaxed), u64::MAX);
    }

    #[tokio::test]
    async fn test_manager_set_reverted() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 4, 12).await;

        // reverted to tx 5
        assert!(manager.set_reverted(5));
        assert_eq!(manager.reverted_tx_seq.load(Ordering::Relaxed), 5);

        // no effect if tx 6 reverted again
        assert!(!manager.set_reverted(6));
        assert_eq!(manager.reverted_tx_seq.load(Ordering::Relaxed), 5);

        // overwrite tx 5 if tx 3 reverted
        assert!(manager.set_reverted(3));
        assert_eq!(manager.reverted_tx_seq.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_manager_handle_reorg() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 4, 12).await;

        // no effect if not reverted
        assert_eq!(manager.handle_on_reorg(), None);
        assert_eq!(manager.reverted_tx_seq.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 4);

        // tx 5 reverted, but sync in future
        assert!(manager.set_reverted(5));
        assert_eq!(manager.handle_on_reorg(), None);
        assert_eq!(manager.reverted_tx_seq.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 4);

        // tx 3 reverted, should terminate tx 4 and re-sync files since tx 3
        assert!(manager.set_reverted(3));
        assert_eq!(manager.handle_on_reorg(), Some(4));
        assert_eq!(manager.reverted_tx_seq.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_manager_update_on_announcement() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 4, 12).await;

        // no effect if tx 10 announced
        manager.update_on_announcement(10).await;
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 4);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 12);

        // `max_tx_seq` enlarged if tx 20 announced
        manager.update_on_announcement(20).await;
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 4);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 20);

        // no effect if announced for a non-pending tx
        manager.update_on_announcement(2).await;
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 4);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 20);
        assert_eq!(manager.sync_store.random_tx().await.unwrap(), None);

        // pending tx upgraded if announcement received
        assert!(manager.sync_store.add_pending_tx(1).await.unwrap());
        assert!(manager.sync_store.add_pending_tx(2).await.unwrap());
        manager.update_on_announcement(2).await;
        assert_eq!(manager.sync_store.random_tx().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn test_manager_move_forward() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 4, 12).await;

        // move forward from 4 to 5
        assert!(manager.move_forward(false).await.unwrap());
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 5);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 12);
        assert_eq!(
            manager.sync_store.get_tx_seq_range().await.unwrap(),
            (Some(5), Some(12))
        );

        // move forward and add tx 5 to pending list
        assert!(manager.move_forward(true).await.unwrap());
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 6);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 12);
        assert_eq!(
            manager.sync_store.get_tx_seq_range().await.unwrap(),
            (Some(6), Some(12))
        );
        assert_eq!(manager.sync_store.random_tx().await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn test_manager_move_forward_failed() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 5, 5).await;

        // 5 -> 6
        assert!(manager.move_forward(false).await.unwrap());
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 6);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 5);

        // cannot move forward anymore
        assert!(!manager.move_forward(false).await.unwrap());
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 6);
        assert_eq!(manager.max_tx_seq.load(Ordering::Relaxed), 5);
        assert_eq!(
            manager.sync_store.get_tx_seq_range().await.unwrap(),
            (Some(6), Some(5))
        );
    }

    #[tokio::test]
    async fn test_manager_sync_tx_unavailable() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 4, 12).await;

        assert!(!manager.sync_tx(4).await.unwrap());
    }

    #[tokio::test]
    async fn test_manager_sync_tx_status_none() {
        let (_, store, _, _) = create_2_store(vec![1314, 1324]);
        let runtime = TestStoreRuntime::new(store);
        let (manager, mut sync_recv) = new_manager(&runtime, 1, 5).await;

        let (_, sync_result) = tokio::join!(
            sync_recv.expect_responses(vec![
                SyncResponse::SyncStatus { status: None },
                // cause to file sync started
                SyncResponse::SyncFile { err: String::new() },
            ]),
            manager.sync_tx(1)
        );
        assert!(!sync_result.unwrap());
        assert!(matches!(sync_recv.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn test_manager_sync_tx_in_progress() {
        let (_, store, _, _) = create_2_store(vec![1314, 1324]);
        let runtime = TestStoreRuntime::new(store);
        let (manager, mut sync_recv) = new_manager(&runtime, 1, 5).await;

        let (_, sync_result) = tokio::join!(
            // unnecessary to start file sync again
            sync_recv.expect_response(SyncResponse::SyncStatus {
                status: Some(SyncState::ConnectingPeers)
            }),
            manager.sync_tx(1)
        );
        assert!(!sync_result.unwrap());
        assert!(matches!(sync_recv.try_recv(), Err(TryRecvError::Empty)));
    }

    async fn expect_no_peer_found(
        sync_recv: &mut TestReceiver<SyncMessage, SyncRequest, SyncResponse>,
    ) {
        let responses = vec![
            // no peers for file sync for a long time
            SyncResponse::SyncStatus {
                status: Some(SyncState::FindingPeers {
                    since: Instant::now().sub(Duration::from_secs(10000)),
                    updated: Instant::now(),
                }),
            },
            // required to terminate the file sync
            SyncResponse::TerminateFileSync { count: 1 },
        ];
        sync_recv.expect_responses(responses).await
    }

    #[tokio::test]
    async fn test_manager_sync_tx_no_peer_found() {
        let (_, store, _, _) = create_2_store(vec![1314, 1324]);
        let runtime = TestStoreRuntime::new(store);
        let (manager, mut sync_recv) = new_manager(&runtime, 1, 5).await;

        let (_, sync_result) =
            tokio::join!(expect_no_peer_found(&mut sync_recv), manager.sync_tx(1));
        assert!(sync_result.unwrap());
        assert!(matches!(sync_recv.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn test_manager_sync_once_already_latest() {
        let runtime = TestStoreRuntime::default();
        let (manager, _sync_recv) = new_manager(&runtime, 6, 5).await;

        assert!(!manager.sync_once().await.unwrap());
    }

    #[tokio::test]
    async fn test_manager_sync_once_finalized() {
        let (_, store, _, _) = create_2_store(vec![1314, 1324]);
        let runtime = TestStoreRuntime::new(store);
        let (manager, _sync_recv) = new_manager(&runtime, 1, 5).await;

        assert!(manager.sync_once().await.unwrap());
        assert_eq!(manager.next_tx_seq.load(Ordering::Relaxed), 2);
        assert_eq!(manager.sync_store.random_tx().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_manager_sync_once_no_peer_found() {
        let (store, _, _, _) = create_2_store(vec![1314]);
        let runtime = TestStoreRuntime::new(store);
        let (manager, mut sync_recv) = new_manager(&runtime, 0, 5).await;

        let (_, sync_result) =
            tokio::join!(expect_no_peer_found(&mut sync_recv), manager.sync_once(),);
        assert!(sync_result.unwrap());
        assert!(matches!(sync_recv.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(manager.sync_store.random_tx().await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn test_manager_sync_pending_tx_finalized() {
        let (_, store, _, _) = create_2_store(vec![1314, 1324]);
        let runtime = TestStoreRuntime::new(store);
        let (manager, _sync_recv) = new_manager(&runtime, 4, 12).await;

        assert!(manager.sync_store.add_pending_tx(0).await.unwrap());
        assert!(manager.sync_store.add_pending_tx(1).await.unwrap());

        assert!(manager.sync_pending_tx(1).await.unwrap());
        assert_eq!(manager.sync_store.random_tx().await.unwrap(), Some(0));
        assert!(manager.sync_store.add_pending_tx(1).await.unwrap());
    }

    #[tokio::test]
    async fn test_manager_sync_pending_tx_no_peer_found() {
        let (store, _, _, _) = create_2_store(vec![1314, 1324]);
        let runtime = TestStoreRuntime::new(store);
        let (manager, mut sync_recv) = new_manager(&runtime, 4, 12).await;

        assert!(manager.sync_store.add_pending_tx(0).await.unwrap());
        assert!(manager.sync_store.add_pending_tx(1).await.unwrap());
        assert!(manager.sync_store.upgrade_tx_to_ready(1).await.unwrap());

        let (_, sync_result) = tokio::join!(
            expect_no_peer_found(&mut sync_recv),
            manager.sync_pending_tx(1),
        );
        assert!(sync_result.unwrap());
        assert!(matches!(sync_recv.try_recv(), Err(TryRecvError::Empty)));
        assert!(manager.sync_store.upgrade_tx_to_ready(1).await.unwrap());
    }
}
