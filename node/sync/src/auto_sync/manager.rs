use super::{batcher_serial::SerialBatcher, sync_store::SyncStore};
use crate::{
    auto_sync::{INTERVAL_ERROR, INTERVAL_IDLE},
    controllers::SyncState,
    Config, SyncRequest, SyncResponse, SyncSender,
};
use anyhow::{bail, Result};
use log_entry_sync::LogSyncEvent;
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::{
    broadcast::Receiver,
    mpsc::{unbounded_channel, UnboundedSender},
};
use tokio::time::sleep;

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

    store: Store,
    sync_store: SyncStore,

    /// Used to interact with sync service for the current file in sync.
    sync_send: SyncSender,
}

impl Manager {
    pub fn new(store: Store, sync_send: SyncSender, config: Config) -> Self {
        let sync_store = SyncStore::new(store.clone());

        Self {
            config,
            store,
            sync_store,
            sync_send,
        }
    }

    pub async fn spwn(
        self,
        executor: &TaskExecutor,
        log_sync_recv: Receiver<LogSyncEvent>,
    ) -> Result<UnboundedSender<u64>> {
        let (send, recv) = unbounded_channel();

        // sync in sequence
        let serial_batcher =
            SerialBatcher::new(self.config, self.store.clone(), self.sync_send.clone()).await?;
        executor.spawn(
            serial_batcher.start(recv, log_sync_recv),
            "auto_sync_serial",
        );

        // sync randomly
        executor.spawn(self.start_sync_pending_txs(), "auto_sync_random");

        Ok(send)
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

        if matches!(state, Some(SyncState::FindingPeers { origin, .. }) if origin.elapsed() > self.config.find_peer_timeout)
        {
            // no peers found for a long time
            self.terminate_file_sync(tx_seq, false).await;
            info!(%tx_seq, "Terminate file sync due to finding peers timeout");
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

    /// Starts to synchronize pending files that unavailable during sequential synchronization.
    async fn start_sync_pending_txs(self) {
        info!("Start to sync pending files");

        let mut tx_seq = 0;
        let mut next = true;

        loop {
            if next {
                match self.sync_store.random_tx().await {
                    Ok(Some(seq)) => {
                        tx_seq = seq;
                        debug!(%tx_seq, "Start to sync pending file");
                    }
                    Ok(None) => {
                        trace!("No pending file to sync");
                        sleep(INTERVAL_IDLE).await;
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
                    next = true;
                }
                Ok(false) => {
                    trace!(%tx_seq, "Pending file in sync or tx unavailable");
                    sleep(INTERVAL_IDLE).await;
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
        let manager = Manager::new(runtime.store.clone(), sync_send, Config::default());
        (manager, sync_recv.into())
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
                    origin: Instant::now().sub(Duration::from_secs(10000)),
                    since: Instant::now(),
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
