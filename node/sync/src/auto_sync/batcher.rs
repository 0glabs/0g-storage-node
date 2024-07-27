use crate::{controllers::SyncState, Config, SyncRequest, SyncResponse, SyncSender};
use anyhow::{bail, Result};
use std::fmt::Debug;
use storage_async::Store;

#[derive(Debug)]
pub enum SyncResult {
    Completed,
    Failed,
    Timeout,
}

/// Supports to sync files concurrently.
pub struct Batcher {
    config: Config,
    capacity: usize,
    tasks: Vec<u64>, // files to sync
    store: Store,
    sync_send: SyncSender,
}

impl Debug for Batcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.tasks)
    }
}

impl Batcher {
    pub fn new(config: Config, capacity: usize, store: Store, sync_send: SyncSender) -> Self {
        Self {
            config,
            capacity,
            tasks: Default::default(),
            store,
            sync_send,
        }
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub async fn add(&mut self, tx_seq: u64) -> Result<bool> {
        // limits the number of threads
        if self.tasks.len() >= self.capacity {
            return Ok(false);
        }

        // requires log entry available before file sync
        if self.store.get_tx_by_seq_number(tx_seq).await?.is_none() {
            return Ok(false);
        }

        self.tasks.push(tx_seq);

        Ok(true)
    }

    pub fn reorg(&mut self, reverted_tx_seq: u64) {
        self.tasks.retain(|&x| x < reverted_tx_seq);
    }

    /// Poll the sync result of any completed file sync.
    pub async fn poll(&mut self) -> Result<Option<(u64, SyncResult)>> {
        let mut result = None;
        let mut index = self.tasks.len();

        for (i, tx_seq) in self.tasks.iter().enumerate() {
            if let Some(ret) = self.poll_tx(*tx_seq).await? {
                result = Some((*tx_seq, ret));
                index = i;
                break;
            }
        }

        if index < self.tasks.len() {
            self.tasks.swap_remove(index);
        }

        Ok(result)
    }

    async fn poll_tx(&self, tx_seq: u64) -> Result<Option<SyncResult>> {
        // file already exists
        if self.store.check_tx_completed(tx_seq).await? {
            // File may be finalized during file sync, e.g. user uploaded file via RPC.
            // In this case, just terminate the file sync.
            let num_terminated = self.terminate_file_sync(tx_seq, false).await;
            info!(%tx_seq, %num_terminated, "Terminate file sync due to file already finalized in db");
            return Ok(Some(SyncResult::Completed));
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
        trace!(?tx_seq, ?state, "File sync status retrieved");

        match state {
            // start file sync if not launched yet
            None => match self
                .sync_send
                .request(SyncRequest::SyncFile { tx_seq })
                .await?
            {
                SyncResponse::SyncFile { err } if err.is_empty() => Ok(None),
                SyncResponse::SyncFile { err } => bail!("Failed to sync file: {:?}", err),
                _ => bail!("Invalid sync response type"),
            },

            // file sync completed
            Some(SyncState::Completed) => Ok(Some(SyncResult::Completed)),

            // file sync failed
            Some(SyncState::Failed { reason }) => {
                debug!(
                    ?reason,
                    "Failed to sync file and terminate the failed file sync"
                );
                self.terminate_file_sync(tx_seq, false).await;
                Ok(Some(SyncResult::Failed))
            }

            // finding peers timeout
            Some(SyncState::FindingPeers { origin, .. })
                if origin.elapsed() > self.config.find_peer_timeout =>
            {
                debug!(%tx_seq, "Terminate file sync due to finding peers timeout");
                self.terminate_file_sync(tx_seq, false).await;
                Ok(Some(SyncResult::Timeout))
            }

            // connecting peers timeout
            Some(SyncState::ConnectingPeers { origin, .. })
                if origin.elapsed() > self.config.find_peer_timeout =>
            {
                debug!(%tx_seq, "Terminate file sync due to connecting peers timeout");
                self.terminate_file_sync(tx_seq, false).await;
                Ok(Some(SyncResult::Timeout))
            }

            // others
            _ => Ok(None),
        }
    }

    pub async fn terminate_file_sync(&self, tx_seq: u64, is_reverted: bool) -> usize {
        match self
            .sync_send
            .request(SyncRequest::TerminateFileSync {
                tx_seq,
                is_reverted,
            })
            .await
        {
            Ok(SyncResponse::TerminateFileSync { count }) => count,
            Ok(resp) => {
                error!(?resp, %tx_seq, %is_reverted, "Invalid sync response type to terminate file sync");
                0
            }
            Err(err) => {
                // just log and go ahead for any error, e.g. timeout
                error!(%err, %tx_seq, %is_reverted, "Failed to terminate file sync");
                0
            }
        }
    }
}
