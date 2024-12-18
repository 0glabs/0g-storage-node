use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use storage::log_store::log_manager::DATA_DB_KEY;
use storage_async::Store;
use tokio::time::sleep;

use crate::Config;

use super::sync_store::{Queue, SyncStore};

const KEY_NEXT_TX_SEQ: &str = "sync.manager.historical.next_tx_seq";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalTxWriterState {
    pub next_tx_seq: u64,
    pub pending_txs: usize,
    pub ready_txs: usize,
}

pub struct HistoricalTxWriter {
    config: Config,
    store: Store,
    sync_store: Arc<SyncStore>,
    next_tx_seq: Arc<AtomicU64>,
}

impl HistoricalTxWriter {
    pub async fn new(config: Config, store: Store, sync_store: Arc<SyncStore>) -> Result<Self> {
        let next_tx_seq = store
            .get_config_decoded(&KEY_NEXT_TX_SEQ, DATA_DB_KEY)
            .await?;

        Ok(Self {
            config,
            store,
            sync_store,
            next_tx_seq: Arc::new(AtomicU64::new(next_tx_seq.unwrap_or(0))),
        })
    }

    pub async fn get_state(&self) -> Result<HistoricalTxWriterState> {
        let (pending_txs, ready_txs, _) = self.sync_store.stat().await?;

        Ok(HistoricalTxWriterState {
            next_tx_seq: self.next_tx_seq.load(Ordering::Relaxed),
            pending_txs,
            ready_txs,
        })
    }

    pub async fn start(mut self) {
        info!(
            "Start to write historical files into sync store, state = {:?}",
            self.get_state().await
        );

        loop {
            match self.write_once().await {
                Ok(true) => {}
                Ok(false) => {
                    trace!(
                        "There is no tx to write in sync store, state = {:?}",
                        self.get_state().await
                    );
                    sleep(self.config.auto_sync_idle_interval).await;
                }
                Err(err) => {
                    warn!(%err, "Failed to write tx once, state = {:?}", self.get_state().await);
                    sleep(self.config.auto_sync_error_interval).await;
                }
            }
        }
    }

    async fn write_once(&mut self) -> Result<bool> {
        let mut next_tx_seq = self.next_tx_seq.load(Ordering::Relaxed);

        // no tx to write in sync store
        if next_tx_seq >= self.store.get_store().next_tx_seq() {
            return Ok(false);
        }

        // write tx in sync store if not finalized or pruned
        if self.store.get_store().get_tx_status(next_tx_seq)?.is_none() {
            self.sync_store.insert(next_tx_seq, Queue::Ready).await?;
        }

        // move forward
        next_tx_seq += 1;
        self.store
            .set_config_encoded(&KEY_NEXT_TX_SEQ, &next_tx_seq, DATA_DB_KEY)
            .await?;
        self.next_tx_seq.store(next_tx_seq, Ordering::Relaxed);

        Ok(true)
    }
}
