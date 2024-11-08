use super::tx_store::TxStore;
use anyhow::Result;
use std::sync::Arc;
use storage::log_store::{
    config::{ConfigTx, ConfigurableExt},
    log_manager::DATA_DB_KEY,
};
use storage_async::Store;
use tokio::sync::RwLock;

const KEY_NEXT_TX_SEQ: &str = "sync.manager.next_tx_seq";
const KEY_MAX_TX_SEQ: &str = "sync.manager.max_tx_seq";

#[derive(Debug, PartialEq, Eq)]
pub enum Queue {
    Ready,
    Pending,
}

#[derive(Debug, PartialEq, Eq)]
pub enum InsertResult {
    NewAdded,      // new added in target queue
    AlreadyExists, // already exists in target queue
    Upgraded,      // upgraded from pending queue to ready queue
    Downgraded,    // downgraged from ready queue to pending queue
}

pub struct SyncStore {
    store: Arc<RwLock<Store>>,

    /// Pending transactions to sync with low priority.
    pending_txs: TxStore,

    /// Ready transactions to sync with high priority since announcement
    /// already received from other peers.
    ready_txs: TxStore,
}

impl SyncStore {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
            pending_txs: TxStore::new("pending"),
            ready_txs: TxStore::new("ready"),
        }
    }

    pub fn new_with_name(store: Store, pending: &'static str, ready: &'static str) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
            pending_txs: TxStore::new(pending),
            ready_txs: TxStore::new(ready),
        }
    }

    /// Returns the number of pending txs and ready txs.
    pub async fn stat(&self) -> Result<(usize, usize)> {
        let async_store = self.store.read().await;
        let store = async_store.get_store();

        let num_pending_txs = self.pending_txs.count(store)?;
        let num_ready_txs = self.ready_txs.count(store)?;

        Ok((num_pending_txs, num_ready_txs))
    }

    pub async fn get_tx_seq_range(&self) -> Result<(Option<u64>, Option<u64>)> {
        let async_store = self.store.read().await;
        let store = async_store.get_store();

        // load next_tx_seq
        let next_tx_seq = store.get_config_decoded(&KEY_NEXT_TX_SEQ, DATA_DB_KEY)?;

        // load max_tx_seq
        let max_tx_seq = store.get_config_decoded(&KEY_MAX_TX_SEQ, DATA_DB_KEY)?;

        Ok((next_tx_seq, max_tx_seq))
    }

    pub async fn set_next_tx_seq(&self, tx_seq: u64) -> Result<()> {
        let async_store = self.store.write().await;
        let store = async_store.get_store();
        store.set_config_encoded(&KEY_NEXT_TX_SEQ, &tx_seq, DATA_DB_KEY)
    }

    pub async fn set_max_tx_seq(&self, tx_seq: u64) -> Result<()> {
        let async_store = self.store.write().await;
        let store = async_store.get_store();
        store.set_config_encoded(&KEY_MAX_TX_SEQ, &tx_seq, DATA_DB_KEY)
    }

    pub async fn contains(&self, tx_seq: u64) -> Result<Option<Queue>> {
        let async_store = self.store.read().await;
        let store = async_store.get_store();

        if self.ready_txs.has(store, tx_seq)? {
            return Ok(Some(Queue::Ready));
        }

        if self.pending_txs.has(store, tx_seq)? {
            return Ok(Some(Queue::Pending));
        }

        Ok(None)
    }

    pub async fn insert(&self, tx_seq: u64, queue: Queue) -> Result<InsertResult> {
        let async_store = self.store.write().await;
        let store = async_store.get_store();

        let mut tx = ConfigTx::default();

        match queue {
            Queue::Ready => {
                if !self.ready_txs.add(store, Some(&mut tx), tx_seq)? {
                    return Ok(InsertResult::AlreadyExists);
                }

                let removed = self.pending_txs.remove(store, Some(&mut tx), tx_seq)?;
                store.exec_configs(tx, DATA_DB_KEY)?;

                if removed {
                    Ok(InsertResult::Upgraded)
                } else {
                    Ok(InsertResult::NewAdded)
                }
            }
            Queue::Pending => {
                if !self.pending_txs.add(store, Some(&mut tx), tx_seq)? {
                    return Ok(InsertResult::AlreadyExists);
                }

                let removed = self.ready_txs.remove(store, Some(&mut tx), tx_seq)?;
                store.exec_configs(tx, DATA_DB_KEY)?;

                if removed {
                    Ok(InsertResult::Downgraded)
                } else {
                    Ok(InsertResult::NewAdded)
                }
            }
        }
    }

    pub async fn upgrade(&self, tx_seq: u64) -> Result<bool> {
        let async_store = self.store.write().await;
        let store = async_store.get_store();

        let mut tx = ConfigTx::default();

        if !self.pending_txs.remove(store, Some(&mut tx), tx_seq)? {
            return Ok(false);
        }

        let added = self.ready_txs.add(store, Some(&mut tx), tx_seq)?;

        store.exec_configs(tx, DATA_DB_KEY)?;

        Ok(added)
    }

    pub async fn random(&self) -> Result<Option<u64>> {
        let async_store = self.store.read().await;
        let store = async_store.get_store();

        // try to find a tx in ready queue with high priority
        if let Some(val) = self.ready_txs.random(store)? {
            return Ok(Some(val));
        }

        // otherwise, find tx in pending queue
        self.pending_txs.random(store)
    }

    pub async fn remove(&self, tx_seq: u64) -> Result<Option<Queue>> {
        let async_store = self.store.write().await;
        let store = async_store.get_store();

        // removed in ready queue
        if self.ready_txs.remove(store, None, tx_seq)? {
            return Ok(Some(Queue::Ready));
        }

        // removed in pending queue
        if self.pending_txs.remove(store, None, tx_seq)? {
            return Ok(Some(Queue::Pending));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::tests::TestStoreRuntime;

    use super::{InsertResult::*, Queue::*, SyncStore};

    #[tokio::test]
    async fn test_tx_seq_range() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // check values by default
        assert_eq!(store.get_tx_seq_range().await.unwrap(), (None, None));

        // update values
        store.set_next_tx_seq(4).await.unwrap();
        store.set_max_tx_seq(12).await.unwrap();

        // check values again
        assert_eq!(store.get_tx_seq_range().await.unwrap(), (Some(4), Some(12)));
    }

    #[tokio::test]
    async fn test_insert() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        assert_eq!(store.contains(1).await.unwrap(), None);
        assert_eq!(store.insert(1, Pending).await.unwrap(), NewAdded);
        assert_eq!(store.contains(1).await.unwrap(), Some(Pending));
        assert_eq!(store.insert(1, Pending).await.unwrap(), AlreadyExists);
        assert_eq!(store.insert(1, Ready).await.unwrap(), Upgraded);
        assert_eq!(store.contains(1).await.unwrap(), Some(Ready));

        assert_eq!(store.insert(2, Ready).await.unwrap(), NewAdded);
        assert_eq!(store.contains(2).await.unwrap(), Some(Ready));
        assert_eq!(store.insert(2, Ready).await.unwrap(), AlreadyExists);
        assert_eq!(store.insert(2, Pending).await.unwrap(), Downgraded);
        assert_eq!(store.contains(2).await.unwrap(), Some(Pending));
    }

    #[tokio::test]
    async fn test_upgrade() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // cannot upgrade by default
        assert!(!store.upgrade(3).await.unwrap());

        // add pending tx 3
        assert_eq!(store.insert(3, Pending).await.unwrap(), NewAdded);

        // can upgrade to ready
        assert!(store.upgrade(3).await.unwrap());
        assert_eq!(store.contains(3).await.unwrap(), Some(Ready));

        // cannot upgrade again
        assert!(!store.upgrade(3).await.unwrap());
    }

    #[tokio::test]
    async fn test_random() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // no tx by default
        assert_eq!(store.random().await.unwrap(), None);

        // add pending txs 1, 2, 3
        assert_eq!(store.insert(1, Pending).await.unwrap(), NewAdded);
        assert_eq!(store.insert(2, Pending).await.unwrap(), NewAdded);
        assert_eq!(store.insert(3, Pending).await.unwrap(), NewAdded);
        let tx = store.random().await.unwrap().unwrap();
        assert!((1..=3).contains(&tx));

        // upgrade tx 2 to ready
        assert_eq!(store.insert(2, Ready).await.unwrap(), Upgraded);
        assert_eq!(store.random().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn test_remove() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // cannot remove by default
        assert_eq!(store.remove(1).await.unwrap(), None);

        // add tx 1, 2
        assert_eq!(store.insert(1, Pending).await.unwrap(), NewAdded);
        assert_eq!(store.insert(2, Ready).await.unwrap(), NewAdded);

        // remove txs
        assert_eq!(store.remove(1).await.unwrap(), Some(Pending));
        assert_eq!(store.remove(1).await.unwrap(), None);
        assert_eq!(store.remove(2).await.unwrap(), Some(Ready));
        assert_eq!(store.remove(2).await.unwrap(), None);
    }
}
