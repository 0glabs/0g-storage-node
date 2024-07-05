use super::tx_store::TxStore;
use anyhow::{bail, Result};
use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};
use storage::log_store::config::{ConfigTx, ConfigurableExt};
use storage_async::Store;

const KEY_NEXT_TX_SEQ: &str = "sync.manager.next_tx_seq";
const KEY_MAX_TX_SEQ: &str = "sync.manager.max_tx_seq";

#[derive(Clone)]
pub struct SyncStore {
    store: Arc<RwLock<Store>>,

    /// Pending transactions to sync with low priority.
    pending_txs: TxStore,

    /// Ready transactions to sync with high priority since announcement
    /// already received from other peers.
    ready_txs: TxStore,
}

impl Debug for SyncStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let async_store = match self.store.read() {
            Ok(v) => v,
            Err(err) => return write!(f, "SyncStore {{ store error: {:?}}}", err),
        };
        let store = async_store.get_store();

        let pendings = match self.pending_txs.count(store) {
            Ok(count) => format!("{}", count),
            Err(err) => format!("Err: {:?}", err),
        };

        let ready = match self.ready_txs.count(store) {
            Ok(count) => format!("{}", count),
            Err(err) => format!("Err: {:?}", err),
        };

        f.debug_struct("SyncStore")
            .field("pending_txs", &pendings)
            .field("ready_txs", &ready)
            .finish()
    }
}

impl SyncStore {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
            pending_txs: TxStore::new("pending"),
            ready_txs: TxStore::new("ready"),
        }
    }

    pub async fn get_tx_seq_range(&self) -> Result<(Option<u64>, Option<u64>)> {
        let async_store = match self.store.read() {
            Ok(v) => v,
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        };
        let store = async_store.get_store();

        // load next_tx_seq
        let next_tx_seq = store.get_config_decoded(&KEY_NEXT_TX_SEQ)?;

        // load max_tx_seq
        let max_tx_seq = store.get_config_decoded(&KEY_MAX_TX_SEQ)?;

        Ok((next_tx_seq, max_tx_seq))
    }

    pub async fn set_next_tx_seq(&self, tx_seq: u64) -> Result<()> {
        match self.store.write() {
            Ok(store) => store
                .get_store()
                .set_config_encoded(&KEY_NEXT_TX_SEQ, &tx_seq),
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        }
    }

    pub async fn set_max_tx_seq(&self, tx_seq: u64) -> Result<()> {
        match self.store.write() {
            Ok(store) => store
                .get_store()
                .set_config_encoded(&KEY_MAX_TX_SEQ, &tx_seq),
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        }
    }

    pub async fn add_pending_tx(&self, tx_seq: u64) -> Result<bool> {
        let async_store = match self.store.write() {
            Ok(v) => v,
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        };
        let store = async_store.get_store();

        // already in ready queue
        if self.ready_txs.has(store, tx_seq)? {
            return Ok(false);
        }

        // always add in pending queue
        self.pending_txs.add(store, None, tx_seq)
    }

    pub async fn upgrade_tx_to_ready(&self, tx_seq: u64) -> Result<bool> {
        let async_store = match self.store.write() {
            Ok(v) => v,
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        };
        let store = async_store.get_store();

        let mut tx = ConfigTx::default();

        // not in pending queue
        if !self.pending_txs.remove(store, Some(&mut tx), tx_seq)? {
            return Ok(false);
        }

        // move from pending to ready queue
        let added = self.ready_txs.add(store, Some(&mut tx), tx_seq)?;

        store.exec_configs(tx)?;

        Ok(added)
    }

    pub async fn downgrade_tx_to_pending(&self, tx_seq: u64) -> Result<bool> {
        let async_store = match self.store.write() {
            Ok(v) => v,
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        };
        let store = async_store.get_store();

        let mut tx = ConfigTx::default();

        // not in ready queue
        if !self.ready_txs.remove(store, Some(&mut tx), tx_seq)? {
            return Ok(false);
        }

        // move from ready to pending queue
        let added = self.pending_txs.add(store, Some(&mut tx), tx_seq)?;

        store.exec_configs(tx)?;

        Ok(added)
    }

    pub async fn random_tx(&self) -> Result<Option<u64>> {
        let async_store = match self.store.read() {
            Ok(v) => v,
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        };
        let store = async_store.get_store();

        // try to find a tx in ready queue with high priority
        if let Some(val) = self.ready_txs.random(store)? {
            return Ok(Some(val));
        }

        // otherwise, find tx in pending queue
        self.pending_txs.random(store)
    }

    pub async fn remove_tx(&self, tx_seq: u64) -> Result<bool> {
        let async_store: std::sync::RwLockWriteGuard<Store> = match self.store.write() {
            Ok(v) => v,
            Err(err) => bail!("Failed to acquire rwlock, err = {:?}", err),
        };
        let store = async_store.get_store();

        // removed in ready queue
        if self.ready_txs.remove(store, None, tx_seq)? {
            return Ok(true);
        }

        // otherwise, try to remove in pending queue
        self.pending_txs.remove(store, None, tx_seq)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::tests::TestStoreRuntime;

    use super::SyncStore;

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
    async fn test_add_pending_tx() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // add pending tx 3
        assert!(store.add_pending_tx(3).await.unwrap());

        // cannot add pending tx 3 again
        assert!(!store.add_pending_tx(3).await.unwrap());
    }

    #[tokio::test]
    async fn test_upgrade_tx() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // cannot upgrade by default
        assert!(!store.upgrade_tx_to_ready(3).await.unwrap());

        // add pending tx 3
        assert!(store.add_pending_tx(3).await.unwrap());

        // can upgrade to ready
        assert!(store.upgrade_tx_to_ready(3).await.unwrap());

        // cannot add pending tx 3 again event upgraded to ready
        assert!(!store.add_pending_tx(3).await.unwrap());

        // cannot upgrade again
        assert!(!store.upgrade_tx_to_ready(3).await.unwrap());
    }

    #[tokio::test]
    async fn test_downgrade_tx() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // cannot downgrade by default
        assert!(!store.downgrade_tx_to_pending(3).await.unwrap());

        // add pending tx 3
        assert!(store.add_pending_tx(3).await.unwrap());

        // cannot downgrade for non-ready tx
        assert!(!store.downgrade_tx_to_pending(3).await.unwrap());

        // upgrade tx 3 to ready
        assert!(store.upgrade_tx_to_ready(3).await.unwrap());

        // can downgrade now
        assert!(store.downgrade_tx_to_pending(3).await.unwrap());

        // cannot downgrade now
        assert!(!store.downgrade_tx_to_pending(3).await.unwrap());
    }

    #[tokio::test]
    async fn test_random_tx() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // no tx by default
        assert_eq!(store.random_tx().await.unwrap(), None);

        // add pending txs 1, 2, 3
        assert!(store.add_pending_tx(1).await.unwrap());
        assert!(store.add_pending_tx(2).await.unwrap());
        assert!(store.add_pending_tx(3).await.unwrap());
        let tx = store.random_tx().await.unwrap().unwrap();
        assert!((1..=3).contains(&tx));

        // upgrade tx 1 to ready
        assert!(store.upgrade_tx_to_ready(2).await.unwrap());
        assert_eq!(store.random_tx().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn test_remove_tx() {
        let runtime = TestStoreRuntime::default();
        let store = SyncStore::new(runtime.store.clone());

        // cannot remove by default
        assert!(!store.remove_tx(1).await.unwrap());

        // add pending tx 1, 2
        assert!(store.add_pending_tx(1).await.unwrap());
        assert!(store.add_pending_tx(2).await.unwrap());

        // upgrade tx 1 to ready
        assert!(store.upgrade_tx_to_ready(1).await.unwrap());
        assert_eq!(store.random_tx().await.unwrap(), Some(1));

        // remove tx 1
        assert!(store.remove_tx(1).await.unwrap());
        assert_eq!(store.random_tx().await.unwrap(), Some(2));
        assert!(!store.remove_tx(1).await.unwrap());

        // remove tx 2
        assert!(store.remove_tx(2).await.unwrap());
        assert_eq!(store.random_tx().await.unwrap(), None);
        assert!(!store.remove_tx(2).await.unwrap());
    }
}
