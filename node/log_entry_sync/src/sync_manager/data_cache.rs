use crate::sync_manager::config::CacheConfig;
use shared_types::DataRoot;
use std::cmp;
use std::collections::HashMap;

struct CachedData {
    /// Used for garbage collection.
    last_seen_tx_seq: u64,
    /// Complete data for a given DataRoot.
    data: Vec<u8>,
}

pub struct DataCache {
    root_to_data: HashMap<DataRoot, CachedData>,
    config: CacheConfig,
}

impl DataCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            root_to_data: HashMap::new(),
            config,
        }
    }

    pub fn add_data(&mut self, root: DataRoot, tx_seq: u64, data: Vec<u8>) -> bool {
        if data.len() > self.config.max_data_size {
            // large data are not cached.
            return false;
        }
        // TODO: insert partial data and merge here.
        self.root_to_data
            .entry(root)
            .and_modify(|cached| {
                cached.last_seen_tx_seq = cmp::max(tx_seq, cached.last_seen_tx_seq)
            })
            .or_insert(CachedData {
                last_seen_tx_seq: tx_seq,
                data,
            });
        true
    }

    /// Remove and return the data of a given `DataRoot`.
    /// If two completed reverted transactions have the same root and both appear later,
    /// the second one will have its data copied in `put_tx`.
    pub fn pop_data(&mut self, root: &DataRoot) -> Option<Vec<u8>> {
        self.root_to_data.remove(root).map(|e| e.data)
    }

    /// Remove timeout data entries according to TTL.
    pub fn garbage_collect(&mut self, latest_tx_seq: u64) {
        // We won't keep too many data, so it's okay to just iterate here.
        self.root_to_data.retain(|_, cached| {
            cached.last_seen_tx_seq + self.config.tx_seq_ttl as u64 >= latest_tx_seq
        })
    }
}
