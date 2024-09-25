use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::RwLock;
use zgs_spec::SEALS_PER_LOAD;

pub struct SealTaskManager {
    // TODO(kevin): This is an in-memory cache for recording which chunks are ready for sealing. It should be persisted on disk.
    pub to_seal_set: RwLock<BTreeMap<usize, u64>>,
    // Data sealing is an asynchronized process.
    // The sealing service uses the version number to distinguish if revert happens during sealing.
    to_seal_version: AtomicU64,
    last_pull_time: AtomicU64,
}

impl Default for SealTaskManager {
    fn default() -> Self {
        Self {
            to_seal_set: Default::default(),
            to_seal_version: Default::default(),
            last_pull_time: AtomicU64::new(current_timestamp()),
        }
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unexpected negative timestamp")
        .as_secs()
}

const SEAL_TASK_PULL_TIMEOUT_SECONDS: u64 = 300;

impl SealTaskManager {
    pub fn delete_batch_list(&self, batch_list: &[u64]) {
        let mut to_seal_set = self.to_seal_set.write();
        for batch_index in batch_list {
            for seal_index in (*batch_index as usize) * SEALS_PER_LOAD
                ..(*batch_index as usize + 1) * SEALS_PER_LOAD
            {
                to_seal_set.remove(&seal_index);
            }
        }
    }

    /// Record the latest timestamp that the miner thread pull seal tasks from the seal status.
    pub fn update_pull_time(&self) {
        // Here we only need an approximate timestamp and can tolerate a few seconds of error, so we used Ordering::Relaxed
        self.last_pull_time
            .store(current_timestamp(), Ordering::Relaxed)
    }

    pub fn seal_worker_available(&self) -> bool {
        let last_pull_time = self.last_pull_time.load(Ordering::Relaxed);
        current_timestamp().saturating_sub(last_pull_time) < SEAL_TASK_PULL_TIMEOUT_SECONDS
    }

    pub fn to_seal_version(&self) -> u64 {
        self.to_seal_version.load(Ordering::Acquire)
    }

    pub fn inc_seal_version(&self) -> u64 {
        self.to_seal_version.fetch_add(1, Ordering::AcqRel) + 1
    }
}
