mod peers;
mod serial;

use std::collections::HashMap;

use peers::PeerState;
use serde::{Deserialize, Serialize};

pub use serial::{FailureReason, SerialSyncController, SyncState, MAX_CHUNKS_TO_REQUEST};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncGoal {
    /// File chunks in total.
    pub num_chunks: u64,
    /// Chunk index to sync from (starts from 0, inclusive).
    pub index_start: u64,
    /// Chunk index to sync to (exclusive).
    pub index_end: u64,
}

impl FileSyncGoal {
    pub fn new(num_chunks: u64, index_start: u64, index_end: u64) -> Self {
        assert!(
            index_start < index_end && index_end <= num_chunks,
            "invalid index_end"
        );
        Self {
            num_chunks,
            index_start,
            index_end,
        }
    }

    pub fn new_file(num_chunks: u64) -> Self {
        Self::new(num_chunks, 0, num_chunks)
    }

    pub fn is_all_chunks(&self) -> bool {
        self.index_start == 0 && self.index_end == self.num_chunks
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncInfo {
    pub elapsed_secs: u64,
    pub peers: HashMap<PeerState, u64>,
    pub goal: FileSyncGoal,
    pub next_chunks: u64,
    pub state: String,
}
