mod peers;
mod serial;

use serde::{Deserialize, Serialize};

pub use serial::{FailureReason, SerialSyncController, SyncState};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncInfo {
    pub elapsed_secs: u64,
    pub peers: usize,
    pub num_chunks: u64,
    pub next_chunks: u64,
    pub state: String,
}
