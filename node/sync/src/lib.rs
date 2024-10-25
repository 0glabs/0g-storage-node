#[macro_use]
extern crate tracing;

pub mod auto_sync;
mod context;
mod controllers;
mod service;
pub mod test_util;

use auto_sync::{batcher_random::RandomBatcherState, batcher_serial::SerialBatcherState};
pub use controllers::FileSyncInfo;
use duration_str::deserialize_duration;
use serde::{Deserialize, Serialize};
pub use service::{SyncMessage, SyncReceiver, SyncRequest, SyncResponse, SyncSender, SyncService};
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    // sync service config
    /// Indicates whether to sync file from neighbor nodes only.
    /// This is to avoid flooding file announcements in the whole network,
    /// which leads to high latency or even timeout to sync files.
    pub neighbors_only: bool,
    #[serde(deserialize_with = "deserialize_duration")]
    pub heartbeat_interval: Duration,
    pub auto_sync_enabled: bool,
    pub max_sync_files: usize,
    pub sync_file_by_rpc_enabled: bool,
    pub sync_file_on_announcement_enabled: bool,

    // serial sync config
    pub max_chunks_to_request: u64,
    pub max_request_failures: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub peer_connect_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub peer_disconnect_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub peer_find_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub peer_chunks_download_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub peer_wait_outgoing_connection_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub peer_next_chunks_request_wait_timeout: Duration,
    pub max_bandwidth_bytes: u64,
    #[serde(deserialize_with = "deserialize_duration")]
    pub bandwidth_wait_timeout: Duration,

    // auto sync config
    #[serde(deserialize_with = "deserialize_duration")]
    pub auto_sync_idle_interval: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub auto_sync_error_interval: Duration,
    pub max_sequential_workers: usize,
    pub max_random_workers: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub sequential_find_peer_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub random_find_peer_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // sync service config
            neighbors_only: false,
            heartbeat_interval: Duration::from_secs(5),
            auto_sync_enabled: false,
            max_sync_files: 8,
            sync_file_by_rpc_enabled: true,
            sync_file_on_announcement_enabled: false,

            // serial sync config
            max_chunks_to_request: 2 * 1024,
            max_request_failures: 5,
            peer_connect_timeout: Duration::from_secs(15),
            peer_disconnect_timeout: Duration::from_secs(15),
            peer_find_timeout: Duration::from_secs(120),
            peer_chunks_download_timeout: Duration::from_secs(15),
            peer_wait_outgoing_connection_timeout: Duration::from_secs(10),
            peer_next_chunks_request_wait_timeout: Duration::from_secs(3),
            max_bandwidth_bytes: 0,
            bandwidth_wait_timeout: Duration::from_secs(5),

            // auto sync config
            auto_sync_idle_interval: Duration::from_secs(3),
            auto_sync_error_interval: Duration::from_secs(10),
            max_sequential_workers: 0,
            max_random_workers: 2,
            sequential_find_peer_timeout: Duration::from_secs(60),
            random_find_peer_timeout: Duration::from_secs(500),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct InstantWrapper(Instant);

impl Debug for InstantWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{} seconds ago\"", self.0.elapsed().as_secs())
    }
}

impl From<Instant> for InstantWrapper {
    fn from(value: Instant) -> Self {
        Self(value)
    }
}

impl InstantWrapper {
    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncServiceState {
    pub num_syncing: usize,
    pub catched_up: Option<bool>,
    pub auto_sync_serial: Option<SerialBatcherState>,
    pub auto_sync_random: Option<RandomBatcherState>,
}
