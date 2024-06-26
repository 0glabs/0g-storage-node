#[macro_use]
extern crate tracing;

mod auto_sync;
mod context;
mod controllers;
mod service;
pub mod test_util;

pub use controllers::FileSyncInfo;
use duration_str::deserialize_duration;
use serde::Deserialize;
pub use service::{SyncMessage, SyncReceiver, SyncRequest, SyncResponse, SyncSender, SyncService};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_sync_enabled: bool,
    pub max_sync_files: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub find_peer_timeout: Duration,
    pub sync_file_by_rpc_enabled: bool,
    pub sync_file_on_announcement_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auto_sync_enabled: false,
            max_sync_files: 8,
            find_peer_timeout: Duration::from_secs(10),
            sync_file_by_rpc_enabled: true,
            sync_file_on_announcement_enabled: false,
        }
    }
}
