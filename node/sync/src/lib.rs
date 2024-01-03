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

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_sync_disabled: bool,
    pub max_sync_files: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub find_peer_timeout: Duration,
    pub enable_chunk_request: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auto_sync_disabled: false,
            max_sync_files: 100,
            find_peer_timeout: Duration::from_secs(30),
            enable_chunk_request: false,
        }
    }
}

impl Config {
    pub fn disable_auto_sync(mut self) -> Self {
        self.auto_sync_disabled = true;
        self
    }
}
