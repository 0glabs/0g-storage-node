#[macro_use]
extern crate tracing;

pub mod auto_sync;
mod context;
mod controllers;
mod service;
pub mod test_util;

pub use controllers::FileSyncInfo;
use duration_str::deserialize_duration;
use serde::Deserialize;
pub use service::{SyncMessage, SyncReceiver, SyncRequest, SyncResponse, SyncSender, SyncService};
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_sync_enabled: bool,
    pub max_sync_files: usize,
    pub sync_file_by_rpc_enabled: bool,
    pub sync_file_on_announcement_enabled: bool,

    // auto_sync config
    pub max_sequential_workers: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub find_peer_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auto_sync_enabled: false,
            max_sync_files: 16,
            sync_file_by_rpc_enabled: true,
            sync_file_on_announcement_enabled: false,

            max_sequential_workers: 8,
            find_peer_timeout: Duration::from_secs(10),
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
