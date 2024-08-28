#[macro_use]
extern crate tracing;

mod batcher;
mod libp2p_event_handler;
mod metrics;
mod peer_manager;
mod service;

use duration_str::deserialize_duration;
use network::Multiaddr;
use serde::Deserialize;
use std::time::Duration;

pub use crate::service::RouterService;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    #[serde(deserialize_with = "deserialize_duration")]
    pub heartbeat_interval: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub idle_time: Duration,
    pub max_idle_incoming_peers: usize,
    pub max_idle_outgoing_peers: usize,
    pub libp2p_nodes: Vec<Multiaddr>,
    pub private_ip_enabled: bool,
    pub check_announced_ip: bool,

    // batcher
    /// Timeout to publish messages in batch
    #[serde(deserialize_with = "deserialize_duration")]
    pub batcher_timeout: Duration,
    /// Number of files in an announcement
    pub batcher_file_capacity: usize,
    /// Number of announcements in a pubsub message
    pub batcher_announcement_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            idle_time: Duration::from_secs(180),
            max_idle_incoming_peers: 12,
            max_idle_outgoing_peers: 20,
            libp2p_nodes: vec![],
            private_ip_enabled: false,
            check_announced_ip: false,

            batcher_timeout: Duration::from_secs(1),
            batcher_file_capacity: 1,
            batcher_announcement_capacity: 1,
        }
    }
}

impl Config {
    pub fn with_private_ip_enabled(mut self, enabled: bool) -> Self {
        self.private_ip_enabled = enabled;
        self
    }
}
