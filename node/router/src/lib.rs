#[macro_use]
extern crate tracing;

mod libp2p_event_handler;
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
        }
    }
}

impl Config {
    pub fn with_private_ip_enabled(mut self, enabled: bool) -> Self {
        self.private_ip_enabled = enabled;
        self
    }
}
