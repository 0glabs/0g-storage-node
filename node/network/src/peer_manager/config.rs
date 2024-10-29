use std::time::Duration;

use duration_str::deserialize_duration;
use serde::{Deserialize, Serialize};

/// The time in seconds between re-status's peers.
pub const DEFAULT_STATUS_INTERVAL: u64 = 300;

/// Default ping interval for outbound connections, in seconds.
pub const DEFAULT_PING_INTERVAL_OUTBOUND: u64 = 15;

/// Default interval for inbound connections.
pub const DEFAULT_PING_INTERVAL_INBOUND: u64 = 20;

/// Default number of peers to connect to.
pub const DEFAULT_TARGET_PEERS: usize = 50;

/// Configurations for the PeerManager.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /* Peer count related configurations */
    /// The heartbeat performs regular updates such as updating reputations and performing discovery
    /// requests. This defines the interval in seconds.
    #[serde(deserialize_with = "deserialize_duration")]
    pub heartbeat_interval: Duration,
    /// Whether discovery is enabled.
    pub discovery_enabled: bool,
    /// Whether metrics are enabled.
    pub metrics_enabled: bool,
    /// Target number of peers to connect to.
    pub target_peer_count: usize,

    /* RPC related configurations */
    /// Time in seconds between status requests sent to peers.
    pub status_interval: u64,
    /// The time in seconds between PING events. We do not send a ping if the other peer has PING'd
    /// us within this time frame (Seconds). This is asymmetric to avoid simultaneous pings. This
    /// interval applies to inbound connections: those in which we are not the dialer.
    pub ping_interval_inbound: u64,
    /// Interval between PING events for peers dialed by us.
    pub ping_interval_outbound: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_interval: Duration::from_secs(30),
            discovery_enabled: true,
            metrics_enabled: false,
            target_peer_count: DEFAULT_TARGET_PEERS,
            status_interval: DEFAULT_STATUS_INTERVAL,
            ping_interval_inbound: DEFAULT_PING_INTERVAL_INBOUND,
            ping_interval_outbound: DEFAULT_PING_INTERVAL_OUTBOUND,
        }
    }
}
