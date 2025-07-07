use std::{net::SocketAddr, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub enabled: bool,
    pub listen_address: SocketAddr,
    pub listen_address_admin: SocketAddr,
    pub listen_address_grpc: SocketAddr,
    pub chunks_per_segment: usize,
    pub max_request_body_size: u32,
    pub max_cache_file_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_address: SocketAddr::from_str("0.0.0.0:5678").unwrap(),
            listen_address_admin: SocketAddr::from_str("127.0.0.1:5679").unwrap(),
            listen_address_grpc: SocketAddr::from_str("0.0.0.0:50051").unwrap(),
            chunks_per_segment: 1024,
            max_request_body_size: 100 * 1024 * 1024, // 100MB
            max_cache_file_size: 10 * 1024 * 1024,    // 10MB
        }
    }
}
