use std::net::SocketAddr;

#[derive(Clone)]
pub struct Config {
    pub enabled: bool,
    pub listen_address: SocketAddr,
    pub chunks_per_segment: usize,
    pub max_cache_file_size: usize,
}
