mod config_macro;

mod convert;
use config_macro::*;
use serde::Deserialize;
use std::ops::Deref;

build_config! {
    // network
    (network_dir, (String), "network".to_string())
    (network_listen_address, (String), "0.0.0.0".to_string())
    (network_enr_address, (Option<String>), None)
    (network_enr_tcp_port, (u16), 1234)
    (network_enr_udp_port, (u16), 1234)
    (network_libp2p_port, (u16), 1234)
    (network_discovery_port, (u16), 1234)
    (network_target_peers, (usize), 50)
    (network_boot_nodes, (Vec<String>), vec![])
    (network_libp2p_nodes, (Vec<String>), vec![])
    (network_private, (bool), false)
    (network_disable_discovery, (bool), false)
    (network_find_chunks_enabled, (bool), false)

    // discv5
    (discv5_request_timeout_secs, (u64), 5)
    (discv5_query_peer_timeout_secs, (u64), 2)
    (discv5_request_retries, (u8), 1)
    (discv5_query_parallelism, (usize), 5)
    (discv5_report_discovered_peers, (bool), false)
    (discv5_disable_packet_filter, (bool), false)
    (discv5_disable_ip_limit, (bool), false)
    (discv5_disable_enr_network_id, (bool), false)

    // log sync
    (blockchain_rpc_endpoint, (String), "http://127.0.0.1:8545".to_string())
    (log_contract_address, (String), "".to_string())
    (log_sync_start_block_number, (u64), 0)
    (force_log_sync_from_start_block_number, (bool), false)
    (confirmation_block_count, (u64), 3)
    (log_page_size, (u64), 999)
    (max_cache_data_size, (usize), 100 * 1024 * 1024) // 100 MB
    (cache_tx_seq_ttl, (usize), 500)

    (rate_limit_retries, (u32), 100)
    (timeout_retries, (u32), 100)
    (initial_backoff, (u64), 500)
    (recover_query_delay, (u64), 50)

    (default_finalized_block_count, (u64), 100)
    (remove_finalized_block_interval_minutes, (u64), 30)
    (watch_loop_wait_time_ms, (u64), 500)

    (blockchain_rpc_timeout_secs, (u64), 120)

    // chunk pool
    (chunk_pool_write_window_size, (usize), 4)
    (chunk_pool_max_cached_chunks_all, (usize), 4*1024*1024)    // 1G
    (chunk_pool_max_writings, (usize), 16)
    (chunk_pool_expiration_time_secs, (u64), 300)   // 5 minutes

    // db
    (db_dir, (String), "db".to_string())
    (db_max_num_sectors, (Option<usize>), None)
    (prune_check_time_s, (u64), 60)
    (prune_batch_size, (usize), 16 * 1024)
    (prune_batch_wait_time_ms, (u64), 1000)
    (merkle_node_cache_capacity, (usize), 32 * 1024 * 1024)

    // misc
    (log_config_file, (String), "log_config".to_string())
    (log_directory, (String), "log".to_string())

    // mine
    (mine_contract_address, (String), "".to_string())
    (miner_id, (Option<String>), None)
    (miner_key, (Option<String>), None)
    (miner_cpu_percentage, (u64), 100)
    (mine_iter_batch_size, (usize), 100)
    (reward_contract_address, (String), "".to_string())
    (shard_position, (Option<String>), None)

    (mine_context_query_seconds, (u64), 5)
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ZgsConfig {
    pub raw_conf: RawConfiguration,

    /// Network peer db config, configured by [network_peer_db] section by `config` crate.
    pub network_peer_db: network::peer_manager::peerdb::PeerDBConfig,

    /// Network peer manager config, configured by [network_peer_manager] section by `config` crate.
    pub network_peer_manager: network::peer_manager::config::Config,

    // router config, configured by [router] section by `config` crate.
    pub router: router::Config,

    // sync config, configured by [sync] section by `config` crate.
    pub sync: sync::Config,

    // file location cache config, configured by [file_location_cache] section by `config` crate.
    pub file_location_cache: file_location_cache::Config,

    // rpc config, configured by [rpc] section by `config` crate.
    pub rpc: rpc::RPCConfig,

    // submission config, configured by [submission_config] section by `config` crate.
    pub submission_config: contract_wrapper::SubmitConfig,

    // metrics config, configured by [metrics] section by `config` crate.
    pub metrics: metrics::MetricsConfiguration,
}

impl Deref for ZgsConfig {
    type Target = RawConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.raw_conf
    }
}

impl ZgsConfig {
    pub fn parse(matches: &clap::ArgMatches) -> Result<ZgsConfig, String> {
        let config_file = match matches.get_one::<String>("config") {
            Some(file) => file.as_str(),
            None => return Err("config file not specified".to_string()),
        };

        let mut config = config::Config::builder()
            .add_source(config::File::with_name(config_file))
            .add_source(
                config::Environment::with_prefix("ZGS_NODE")
                    .separator("__")
                    .list_separator(" "),
            )
            .build()
            .map_err(|e| format!("Failed to build config: {:?}", e))?
            .try_deserialize::<ZgsConfig>()
            .map_err(|e| format!("Failed to deserialize config: {:?}", e))?;

        config.raw_conf = RawConfiguration::parse(matches)?;

        Ok(config)
    }
}
