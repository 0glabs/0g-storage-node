#![allow(clippy::field_reassign_with_default)]

use crate::ZgsConfig;
use ethereum_types::{H256, U256};
use log_entry_sync::{CacheConfig, ContractAddress, LogSyncConfig};
use miner::MinerConfig;
use network::NetworkConfig;
use pruner::PrunerConfig;
use rpc::RPCConfig;
use std::net::IpAddr;
use std::time::Duration;
use storage::config::ShardConfig;
use storage::StorageConfig;

impl ZgsConfig {
    pub async fn network_config(&self) -> Result<NetworkConfig, String> {
        let mut network_config = NetworkConfig::default();

        network_config.listen_address = self
            .network_listen_address
            .parse::<std::net::IpAddr>()
            .map_err(|e| format!("Unable to parse network_listen_address: {:?}", e))?;

        network_config.network_dir = self.network_dir.clone().into();
        network_config.libp2p_port = self.network_libp2p_port;
        network_config.disable_discovery = self.network_disable_discovery;
        network_config.discovery_port = self.network_discovery_port;

        if !self.network_disable_discovery {
            network_config.enr_tcp_port = Some(self.network_enr_tcp_port);
            network_config.enr_udp_port = Some(self.network_enr_udp_port);
            network_config.enr_address = match &self.network_enr_address {
                Some(addr) => Some(addr.parse().unwrap()),
                None => match public_ip::addr_v4().await {
                    Some(ipv4_addr) => {
                        info!(?ipv4_addr, "Auto detect public IP as ENR address");
                        Some(IpAddr::V4(ipv4_addr))
                    }
                    None => {
                        return Err(
                            "ENR address not configured and failed to detect public IP address"
                                .into(),
                        )
                    }
                },
            };
        }

        network_config.boot_nodes_multiaddr = self
            .network_boot_nodes
            .iter()
            .map(|addr| addr.parse::<libp2p::Multiaddr>())
            .collect::<Result<_, _>>()
            .map_err(|e| format!("Unable to parse network_boot_nodes: {:?}", e))?;

        network_config.libp2p_nodes = self
            .network_libp2p_nodes
            .iter()
            .map(|addr| addr.parse::<libp2p::Multiaddr>())
            .collect::<Result<_, _>>()
            .map_err(|e| format!("Unable to parse network_libp2p_nodes: {:?}", e))?;

        network_config.discv5_config.table_filter = |_| true;
        network_config.discv5_config.request_timeout =
            Duration::from_secs(self.discv5_request_timeout_secs);
        network_config.discv5_config.query_peer_timeout =
            Duration::from_secs(self.discv5_query_peer_timeout_secs);
        network_config.discv5_config.request_retries = self.discv5_request_retries;
        network_config.discv5_config.query_parallelism = self.discv5_query_parallelism;
        network_config.discv5_config.report_discovered_peers = self.discv5_report_discovered_peers;
        network_config.discv5_config.enable_packet_filter = !self.discv5_disable_packet_filter;
        network_config.discv5_config.ip_limit = !self.discv5_disable_ip_limit;

        network_config.target_peers = self.network_target_peers;
        network_config.private = self.network_private;

        Ok(network_config)
    }

    pub fn storage_config(&self) -> Result<StorageConfig, String> {
        Ok(StorageConfig {
            db_dir: self.db_dir.clone().into(),
        })
    }

    pub fn rpc_config(&self) -> Result<RPCConfig, String> {
        let listen_address = self
            .rpc_listen_address
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("Unable to parse rpc_listen_address: {:?}", e))?;

        let listen_address_admin = if self.rpc_listen_address_admin.is_empty() {
            None
        } else {
            Some(
                self.rpc_listen_address_admin
                    .parse::<std::net::SocketAddr>()
                    .map_err(|e| format!("Unable to parse rpc_listen_address_admin: {:?}", e))?,
            )
        };

        Ok(RPCConfig {
            enabled: self.rpc_enabled,
            listen_address,
            listen_address_admin,
            max_request_body_size: self.max_request_body_size,
            chunks_per_segment: self.rpc_chunks_per_segment,
            max_cache_file_size: self.rpc_max_cache_file_size,
        })
    }

    pub fn log_sync_config(&self) -> Result<LogSyncConfig, String> {
        let contract_address = self
            .log_contract_address
            .parse::<ContractAddress>()
            .map_err(|e| format!("Unable to parse log_contract_address: {:?}", e))?;
        let cache_config = CacheConfig {
            // 100 MB.
            max_data_size: self.max_cache_data_size,
            // This should be enough if we have about one Zgs tx per block.
            tx_seq_ttl: self.cache_tx_seq_ttl,
        };
        Ok(LogSyncConfig::new(
            self.blockchain_rpc_endpoint.clone(),
            contract_address,
            self.log_sync_start_block_number,
            self.confirmation_block_count,
            cache_config,
            self.log_page_size,
            self.rate_limit_retries,
            self.timeout_retries,
            self.initial_backoff,
            self.recover_query_delay,
            self.default_finalized_block_count,
            self.remove_finalized_block_interval_minutes,
            self.watch_loop_wait_time_ms,
        ))
    }

    pub fn mine_config(&self) -> Result<Option<MinerConfig>, String> {
        let flow_address = self
            .log_contract_address
            .parse::<ContractAddress>()
            .map_err(|e| format!("Unable to parse log_contract_address: {:?}", e))?;
        let mine_address = self
            .mine_contract_address
            .parse::<ContractAddress>()
            .map_err(|e| format!("Unable to parse mine_address: {:?}", e))?;

        let miner_id = if let Some(ref miner_id) = self.miner_id {
            Some(
                miner_id
                    .parse::<H256>()
                    .map_err(|e| format!("Unable to parse miner_id: {:?}", e))?,
            )
        } else {
            None
        };
        let miner_key = if let Some(ref miner_key) = self.miner_key {
            Some(
                miner_key
                    .parse::<H256>()
                    .map_err(|e| format!("Unable to parse miner_key: {:?}", e))?,
            )
        } else {
            None
        };
        let submission_gas = self.miner_submission_gas.map(U256::from);
        let cpu_percentage = self.miner_cpu_percentage;
        let iter_batch = self.mine_iter_batch_size;

        let shard_config = self.shard_config()?;

        Ok(MinerConfig::new(
            miner_id,
            miner_key,
            self.blockchain_rpc_endpoint.clone(),
            mine_address,
            flow_address,
            submission_gas,
            cpu_percentage,
            iter_batch,
            shard_config,
        ))
    }

    pub fn chunk_pool_config(&self) -> Result<chunk_pool::Config, String> {
        Ok(chunk_pool::Config {
            write_window_size: self.chunk_pool_write_window_size,
            max_cached_chunks_all: self.chunk_pool_max_cached_chunks_all,
            max_writings: self.chunk_pool_max_writings,
            expiration_time_secs: self.chunk_pool_expiration_time_secs,
            shard_config: self.shard_config()?,
        })
    }

    pub fn router_config(&self, network_config: &NetworkConfig) -> Result<router::Config, String> {
        let mut router_config = self.router.clone();
        router_config.libp2p_nodes = network_config.libp2p_nodes.to_vec();
        Ok(router_config)
    }

    pub fn pruner_config(&self) -> Result<Option<PrunerConfig>, String> {
        if let Some(max_num_sectors) = self.db_max_num_sectors {
            let shard_config = self.shard_config()?;
            Ok(Some(PrunerConfig {
                shard_config,
                db_path: self.db_dir.clone().into(),
                max_num_sectors,
                check_time: Duration::from_secs(self.prune_check_time_s),
                batch_size: self.prune_batch_size,
                batch_wait_time: Duration::from_millis(self.prune_batch_wait_time_ms),
            }))
        } else {
            Ok(None)
        }
    }

    fn shard_config(&self) -> Result<ShardConfig, String> {
        ShardConfig::new(&self.shard_position)
    }
}
