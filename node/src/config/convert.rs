#![allow(clippy::field_reassign_with_default)]

use crate::ZgsConfig;
use ethereum_types::H256;
use ethers::prelude::{Http, Middleware, Provider};
use log_entry_sync::{CacheConfig, ContractAddress, LogSyncConfig};
use miner::MinerConfig;
use network::{EnrExt, NetworkConfig};
use pruner::PrunerConfig;
use shared_types::{NetworkIdentity, ProtocolVersion};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use storage::config::ShardConfig;
use storage::log_store::log_manager::LogConfig;
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
        let flow_address = self
            .log_contract_address
            .parse::<ContractAddress>()
            .map_err(|e| format!("Unable to parse log_contract_address: {:?}", e))?;
        let provider = Provider::<Http>::try_from(&self.blockchain_rpc_endpoint)
            .map_err(|e| format!("Can not parse blockchain endpoint: {:?}", e))?;
        let chain_id = provider
            .get_chainid()
            .await
            .map_err(|e| format!("Unable to get chain id: {:?}", e))?
            .as_u64();
        let local_network_id = NetworkIdentity {
            chain_id,
            flow_address,
            p2p_protocol_version: ProtocolVersion {
                major: network::PROTOCOL_VERSION_V4[0],
                minor: network::PROTOCOL_VERSION_V4[1],
                build: network::PROTOCOL_VERSION_V4[2],
            },
        };
        network_config.network_id = local_network_id.clone();

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

        network_config.discv5_config.table_filter = if self.discv5_disable_enr_network_id {
            Arc::new(|_| true)
        } else {
            Arc::new(
                move |enr| matches!(enr.network_identity(), Some(Ok(id)) if id == local_network_id),
            )
        };
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

        network_config.peer_db = self.network_peer_db;
        network_config.peer_manager = self.network_peer_manager.clone();
        network_config.disable_enr_network_id = self.discv5_disable_enr_network_id;
        network_config.find_chunks_enabled = self.network_find_chunks_enabled;

        Ok(network_config)
    }

    pub fn storage_config(&self) -> Result<StorageConfig, String> {
        let mut log_config = LogConfig::default();
        log_config.flow.merkle_node_cache_capacity = self.merkle_node_cache_capacity;
        Ok(StorageConfig {
            db_dir: self.db_dir.clone().into(),
            log_config,
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
            self.force_log_sync_from_start_block_number,
            Duration::from_secs(self.blockchain_rpc_timeout_secs),
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
        let cpu_percentage = self.miner_cpu_percentage;
        let iter_batch = self.mine_iter_batch_size;
        let context_query_seconds = self.mine_context_query_seconds;

        let shard_config = self.shard_config()?;

        Ok(MinerConfig::new(
            miner_id,
            miner_key,
            self.blockchain_rpc_endpoint.clone(),
            mine_address,
            flow_address,
            cpu_percentage,
            iter_batch,
            context_query_seconds,
            shard_config,
            self.rate_limit_retries,
            self.timeout_retries,
            self.initial_backoff,
            self.submission_config,
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

        if router_config.public_address.is_none() {
            if let Some(addr) = &self.network_enr_address {
                router_config.public_address = Some(addr.parse().unwrap());
            }
        }

        Ok(router_config)
    }

    pub fn pruner_config(&self) -> Result<Option<PrunerConfig>, String> {
        if let Some(max_num_sectors) = self.db_max_num_sectors {
            let shard_config = self.shard_config()?;
            let reward_address = self
                .reward_contract_address
                .parse::<ContractAddress>()
                .map_err(|e| format!("Unable to parse reward_contract_address: {:?}", e))?;
            Ok(Some(PrunerConfig {
                shard_config,
                db_path: self.db_dir.clone().into(),
                max_num_sectors,
                check_time: Duration::from_secs(self.prune_check_time_s),
                batch_size: self.prune_batch_size,
                batch_wait_time: Duration::from_millis(self.prune_batch_wait_time_ms),
                rpc_endpoint_url: self.blockchain_rpc_endpoint.clone(),
                reward_address,
                rate_limit_retries: self.rate_limit_retries,
                timeout_retries: self.timeout_retries,
                initial_backoff: self.initial_backoff,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn shard_config(&self) -> Result<ShardConfig, String> {
        self.shard_position.clone().try_into()
    }
}
