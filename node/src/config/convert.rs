#![allow(clippy::field_reassign_with_default)]

use crate::ZgsConfig;
use ethereum_types::{U256, H256};
use log_entry_sync::{CacheConfig, ContractAddress, LogSyncConfig};
use miner::MinerConfig;
use network::NetworkConfig;
use rpc::RPCConfig;
use storage::StorageConfig;

impl ZgsConfig {
    pub fn network_config(&self) -> Result<NetworkConfig, String> {
        let mut network_config = NetworkConfig::default();

        network_config.listen_address = self
            .network_listen_address
            .parse::<std::net::IpAddr>()
            .map_err(|e| format!("Unable to parse network_listen_address: {:?}", e))?;

        network_config.network_dir = self.network_dir.clone().into();
        network_config.libp2p_port = self.network_libp2p_port;
        network_config.disable_discovery = self.network_disable_discovery;
        network_config.discovery_port = self.network_discovery_port;
        network_config.enr_tcp_port = self.network_enr_tcp_port;
        network_config.enr_udp_port = self.network_enr_udp_port;
        network_config.enr_address = self
            .network_enr_address
            .as_ref()
            .map(|x| x.parse::<std::net::IpAddr>().unwrap());

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

        // TODO
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
        let submission_gas = if let Some(ref gas_limit) = self.miner_submission_gas {
            Some(gas_limit.parse::<U256>().map_err(|e| format!("Unable to parse miner_submission_gas: {:?}", e))?)
        } else {
            None
        };
        Ok(MinerConfig::new(
            miner_id,
            miner_key,
            self.blockchain_rpc_endpoint.clone(),
            mine_address,
            flow_address,
            submission_gas
        ))
    }

    pub fn chunk_pool_config(&self) -> chunk_pool::Config {
        chunk_pool::Config {
            write_window_size: self.chunk_pool_write_window_size,
            max_cached_chunks_all: self.chunk_pool_max_cached_chunks_all,
            max_writings: self.chunk_pool_max_writings,
            expiration_time_secs: self.chunk_pool_expiration_time_secs,
        }
    }

    pub fn router_config(&self, network_config: &NetworkConfig) -> Result<router::Config, String> {
        let mut router_config = router::Config::default();
        router_config.libp2p_nodes = network_config.libp2p_nodes.to_vec();
        Ok(router_config)
    }
}
