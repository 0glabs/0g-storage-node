use crate::rpc_proxy::ContractAddress;

pub struct LogSyncConfig {
    pub rpc_endpoint_url: String,
    pub contract_address: ContractAddress,
    pub cache_config: CacheConfig,

    /// The block number where we start to sync data.
    /// This is usually the block number when Zgs contract is deployed.
    pub start_block_number: u64,
    /// The number of blocks needed for confirmation on the blockchain.
    /// This is used to rollback to a stable height if reorg happens during node restart.
    /// TODO(zz): Some blockchains have better confirmation/finalization mechanisms.
    pub confirmation_block_count: u64,
    /// Maximum number of event logs to poll at a time.
    pub log_page_size: u64,

    // blockchain provider retry params
    // the number of retries after a connection times out
    pub rate_limit_retries: u32,
    // the nubmer of retries for rate limited responses
    pub timeout_retries: u32,
    // the duration to wait before retry, in ms
    pub initial_backoff: u64,
    // the duration between each paginated getLogs RPC call, in ms.
    // This is set to avoid triggering the throttling mechanism in the RPC server.
    pub recover_query_delay: u64,
}

#[derive(Clone)]
pub struct CacheConfig {
    /// The data with a size larger than this will not be cached.
    /// This is reasonable because uploading
    pub max_data_size: usize,
    pub tx_seq_ttl: usize,
}

impl LogSyncConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_endpoint_url: String,
        contract_address: ContractAddress,
        start_block_number: u64,
        confirmation_block_count: u64,
        cache_config: CacheConfig,
        log_page_size: u64,
        rate_limit_retries: u32,
        timeout_retries: u32,
        initial_backoff: u64,
        recover_query_delay: u64,
    ) -> Self {
        Self {
            rpc_endpoint_url,
            contract_address,
            cache_config,
            start_block_number,
            confirmation_block_count,
            log_page_size,
            rate_limit_retries,
            timeout_retries,
            initial_backoff,
            recover_query_delay,
        }
    }
}
