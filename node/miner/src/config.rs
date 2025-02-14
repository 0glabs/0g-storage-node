use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use contract_wrapper::SubmitConfig;
use ethereum_types::{Address, H256};
use ethers::core::k256::SecretKey;
use ethers::middleware::SignerMiddleware;
use ethers::providers::Http;
use ethers::providers::HttpRateLimitRetryPolicy;
use ethers::providers::Middleware;
use ethers::providers::Provider;
use ethers::providers::RetryClient;
use ethers::providers::RetryClientBuilder;
use ethers::signers::LocalWallet;
use ethers::signers::Signer;
use storage::config::ShardConfig;

pub struct MinerConfig {
    pub(crate) miner_id: Option<H256>,
    pub(crate) miner_key: H256,
    pub(crate) rpc_endpoint_url: String,
    pub(crate) mine_address: Address,
    pub(crate) flow_address: Address,
    pub(crate) cpu_percentage: u64,
    pub(crate) iter_batch: usize,
    pub(crate) shard_config: ShardConfig,
    pub(crate) context_query_interval: Duration,
    pub(crate) rate_limit_retries: u32,
    pub(crate) timeout_retries: u32,
    pub(crate) initial_backoff: u64,
    pub(crate) submission_config: SubmitConfig,
}

pub type MineServiceMiddleware = SignerMiddleware<Arc<Provider<RetryClient<Http>>>, LocalWallet>;

impl MinerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        miner_id: Option<H256>,
        miner_key: Option<H256>,
        rpc_endpoint_url: String,
        mine_address: Address,
        flow_address: Address,
        cpu_percentage: u64,
        iter_batch: usize,
        context_query_seconds: u64,
        shard_config: ShardConfig,
        rate_limit_retries: u32,
        timeout_retries: u32,
        initial_backoff: u64,
        submission_config: SubmitConfig,
    ) -> Option<MinerConfig> {
        miner_key.map(|miner_key| MinerConfig {
            miner_id,
            miner_key,
            rpc_endpoint_url,
            mine_address,
            flow_address,
            cpu_percentage,
            iter_batch,
            shard_config,
            context_query_interval: Duration::from_secs(context_query_seconds),
            rate_limit_retries,
            timeout_retries,
            initial_backoff,
            submission_config,
        })
    }

    pub(crate) fn make_provider(&self) -> Result<Arc<Provider<RetryClient<Http>>>, String> {
        Ok(Arc::new(Provider::new(
            RetryClientBuilder::default()
                .rate_limit_retries(self.rate_limit_retries)
                .timeout_retries(self.timeout_retries)
                .initial_backoff(Duration::from_millis(self.initial_backoff))
                .build(
                    Http::from_str(&self.rpc_endpoint_url)
                        .map_err(|e| format!("Cannot parse blockchain endpoint: {:?}", e))?,
                    Box::new(HttpRateLimitRetryPolicy),
                ),
        )))
    }

    pub(crate) async fn make_signing_provider(&self) -> Result<MineServiceMiddleware, String> {
        let provider = self.make_provider()?;
        let chain_id = provider
            .get_chainid()
            .await
            .map_err(|e| format!("Unable to get chain_id: {:?}", e))?;
        let secret_key = SecretKey::from_bytes(self.miner_key.as_ref().into())
            .map_err(|e| format!("Cannot parse private key: {:?}", e))?;
        let signer = LocalWallet::from(secret_key).with_chain_id(chain_id.as_u64());
        let middleware = SignerMiddleware::new(provider, signer);

        Ok(middleware)
    }
}
