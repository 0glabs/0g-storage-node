use std::time::Duration;

use ethereum_types::{Address, H256, U256};
use ethers::core::k256::SecretKey;
use ethers::middleware::SignerMiddleware;
use ethers::providers::Http;
use ethers::providers::Middleware;
use ethers::providers::Provider;
use ethers::signers::LocalWallet;
use ethers::signers::Signer;
use storage::config::ShardConfig;

pub struct MinerConfig {
    pub(crate) miner_id: Option<H256>,
    pub(crate) miner_key: H256,
    pub(crate) rpc_endpoint_url: String,
    pub(crate) mine_address: Address,
    pub(crate) flow_address: Address,
    pub(crate) submission_gas: Option<U256>,
    pub(crate) cpu_percentage: u64,
    pub(crate) iter_batch: usize,
    pub(crate) shard_config: ShardConfig,
    pub(crate) context_query_interval: Duration,
}

pub type MineServiceMiddleware = SignerMiddleware<Provider<Http>, LocalWallet>;

impl MinerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        miner_id: Option<H256>,
        miner_key: Option<H256>,
        rpc_endpoint_url: String,
        mine_address: Address,
        flow_address: Address,
        submission_gas: Option<U256>,
        cpu_percentage: u64,
        iter_batch: usize,
        context_query_seconds: u64,
        shard_config: ShardConfig,
    ) -> Option<MinerConfig> {
        miner_key.map(|miner_key| MinerConfig {
            miner_id,
            miner_key,
            rpc_endpoint_url,
            mine_address,
            flow_address,
            submission_gas,
            cpu_percentage,
            iter_batch,
            shard_config,
            context_query_interval: Duration::from_secs(context_query_seconds),
        })
    }

    pub(crate) async fn make_provider(&self) -> Result<MineServiceMiddleware, String> {
        let provider = Provider::<Http>::try_from(&self.rpc_endpoint_url)
            .map_err(|e| format!("Can not parse blockchain endpoint: {:?}", e))?;
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
