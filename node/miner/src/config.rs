use ethereum_types::{Address, H256, U256};
use ethers::core::k256::SecretKey;
use ethers::middleware::SignerMiddleware;
use ethers::providers::Http;
use ethers::providers::Middleware;
use ethers::providers::Provider;
use ethers::signers::LocalWallet;
use ethers::signers::Signer;

#[derive(Clone, Copy, Debug)]
pub struct ShardConfig {
    pub shard_id: usize,
    pub num_shard: usize,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shard_id: 0,
            num_shard: 1,
        }
    }
}

impl ShardConfig {
    pub fn new(shard_position: &Option<String>) -> Result<Self, String> {
        let (id, num) = if let Some(position) = shard_position {
            Self::parse_position(position)?
        } else {
            (0, 1)
        };

        if id >= num {
            return Err(format!(
                "Incorrect shard_id: expected [0, {}), actual {}",
                num, id
            ));
        }

        if !num.is_power_of_two() {
            return Err(format!(
                "Incorrect shard group bytes: {}, should be power of two",
                num
            ));
        }
        Ok(ShardConfig {
            shard_id: id,
            num_shard: num,
        })
    }

    pub fn shard_mask(&self) -> u64 {
        !(self.shard_id as u64)
    }

    pub fn parse_position(input: &str) -> Result<(usize, usize), String> {
        let parts: Vec<&str> = input.trim().split('/').map(|s| s.trim()).collect();

        if parts.len() != 2 {
            return Err("Incorrect format, expected like: '0 / 8'".into());
        }

        let numerator = parts[0]
            .parse::<usize>()
            .map_err(|e| format!("Cannot parse shard position {:?}", e))?;
        let denominator = parts[1]
            .parse::<usize>()
            .map_err(|e| format!("Cannot parse shard position {:?}", e))?;

        Ok((numerator, denominator))
    }
}

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
