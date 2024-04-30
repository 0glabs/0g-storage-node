use ethereum_types::{Address, H256, U256};
use ethers::core::k256::SecretKey;
use ethers::middleware::SignerMiddleware;
use ethers::providers::Http;
use ethers::providers::Middleware;
use ethers::providers::Provider;
use ethers::signers::LocalWallet;
use ethers::signers::Signer;
use zgs_spec::BYTES_PER_LOAD;

#[derive(Clone, Copy, Debug)]
pub struct ShardConfig {
    pub shard_group_load_chunks: usize,
    pub shard_id: usize,
    pub num_shard: usize,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shard_group_load_chunks: 1,
            shard_id: 0,
            num_shard: 1,
        }
    }
}

impl ShardConfig {
    pub fn new(
        shard_group_bytes: Option<usize>,
        shard_position: &Option<String>,
    ) -> Result<Option<Self>, String> {
        let (group_bytes, (id, num)) = match (shard_group_bytes, shard_position) {
            (None, None) => {
                return Ok(None);
            }
            (Some(bytes), Some(position)) => (bytes, Self::parse_position(position)?),
            _ => {
                return Err(
                    "`shard_group_bytes` and `shard_position` should be set simultaneously".into(),
                );
            }
        };

        if group_bytes < BYTES_PER_LOAD || group_bytes & (group_bytes - 1) != 0 {
            return Err(format!(
                "Incorrect shard group bytes: {}, should be power of two and >= {}",
                group_bytes, BYTES_PER_LOAD
            ));
        }

        let group_chunks = group_bytes / BYTES_PER_LOAD;

        if id >= num {
            return Err(format!(
                "Incorrect shard_id: expected [0, {}), actual {}",
                num, id
            ));
        }

        if group_chunks < num {
            return Err(format!("Incorrect shard_group_number: the shard group contains {} loading chunks, which cannot be divided into {} shards", group_chunks, num));
        }

        Ok(Some(ShardConfig {
            shard_group_load_chunks: group_chunks,
            shard_id: id,
            num_shard: num,
        }))
    }

    pub fn shard_chunks(&self) -> u64 {
        (self.shard_group_load_chunks / self.num_shard) as u64
    }

    pub fn shard_mask(&self) -> u64 {
        let x = self.shard_group_load_chunks as u64 - self.shard_chunks();
        !x
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
    pub(crate) shard_config: Option<ShardConfig>,
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
        shard_config: Option<ShardConfig>,
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
