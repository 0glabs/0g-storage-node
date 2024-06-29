use crate::config::MineServiceMiddleware;
use crate::config::MinerConfig;
use contract_interface::{NewMinerIdFilter, PoraMine};
use ethereum_types::Address;
use ethers::contract::ContractCall;
use ethers::contract::EthEvent;
use std::sync::Arc;
use storage::H256;
use storage_async::Store;

const MINER_ID: &str = "mine.miner_id";

pub async fn load_miner_id(store: &Store) -> storage::error::Result<Option<H256>> {
    store.get_config_decoded(&MINER_ID).await
}

async fn set_miner_id(store: &Store, miner_id: &H256) -> storage::error::Result<()> {
    store.set_config_encoded(&MINER_ID, miner_id).await
}

pub(crate) async fn check_and_request_miner_id(
    config: &MinerConfig,
    store: &Store,
    provider: &Arc<MineServiceMiddleware>,
) -> Result<H256, String> {
    let db_miner_id = load_miner_id(store)
        .await
        .map_err(|e| format!("miner_id on db corrupt: {:?}", e))?;

    let mine_contract = PoraMine::new(config.mine_address, provider.clone());

    match (db_miner_id, config.miner_id) {
        (Some(d_id), Some(c_id)) => {
            if d_id != c_id {
                Err(format!(
                    "database miner id {} != configuration miner id {}",
                    d_id, c_id
                ))
            } else {
                Ok(d_id)
            }
        }
        (None, Some(c_id)) => {
            check_miner_id(&mine_contract, c_id).await?;
            set_miner_id(store, &c_id)
                .await
                .map_err(|e| format!("set miner id on db corrupt: {:?}", e))?;
            Ok(c_id)
        }
        (Some(d_id), None) => {
            check_miner_id(&mine_contract, d_id).await?;
            Ok(d_id)
        }
        (None, None) => {
            let beneficiary = provider.address();
            let id = request_miner_id(&mine_contract, beneficiary).await?;
            set_miner_id(store, &id)
                .await
                .map_err(|e| format!("set miner id on db corrupt: {:?}", e))?;
            Ok(id)
        }
    }
}

async fn check_miner_id(
    mine_contract: &PoraMine<MineServiceMiddleware>,
    miner_id: H256,
) -> Result<Address, String> {
    debug!("Checking miner id on chain...");

    let beneficiary = mine_contract
        .beneficiaries(miner_id.0)
        .call()
        .await
        .map_err(|e| format!("Fail to query miner id information: {:?}", e))?;

    if beneficiary == Address::zero() {
        Err("candidate miner id is not registered".into())
    } else {
        Ok(beneficiary)
    }
}

async fn request_miner_id(
    mine_contract: &PoraMine<MineServiceMiddleware>,
    beneficiary: Address,
) -> Result<H256, String> {
    debug!("Requesting miner id on chain...");

    let submission_call: ContractCall<_, _> =
        mine_contract.request_miner_id(beneficiary, 0).legacy();

    let pending_tx = submission_call
        .send()
        .await
        .map_err(|e| format!("Fail to request miner id: {:?}", e))?;

    let receipt = pending_tx
        .retries(3)
        .await
        .map_err(|e| format!("Fail to execute mine answer transaction: {:?}", e))?
        .ok_or("Request miner id transaction dropped after 3 retires")?;

    let first_log = receipt
        .logs
        .first()
        .ok_or("Fail to find minerId in receipt")?;

    let new_id_event = NewMinerIdFilter::decode_log(&first_log.clone().into())
        .map_err(|e| format!("Fail to decode NewMinerId event: {:?}", e))?;

    Ok(H256(new_id_event.miner_id))
}
