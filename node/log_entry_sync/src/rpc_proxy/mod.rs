use anyhow::Result;
use async_trait::async_trait;
use ethereum_types::{H160, H256};
use ethers::prelude::Bytes;
use jsonrpsee::core::client::Subscription;

// TODO: Define accounts/filter/events as associated types?
// TODO: Define an abstraction suitable for other chains.
#[async_trait]
pub trait EvmRpcProxy {
    async fn call(&self, to: ContractAddress, data: Bytes) -> Result<Bytes>;

    async fn sub_events(&self, filter: SubFilter) -> Subscription<SubEvent>;
}

pub type ContractAddress = H160;

pub type Topic = H256;

#[allow(unused)]
pub struct SubFilter {
    to: Option<ContractAddress>,
    topics: Vec<Topic>,
}

#[allow(unused)]
pub struct SubEvent {
    /// Address
    pub address: ContractAddress,

    /// Topics
    pub topics: Vec<Topic>,

    /// Data
    pub data: Bytes,
}

pub(crate) mod eth;
