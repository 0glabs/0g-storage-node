extern crate core;

pub(crate) mod rpc_proxy;
mod sync_manager;

pub use rpc_proxy::ContractAddress;
pub use sync_manager::{
    config::{CacheConfig, LogSyncConfig},
    LogSyncEvent, LogSyncManager,
};
