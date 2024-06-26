extern crate core;

mod sync_manager;

use ethers::prelude::H160;
pub use sync_manager::{
    config::{CacheConfig, LogSyncConfig},
    LogSyncEvent, LogSyncManager,
};

pub type ContractAddress = H160;
