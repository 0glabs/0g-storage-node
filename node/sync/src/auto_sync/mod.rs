mod batcher;
mod batcher_serial;
mod manager;
mod sync_store;
mod tx_store;

use std::time::Duration;

pub use manager::Manager as AutoSyncManager;

const INTERVAL_IDLE: Duration = Duration::from_secs(3);
const INTERVAL_ERROR: Duration = Duration::from_secs(10);
