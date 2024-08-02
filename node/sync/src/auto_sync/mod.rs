mod batcher;
pub mod batcher_random;
pub mod batcher_serial;
pub mod manager;
pub mod sync_store;
mod tx_store;

use std::time::Duration;

const INTERVAL_IDLE: Duration = Duration::from_secs(3);
const INTERVAL_ERROR: Duration = Duration::from_secs(10);
