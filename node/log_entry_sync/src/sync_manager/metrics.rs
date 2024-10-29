use std::sync::Arc;

use metrics::{register_timer, Gauge, GaugeUsize, Timer};

lazy_static::lazy_static! {
    pub static ref LOG_MANAGER_HANDLE_DATA_TRANSACTION: Arc<dyn Timer> = register_timer("log_manager_handle_data_transaction");

    pub static ref STORE_PUT_TX: Arc<dyn Timer> = register_timer("log_entry_sync_manager_put_tx_inner");

    pub static ref STORE_PUT_TX_SPEED_IN_BYTES: Arc<dyn Gauge<usize>> = GaugeUsize::register("log_entry_sync_manager_put_tx_speed_in_bytes");

    pub static ref RECOVER_LOG: Arc<dyn Timer> = register_timer("log_entry_sync_manager_recover_log");
}
