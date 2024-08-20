use std::sync::Arc;

use metrics::{register_timer, Timer};

lazy_static::lazy_static! {
    pub static ref STORE_PUT_TX: Arc<dyn Timer> = register_timer("log_entry_sync_store_put_tx");
}
