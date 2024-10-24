use std::sync::Arc;

use metrics::{register_timer, Timer};

lazy_static::lazy_static! {
    pub static ref TX_STORE_PUT: Arc<dyn Timer> = register_timer("log_store_tx_store_put_tx");


    pub static ref APPEND_SUBTREE_LIST: Arc<dyn Timer> =
        register_timer("log_store_log_manager_append_subtree_list");

    pub static ref COPY_TX_AND_FINALIZE: Arc<dyn Timer> =
        register_timer("log_store_log_manager_copy_tx_and_finalize");
}
