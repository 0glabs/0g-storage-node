use std::sync::Arc;

use metrics::{register_timer, Timer};

lazy_static::lazy_static! {
    pub static ref PUT_TX: Arc<dyn Timer> = register_timer("log_store_put_tx");

    pub static ref PUT_CHUNKS: Arc<dyn Timer> = register_timer("log_store_put_chunks");

    pub static ref TX_STORE_PUT: Arc<dyn Timer> = register_timer("log_store_tx_store_put_tx");

    pub static ref CHECK_TX_COMPLETED: Arc<dyn Timer> =
        register_timer("log_store_log_manager_check_tx_completed");

    pub static ref APPEND_SUBTREE_LIST: Arc<dyn Timer> =
        register_timer("log_store_log_manager_append_subtree_list");

    pub static ref COPY_TX_AND_FINALIZE: Arc<dyn Timer> =
        register_timer("log_store_log_manager_copy_tx_and_finalize");

    pub static ref PAD_TX: Arc<dyn Timer> = register_timer("log_store_log_manager_pad_tx");

    pub static ref PUT_BATCH_ROOT_LIST: Arc<dyn Timer> = register_timer("log_store_flow_store_put_batch_root_list");

    pub static ref INSERT_SUBTREE_LIST: Arc<dyn Timer> =
        register_timer("log_store_log_manager_insert_subtree_list");

    pub static ref PUT_MPT_NODE: Arc<dyn Timer> = register_timer("log_store_flow_store_put_mpt_node");

    pub static ref PUT_ENTRY_BATCH_LIST: Arc<dyn Timer> =
        register_timer("log_store_flow_store_put_entry_batch_list");

    pub static ref APPEND_ENTRIES: Arc<dyn Timer> = register_timer("log_store_flow_store_append_entries");

    pub static ref FINALIZE_TX_WITH_HASH: Arc<dyn Timer> = register_timer("log_store_log_manager_finalize_tx_with_hash");
}
