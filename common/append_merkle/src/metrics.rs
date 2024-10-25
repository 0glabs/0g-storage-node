use std::sync::Arc;

use metrics::{register_timer, Timer};

lazy_static::lazy_static! {
    pub static ref APPEND: Arc<dyn Timer> = register_timer("append_merkle_append");
    pub static ref APPEND_LIST: Arc<dyn Timer> = register_timer("append_merkle_append_list");
    pub static ref APPEND_SUBTREE: Arc<dyn Timer> = register_timer("append_merkle_append_subtree");
    pub static ref APPEND_SUBTREE_LIST: Arc<dyn Timer> = register_timer("append_merkle_append_subtree_list");
    pub static ref UPDATE_LAST: Arc<dyn Timer> = register_timer("append_merkle_update_last");
}
