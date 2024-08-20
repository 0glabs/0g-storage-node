use std::sync::Arc;

use metrics::{register_timer, Timer};

lazy_static::lazy_static! {
    pub static ref SERIAL_SYNC_FILE_COMPLETED: Arc<dyn Timer> = register_timer("sync_controllers_serial_sync_file_completed");
    pub static ref SERIAL_SYNC_CHUNKS_COMPLETED: Arc<dyn Timer> = register_timer("sync_controllers_serial_sync_chunks_completed");
}
