use std::sync::Arc;

use metrics::{Counter, CounterUsize, Gauge, GaugeUsize, Histogram, Sample};

lazy_static::lazy_static! {
    // sequential auto sync
    pub static ref SEQUENTIAL_STATE_TXS_SYNCING: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("sync_auto_sequential_state_txs_syncing", 1024);
    pub static ref SEQUENTIAL_STATE_GAP_NEXT_MAX: Arc<dyn Gauge<usize>> = GaugeUsize::register("sync_auto_sequential_state_gap_next_max");
    pub static ref SEQUENTIAL_STATE_TXS_PENDING: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("sync_auto_sequential_state_txs_pending", 1024);
    pub static ref SEQUENTIAL_STATE_GAP_NEXT_DB: Arc<dyn Gauge<usize>> = GaugeUsize::register("sync_auto_sequential_state_gap_next_db");

    pub static ref SEQUENTIAL_SYNC_RESULT_TOTAL: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_sequential_sync_result_total");
    pub static ref SEQUENTIAL_SYNC_RESULT_COMPLETED: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_sequential_sync_result_completed");
    pub static ref SEQUENTIAL_SYNC_RESULT_FAILED: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_sequential_sync_result_failed");
    pub static ref SEQUENTIAL_SYNC_RESULT_TIMEOUT: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_sequential_sync_result_timeout");

    // random auto sync
    pub static ref RANDOM_STATE_TXS_SYNCING: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("sync_auto_random_state_txs_syncing", 1024);
    pub static ref RANDOM_STATE_TXS_READY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("sync_auto_random_state_txs_ready", 1024);
    pub static ref RANDOM_STATE_TXS_PENDING: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("sync_auto_random_state_txs_pending", 1024);

    pub static ref RANDOM_SYNC_RESULT_TOTAL: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_random_sync_result_total");
    pub static ref RANDOM_SYNC_RESULT_COMPLETED: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_random_sync_result_completed");
    pub static ref RANDOM_SYNC_RESULT_FAILED: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_random_sync_result_failed");
    pub static ref RANDOM_SYNC_RESULT_TIMEOUT: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_random_sync_result_timeout");
}
