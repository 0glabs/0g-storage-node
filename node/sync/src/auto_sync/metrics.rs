use std::sync::Arc;

use metrics::{
    register_meter, register_meter_with_group, Counter, CounterUsize, Gauge, GaugeUsize, Histogram,
    Meter, Sample,
};

use super::batcher::SyncResult;

#[derive(Clone)]
pub struct RandomBatcherMetrics {
    pub ready_txs: Arc<dyn Gauge<usize>>,
    pub pending_txs: Arc<dyn Gauge<usize>>,

    pub completed_qps: Arc<dyn Meter>,
    pub failed_qps: Arc<dyn Meter>,
    pub timeout_qps: Arc<dyn Meter>,
}

impl RandomBatcherMetrics {
    pub fn new(group_name: &str) -> Self {
        Self {
            ready_txs: GaugeUsize::register_with_group(group_name, "ready_txs"),
            pending_txs: GaugeUsize::register_with_group(group_name, "pending_txs"),
            completed_qps: register_meter_with_group(group_name, "completed_qps"),
            failed_qps: register_meter_with_group(group_name, "failed_qps"),
            timeout_qps: register_meter_with_group(group_name, "timeout_qps"),
        }
    }

    pub fn update_state(&self, ready_txs: usize, pending_txs: usize) {
        self.ready_txs.update(ready_txs);
        self.pending_txs.update(pending_txs);
    }

    pub fn update_result(&self, result: SyncResult) {
        match result {
            SyncResult::Completed => self.completed_qps.mark(1),
            SyncResult::Failed => self.failed_qps.mark(1),
            SyncResult::Timeout => self.timeout_qps.mark(1),
        }
    }
}

lazy_static::lazy_static! {
    // sequential auto sync
    pub static ref SEQUENTIAL_STATE_TXS_SYNCING: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("sync_auto_sequential_state_txs_syncing", 1024);
    pub static ref SEQUENTIAL_STATE_GAP_NEXT_MAX: Arc<dyn Gauge<usize>> = GaugeUsize::register("sync_auto_sequential_state_gap_next_max");
    pub static ref SEQUENTIAL_STATE_TXS_PENDING: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("sync_auto_sequential_state_txs_pending", 1024);
    pub static ref SEQUENTIAL_STATE_GAP_NEXT_DB: Arc<dyn Gauge<usize>> = GaugeUsize::register("sync_auto_sequential_state_gap_next_db");

    pub static ref SEQUENTIAL_SYNC_RESULT_COMPLETED: Arc<dyn Meter> = register_meter("sync_auto_sequential_sync_result_completed");
    pub static ref SEQUENTIAL_SYNC_RESULT_FAILED: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_sequential_sync_result_failed");
    pub static ref SEQUENTIAL_SYNC_RESULT_TIMEOUT: Arc<dyn Counter<usize>> = CounterUsize::register("sync_auto_sequential_sync_result_timeout");

    // random auto sync
    pub static ref RANDOM_ANNOUNCED: Arc<RandomBatcherMetrics> = Arc::new(RandomBatcherMetrics::new("sync_auto_random_announced"));
    pub static ref RANDOM_HISTORICAL: Arc<RandomBatcherMetrics> = Arc::new(RandomBatcherMetrics::new("sync_auto_random_historical"));
}
