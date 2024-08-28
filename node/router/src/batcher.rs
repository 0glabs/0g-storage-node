use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use shared_types::TxID;

use crate::metrics;

pub(crate) struct FileBatcher {
    txs: VecDeque<(TxID, Instant)>,
    capacity: usize,
    timeout: Duration,
}

impl FileBatcher {
    pub fn new(capacity: usize, timeout: Duration) -> Self {
        Self {
            txs: VecDeque::with_capacity(capacity),
            capacity,
            timeout,
        }
    }

    pub fn add(&mut self, tx_id: TxID) -> Option<Vec<TxID>> {
        self.txs.push_front((tx_id, Instant::now()));

        let size = self.txs.len();
        if size < self.capacity {
            return self.expire();
        }

        metrics::BATCHER_ANNOUNCE_FILE_SIZE.update(size as u64);

        let batch = self
            .txs
            .split_off(0)
            .iter()
            .map(|(tx_id, _)| tx_id.clone())
            .collect();

        Some(batch)
    }

    pub fn expire(&mut self) -> Option<Vec<TxID>> {
        let pos = self
            .txs
            .iter()
            .position(|(_, ts)| ts.elapsed() > self.timeout)?;

        metrics::BATCHER_ANNOUNCE_FILE_SIZE.update((self.txs.len() - pos) as u64);

        let batch = self
            .txs
            .split_off(pos)
            .iter()
            .map(|(tx_id, _)| tx_id.clone())
            .collect();

        Some(batch)
    }
}
