use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use ::metrics::{Histogram, Sample};

/// `Batcher` is used to handle data in batch, when `capacity` or `timeout` matches.
pub(crate) struct Batcher<T> {
    items: VecDeque<(T, Instant)>,
    capacity: usize,
    timeout: Duration,
    metrics_batch_size: Arc<dyn Histogram>,
}

impl<T> Batcher<T> {
    pub fn new(capacity: usize, timeout: Duration, name: &str) -> Self {
        Self {
            items: VecDeque::with_capacity(capacity),
            capacity,
            timeout,
            metrics_batch_size: Sample::ExpDecay(0.015).register_with_group(
                "router_batcher_size",
                name,
                1024,
            ),
        }
    }

    pub fn add(&mut self, value: T) -> Option<Vec<T>> {
        // push at front so as to use `split_off` to remove expired items
        self.items.push_front((value, Instant::now()));

        // remove expired items if not full
        let size = self.items.len();
        if size < self.capacity {
            return self.expire();
        }

        // remove all items for batch operation in advance
        self.metrics_batch_size.update(size as u64);

        Some(Vec::from_iter(
            self.items.split_off(0).into_iter().map(|(val, _)| val),
        ))
    }

    pub fn expire(&mut self) -> Option<Vec<T>> {
        // find the index of first expired item if any
        let pos = self
            .items
            .iter()
            .position(|(_, ts)| ts.elapsed() > self.timeout)?;

        // remove expired items for batch operation in advance
        let size = self.items.len() - pos;
        self.metrics_batch_size.update(size as u64);

        Some(Vec::from_iter(
            self.items.split_off(pos).into_iter().map(|(val, _)| val),
        ))
    }
}
