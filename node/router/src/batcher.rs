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
        self.add_with_time(value, Instant::now())
    }

    fn add_with_time(&mut self, value: T, now: Instant) -> Option<Vec<T>> {
        // push at front so as to use `split_off` to remove expired items
        self.items.push_front((value, now));

        // remove expired items if not full
        let size = self.items.len();
        if size < self.capacity {
            return self.expire_with_time(now);
        }

        // remove all items for batch operation in advance
        self.metrics_batch_size.update(size as u64);

        Some(Vec::from_iter(
            self.items
                .split_off(0)
                .into_iter()
                .rev()
                .map(|(val, _)| val),
        ))
    }

    pub fn expire(&mut self) -> Option<Vec<T>> {
        self.expire_with_time(Instant::now())
    }

    fn expire_with_time(&mut self, now: Instant) -> Option<Vec<T>> {
        let total = self.items.len();

        // find the index of first expired item if any
        let first_unexpired = self
            .items
            .iter()
            .rev()
            .position(|(_, ts)| now.duration_since(*ts) < self.timeout)
            .unwrap_or(total);

        if first_unexpired == 0 {
            return None;
        }

        let pos = total - first_unexpired;

        // remove expired items for batch operation in advance
        self.metrics_batch_size.update((total - pos) as u64);

        Some(Vec::from_iter(
            self.items
                .split_off(pos)
                .into_iter()
                .rev()
                .map(|(val, _)| val),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::Batcher;

    #[test]
    fn test_add() {
        let mut batcher: Batcher<usize> = Batcher::new(3, Duration::from_secs(10), "test");

        assert_eq!(batcher.add(1), None);
        assert_eq!(batcher.add(2), None);
        assert_eq!(batcher.add(3), Some(vec![1, 2, 3]));
        assert_eq!(batcher.items.len(), 0);
    }

    #[test]
    fn test_expire() {
        let mut batcher: Batcher<usize> = Batcher::new(5, Duration::from_secs(10), "test");

        let now = Instant::now();

        // enqueue: 1, 2, 3, 4
        assert_eq!(batcher.add_with_time(1, now + Duration::from_secs(1)), None);
        assert_eq!(batcher.add_with_time(2, now + Duration::from_secs(2)), None);
        assert_eq!(batcher.add_with_time(3, now + Duration::from_secs(4)), None);
        assert_eq!(batcher.add_with_time(4, now + Duration::from_secs(5)), None);

        // expire None
        assert_eq!(batcher.expire_with_time(now + Duration::from_secs(6)), None);

        // expire 1, 2
        assert_eq!(
            batcher.expire_with_time(now + Duration::from_secs(13)),
            Some(vec![1, 2])
        );
        assert_eq!(batcher.items.len(), 2);

        // expire 3, 4
        assert_eq!(
            batcher.expire_with_time(now + Duration::from_secs(20)),
            Some(vec![3, 4])
        );
        assert_eq!(batcher.items.len(), 0);
    }
}
