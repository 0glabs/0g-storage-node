use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use ::metrics::{Histogram, Sample};

/// `Batcher` is used to handle data in batch, when `capacity` or `timeout` matches.
pub(crate) struct Batcher<T> {
    items: VecDeque<T>,
    earliest_time: Option<Instant>,
    capacity: usize,
    timeout: Duration,
    metrics_batch_size: Arc<dyn Histogram>,
}

impl<T> Batcher<T> {
    pub fn new(capacity: usize, timeout: Duration, name: &str) -> Self {
        Self {
            items: VecDeque::with_capacity(capacity),
            earliest_time: None,
            capacity,
            timeout,
            metrics_batch_size: Sample::ExpDecay(0.015).register_with_group(
                "router_batcher_size",
                name,
                1024,
            ),
        }
    }

    fn remove_all(&mut self) -> Option<Vec<T>> {
        let size = self.items.len();
        if size == 0 {
            return None;
        }

        self.metrics_batch_size.update(size as u64);
        self.earliest_time = None;

        Some(Vec::from_iter(self.items.split_off(0).into_iter().rev()))
    }

    pub fn add(&mut self, value: T) -> Option<Vec<T>> {
        self.add_with_time(value, Instant::now())
    }

    fn add_with_time(&mut self, value: T, now: Instant) -> Option<Vec<T>> {
        // push at front so as to use `split_off` to remove expired items
        self.items.push_front(value);
        if self.earliest_time.is_none() {
            self.earliest_time = Some(now);
        }

        // cache if not full
        let size = self.items.len();
        if size < self.capacity {
            return None;
        }

        // cache is full
        self.remove_all()
    }

    pub fn expire(&mut self) -> Option<Vec<T>> {
        self.expire_with_time(Instant::now())
    }

    fn expire_with_time(&mut self, now: Instant) -> Option<Vec<T>> {
        if now.duration_since(self.earliest_time?) < self.timeout {
            None
        } else {
            self.remove_all()
        }
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

        // expire all
        assert_eq!(
            batcher.expire_with_time(now + Duration::from_secs(13)),
            Some(vec![1, 2, 3, 4])
        );
        assert_eq!(batcher.items.len(), 0);
    }
}
