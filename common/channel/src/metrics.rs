use std::{fmt::Debug, sync::Arc, time::Instant};

use metrics::{register_meter_with_group, Counter, CounterUsize, Histogram, Meter, Sample};
use tokio::sync::mpsc::{
    error::{SendError, TryRecvError},
    unbounded_channel as new_unbounded_channel, UnboundedReceiver, UnboundedSender,
};

pub fn unbounded_channel<T>(metric_name: &str) -> (Sender<T>, Receiver<T>) {
    let (sender, receiver) = new_unbounded_channel();
    let metrics_queued = CounterUsize::register_with_group(metric_name, "size");
    (
        Sender::new(sender, metric_name, metrics_queued.clone()),
        Receiver::new(receiver, metric_name, metrics_queued),
    )
}

pub struct Sender<T> {
    sender: UnboundedSender<(Instant, T)>,
    metrics_send_qps: Arc<dyn Meter>,
    metrics_queued: Arc<dyn Counter<usize>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            metrics_send_qps: self.metrics_send_qps.clone(),
            metrics_queued: self.metrics_queued.clone(),
        }
    }
}

impl<T> Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.sender)
    }
}

impl<T> Sender<T> {
    pub(crate) fn new(
        sender: UnboundedSender<(Instant, T)>,
        metrics_group: &str,
        metrics_queued: Arc<dyn Counter<usize>>,
    ) -> Self {
        Self {
            sender,
            metrics_send_qps: register_meter_with_group(metrics_group, "send"),
            metrics_queued,
        }
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.sender.send((Instant::now(), value)) {
            Ok(()) => {
                self.metrics_send_qps.mark(1);
                self.metrics_queued.inc(1);
                Ok(())
            }
            Err(e) => Err(SendError(e.0 .1)),
        }
    }
}

pub struct Receiver<T> {
    receiver: UnboundedReceiver<(Instant, T)>,
    metrics_recv_qps: Arc<dyn Meter>,
    metrics_queued: Arc<dyn Counter<usize>>,
    metrics_queue_latency: Arc<dyn Histogram>,
}

impl<T> Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.receiver)
    }
}

impl<T> Receiver<T> {
    pub(crate) fn new(
        receiver: UnboundedReceiver<(Instant, T)>,
        metrics_group: &str,
        metrics_queued: Arc<dyn Counter<usize>>,
    ) -> Self {
        Self {
            receiver,
            metrics_recv_qps: register_meter_with_group(metrics_group, "recv"),
            metrics_queued,
            metrics_queue_latency: Sample::ExpDecay(0.015).register_with_group(
                metrics_group,
                "latency",
                1024,
            ),
        }
    }

    fn on_recv(&self, value: (Instant, T)) -> T {
        self.metrics_recv_qps.mark(1);
        self.metrics_queued.dec(1);
        self.metrics_queue_latency.update_since(value.0);
        value.1
    }

    pub async fn recv(&mut self) -> Option<T> {
        let value = self.receiver.recv().await?;
        Some(self.on_recv(value))
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let value = self.receiver.try_recv()?;
        Ok(self.on_recv(value))
    }
}
