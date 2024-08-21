use crate::error::Error;
use metrics::{register_meter_with_group, Counter, CounterUsize, Histogram, Meter, Sample};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::{SendError, TryRecvError};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

pub type ResponseSender<Res> = oneshot::Sender<Res>;

#[derive(Debug)]
pub enum Message<N, Req, Res> {
    Notification(N),
    Request(Req, ResponseSender<Res>),
}

pub struct Channel<N, Req, Res> {
    _phantom: std::marker::PhantomData<(N, Req, Res)>,
}

impl<N, Req, Res> Channel<N, Req, Res> {
    pub fn unbounded(name: &str) -> (Sender<N, Req, Res>, Receiver<N, Req, Res>) {
        let (sender, receiver) = mpsc::unbounded_channel();

        let metrics_group = format!("common_channel_{}", name);
        let metrics_queued = CounterUsize::register_with_group(metrics_group.as_str(), "size");

        (
            Sender {
                chan: sender,
                metrics_send_qps: register_meter_with_group(metrics_group.as_str(), "send"),
                metrics_queued: metrics_queued.clone(),
                metrics_timeout: CounterUsize::register_with_group(
                    metrics_group.as_str(),
                    "timeout",
                ),
            },
            Receiver {
                chan: receiver,
                metrics_recv_qps: register_meter_with_group(metrics_group.as_str(), "recv"),
                metrics_queued,
                metrics_queue_latency: Sample::ExpDecay(0.015).register_with_group(
                    metrics_group.as_str(),
                    "latency",
                    1024,
                ),
            },
        )
    }
}

enum TimedMessage<N, Req, Res> {
    Notification(Instant, N),
    Request(Instant, Req, ResponseSender<Res>),
}

impl<N, Req, Res> From<Message<N, Req, Res>> for TimedMessage<N, Req, Res> {
    fn from(value: Message<N, Req, Res>) -> Self {
        match value {
            Message::Notification(n) => TimedMessage::Notification(Instant::now(), n),
            Message::Request(req, res) => TimedMessage::Request(Instant::now(), req, res),
        }
    }
}

impl<N, Req, Res> TimedMessage<N, Req, Res> {
    fn into_message(self) -> (Instant, Message<N, Req, Res>) {
        match self {
            TimedMessage::Notification(since, n) => (since, Message::Notification(n)),
            TimedMessage::Request(since, req, res) => (since, Message::Request(req, res)),
        }
    }
}

pub struct Sender<N, Req, Res> {
    chan: mpsc::UnboundedSender<TimedMessage<N, Req, Res>>,

    metrics_send_qps: Arc<dyn Meter>,
    metrics_queued: Arc<dyn Counter<usize>>,
    metrics_timeout: Arc<dyn Counter<usize>>,
}

impl<N, Req, Res> Clone for Sender<N, Req, Res> {
    fn clone(&self) -> Self {
        Sender {
            chan: self.chan.clone(),
            metrics_send_qps: self.metrics_send_qps.clone(),
            metrics_queued: self.metrics_queued.clone(),
            metrics_timeout: self.metrics_timeout.clone(),
        }
    }
}

impl<N, Req, Res> Sender<N, Req, Res> {
    pub fn notify(&self, msg: N) -> Result<(), Error<N, Req, Res>> {
        self.send(Message::Notification(msg))
    }

    pub async fn request(&self, request: Req) -> Result<Res, Error<N, Req, Res>> {
        let (sender, receiver) = oneshot::channel();

        self.send(Message::Request(request, sender))?;

        timeout(DEFAULT_REQUEST_TIMEOUT, receiver)
            .await
            .map_err(|_| {
                self.metrics_timeout.inc(1);
                Error::TimeoutError
            })?
            .map_err(|e| Error::RecvError(e))
    }

    fn send(&self, message: Message<N, Req, Res>) -> Result<(), Error<N, Req, Res>> {
        match self.chan.send(message.into()) {
            Ok(()) => {
                self.metrics_send_qps.mark(1);
                self.metrics_queued.inc(1);
                Ok(())
            }
            Err(e) => {
                let (_, msg) = e.0.into_message();
                Err(Error::SendError(SendError(msg)))
            }
        }
    }
}

pub struct Receiver<N, Req, Res> {
    chan: mpsc::UnboundedReceiver<TimedMessage<N, Req, Res>>,

    metrics_recv_qps: Arc<dyn Meter>,
    metrics_queued: Arc<dyn Counter<usize>>,
    metrics_queue_latency: Arc<dyn Histogram>,
}

impl<N, Req, Res> Receiver<N, Req, Res> {
    pub async fn recv(&mut self) -> Option<Message<N, Req, Res>> {
        let data = self.chan.recv().await?;
        Some(self.on_recv_data(data))
    }

    pub fn try_recv(&mut self) -> Result<Message<N, Req, Res>, TryRecvError> {
        let data = self.chan.try_recv()?;
        Ok(self.on_recv_data(data))
    }

    fn on_recv_data(&self, data: TimedMessage<N, Req, Res>) -> Message<N, Req, Res> {
        self.metrics_recv_qps.mark(1);
        self.metrics_queued.dec(1);

        let (since, msg) = data.into_message();
        self.metrics_queue_latency.update_since(since);

        msg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    enum Notification {}

    #[derive(Debug)]
    enum Request {
        GetNumber,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Response {
        GetNumber(u32),
    }

    #[tokio::test]
    async fn request_response() {
        let (tx, mut rx) = Channel::<Notification, Request, Response>::unbounded("test");

        let task1 = async move {
            match rx.recv().await.expect("not dropped") {
                Message::Notification(_) => {}
                Message::Request(Request::GetNumber, sender) => {
                    sender.send(Response::GetNumber(42)).expect("not dropped");
                }
            }
        };

        let task2 = async move {
            let result = tx.request(Request::GetNumber).await.expect("not dropped");
            assert_eq!(result, Response::GetNumber(42));
        };

        tokio::join!(task1, task2);
    }
}
