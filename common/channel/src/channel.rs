use crate::error::Error;
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
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
    pub fn unbounded() -> (Sender<N, Req, Res>, Receiver<N, Req, Res>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Sender { chan: sender }, Receiver { chan: receiver })
    }
}

pub struct Sender<N, Req, Res> {
    chan: mpsc::UnboundedSender<Message<N, Req, Res>>,
}

impl<N, Req, Res> Clone for Sender<N, Req, Res> {
    fn clone(&self) -> Self {
        Sender {
            chan: self.chan.clone(),
        }
    }
}

impl<N, Req, Res> Sender<N, Req, Res> {
    pub fn notify(&self, msg: N) -> Result<(), Error<N, Req, Res>> {
        self.chan
            .send(Message::Notification(msg))
            .map_err(|e| Error::SendError(e))
    }

    pub async fn request(&self, request: Req) -> Result<Res, Error<N, Req, Res>> {
        let (sender, receiver) = oneshot::channel();

        self.chan
            .send(Message::Request(request, sender))
            .map_err(|e| Error::SendError(e))?;

        timeout(DEFAULT_REQUEST_TIMEOUT, receiver)
            .await
            .map_err(|_| Error::TimeoutError)?
            .map_err(|e| Error::RecvError(e))
    }
}

pub struct Receiver<N, Req, Res> {
    chan: mpsc::UnboundedReceiver<Message<N, Req, Res>>,
}

impl<N, Req, Res> Receiver<N, Req, Res> {
    pub async fn recv(&mut self) -> Option<Message<N, Req, Res>> {
        self.chan.recv().await
    }

    pub fn try_recv(&mut self) -> Result<Message<N, Req, Res>, TryRecvError> {
        self.chan.try_recv()
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
        let (tx, mut rx) = Channel::<Notification, Request, Response>::unbounded();

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
