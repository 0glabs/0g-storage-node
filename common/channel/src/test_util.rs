use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    time::Duration,
};

use tokio::time::timeout;

use crate::{Message, Receiver};

pub struct TestReceiver<N, Req, Res> {
    recv: Receiver<N, Req, Res>,
}

impl<N, Req, Res> From<Receiver<N, Req, Res>> for TestReceiver<N, Req, Res> {
    fn from(recv: Receiver<N, Req, Res>) -> Self {
        Self { recv }
    }
}

impl<N, Req, Res> Deref for TestReceiver<N, Req, Res> {
    type Target = Receiver<N, Req, Res>;

    fn deref(&self) -> &Self::Target {
        &self.recv
    }
}

impl<N, Req, Res> DerefMut for TestReceiver<N, Req, Res> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.recv
    }
}

impl<N, Req, Res: Debug> TestReceiver<N, Req, Res> {
    pub async fn expect_response(&mut self, response: Res) {
        let request = timeout(Duration::from_secs(3), self.recv())
            .await
            .expect("Timeout to receive request")
            .expect("Channel closed");

        match request {
            Message::Notification(..) => panic!("Unexpected message type"),
            Message::Request(_, resp_sender) => {
                resp_sender.send(response).expect("Channel closed");
            }
        }
    }

    pub async fn expect_responses(&mut self, responses: Vec<Res>) {
        for resp in responses {
            self.expect_response(resp).await;
        }
    }
}
