use crate::Message;
use std::fmt::{Debug, Display, Formatter};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum Error<N, Req, Res> {
    SendError(mpsc::error::SendError<Message<N, Req, Res>>),
    RecvError(oneshot::error::RecvError),
    TimeoutError,
}

impl<N: Debug, Req: Debug, Res: Debug> Display for Error<N, Req, Res> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChannelError: {:?}", self)
    }
}

impl<N: Debug, Req: Debug, Res: Debug> std::error::Error for Error<N, Req, Res> {}
