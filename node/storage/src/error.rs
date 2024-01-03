use anyhow;
use ssz::DecodeError;
use std::error::Error as ErrorTrait;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error as IoError;

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub enum Error {
    Io(IoError),
    /// A partial chunk batch is written.
    InvalidBatchBoundary,
    ValueDecodingError(DecodeError),
    Custom(String),
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Error::Io(e)
    }
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Self {
        Error::ValueDecodingError(e)
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Custom(e.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageError: {:?}", self)
    }
}

impl ErrorTrait for Error {}
