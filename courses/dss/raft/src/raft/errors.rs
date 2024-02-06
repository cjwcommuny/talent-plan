use std::{error, fmt, result};

use tokio::sync::mpsc::error::SendError;

use crate::raft::sink::SinkError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Encode(labcodec::EncodeError),
    Decode(labcodec::DecodeError),
    PollSendError,
    Rpc(labrpc::Error),
    NotLeader,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::Encode(ref e) => Some(e),
            Self::Decode(ref e) => Some(e),
            Self::PollSendError => None,
            Self::Rpc(ref e) => Some(e),
            Self::NotLeader => None,
        }
    }
}

impl From<SinkError> for Error {
    fn from(_value: SinkError) -> Self {
        Self::PollSendError
    }
}

impl<T> From<SendError<T>> for Error {
    fn from(_value: SendError<T>) -> Self {
        Self::PollSendError
    }
}

pub type Result<T> = result::Result<T, Error>;
