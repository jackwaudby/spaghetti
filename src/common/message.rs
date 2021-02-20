use crate::common::frame::Frame;
use crate::workloads::tatp::profiles::TatpTransaction;
use crate::workloads::tpcc::profiles::TpccTransaction;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents all messages types that can be sent in `spaghetti`.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Message {
    /// Indicates the client has requested to close its connection with the server.
    CloseConnection,
    /// Indicates ther server has succesfully closed the client's connection.
    ConnectionClosed,
    /// Response to a transaction request.
    Response { request_no: u32, resp: Response },
    /// TATP transaction.
    TatpTransaction {
        request_no: u32,
        params: TatpTransaction,
    },
    /// TPCC transaction.
    TpccTransaction(TpccTransaction),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Message::*;
        match *self {
            Response {
                request_no,
                ref resp,
            } => write!(f, "[id=\"{}\",{}]", request_no, resp),
            _ => write!(f, "{:?}", self),
        }
    }
}

impl Message {
    /// Convert `Message` into a `Frame`.
    pub fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
}

/// Response to a transaction request.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Response {
    Committed { value: Option<String> },
    Aborted { err: String },
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Response::*;
        match &*self {
            Committed { value } => write!(f, "val={}", value.as_ref().unwrap()),
            Aborted { err } => write!(f, "val={}", err),
        }
    }
}

/// Internal to the server, used to send the response to the correct client write handler.
#[derive(Debug)]
pub struct Request {
    pub request_no: u32,
    // Transaction type and parameters.
    pub transaction: Transaction,
    // Channel to route response to `WriteHandler`.
    pub response_sender: tokio::sync::mpsc::UnboundedSender<Message>,
}

/// Transaction types supported.
#[derive(Debug)]
pub enum Transaction {
    Tatp(TatpTransaction),
    Tpcc(TpccTransaction),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_into_frame() {
        let m = Message::CloseConnection;

        let s: Bytes = bincode::serialize(&m).unwrap().into();

        assert_eq!(m.into_frame(), Frame::new(s));
    }
}
