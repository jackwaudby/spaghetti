use crate::common::error::SpaghettiError;
use crate::common::frame::Frame;
use crate::workloads::tatp::TatpTransaction;
use crate::workloads::tpcc::TpccTransaction;

use bytes::Bytes;
use chrono::offset::Utc;
use chrono::DateTime;
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
    Response(Response),
    /// TATP transaction.
    TatpTransaction(TatpTransaction),
    /// TPCC transaction.
    TpccTransaction(TpccTransaction),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
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
    Aborted { err: SpaghettiError },
}

/// Internal to the server, used to send the response to the correct client write handler.
#[derive(Debug)]
pub struct Request {
    // Transaction type and parameters.
    pub transaction: Transaction,
    // Transaction ID.
    pub id: Option<String>,
    // Timestamp used in deadlock detection.
    pub timestamp: Option<DateTime<Utc>>,
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
