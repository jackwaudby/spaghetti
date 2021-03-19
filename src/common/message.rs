use crate::common::error::NonFatalError;
use crate::common::frame::Frame;
use crate::workloads::tatp::profiles::TatpTransactionProfile;
use crate::workloads::tatp::TatpTransaction;
use crate::workloads::tpcc::profiles::TpccTransactionProfile;
use crate::workloads::tpcc::TpccTransaction;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

///////////////////////////////////////
//// External messages ////
///////////////////////////////////////

/// Represents all messages types that can be sent in `spaghetti`.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Message {
    /// Indicates the client has requested to close its connection with the server.
    CloseConnection,

    /// Indicates ther server has succesfully closed the client's connection.
    ConnectionClosed,

    /// TATP transaction request.
    Request {
        /// Client request no.
        request_no: u32,

        /// Transaction type.
        transaction: Transaction,

        /// Transaction parameters.
        parameters: Parameters,
    },

    /// Response to a transaction request.
    Response {
        /// Client request no.
        request_no: u32,

        /// Transaction outcome.
        outcome: Outcome,
    },
}

/// Transaction types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Transaction {
    Tatp(TatpTransaction),
    Tpcc(TpccTransaction),
}

/// Transaction parameters.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Parameters {
    Tatp(TatpTransactionProfile),
    Tpcc(TpccTransactionProfile),
}

///////////////////////////////////////
//// Internal messages /////
///////////////////////////////////////

/// Sent from read handlers to the transaction manager.
///
/// Contains a chanel to route the response to the correct write handler.
#[derive(Debug)]
pub struct InternalRequest {
    /// Request number from this client.
    pub request_no: u32,

    /// Transaction type
    pub transaction: Transaction,

    /// Trasaction parameters.
    pub parameters: Parameters,

    /// Channel to route response to `WriteHandler`.
    pub response_sender: tokio::sync::mpsc::UnboundedSender<InternalResponse>,
}

/// Sent from the transaction manager to a write handler.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct InternalResponse {
    /// Request number from this client.
    pub request_no: u32,

    /// Transaction outcome.
    pub transaction: Transaction,

    /// Transaction outcome.
    pub outcome: Outcome,

    /// Latency of the request.
    pub latency: Option<Duration>,
}

/// Outcome of a transaction with associated value or abort reason.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Outcome {
    /// Transaction committed.
    Committed { value: Option<String> },

    /// Transaction aborted.
    Aborted { reason: NonFatalError },
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

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Message::*;
        match *self {
            Response {
                request_no,
                ref outcome,
                ..
            } => write!(f, "[id=\"{}\",{}]", request_no, outcome),
            _ => write!(f, "{:?}", self),
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Outcome::*;
        match &*self {
            Committed { value } => write!(f, "val={}", value.as_ref().unwrap()),
            Aborted { reason } => write!(f, "val={{{}}}", reason),
        }
    }
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
