use crate::common::error::NonFatalError;
use crate::workloads::acid::paramgen::AcidTransactionProfile;
use crate::workloads::acid::AcidTransaction;
use crate::workloads::smallbank::paramgen::SmallBankTransactionProfile;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::tatp::paramgen::TatpTransactionProfile;
use crate::workloads::tatp::TatpTransaction;

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
    Acid(AcidTransaction),
    Tatp(TatpTransaction),
    SmallBank(SmallBankTransaction),
}

/// Transaction parameters.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Parameters {
    Acid(AcidTransactionProfile),
    Tatp(TatpTransactionProfile),
    SmallBank(SmallBankTransactionProfile),
}

///////////////////////////////////////
//// Internal messages /////
///////////////////////////////////////

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
            Committed { value } => write!(f, "{}", value.as_ref().unwrap()),
            Aborted { reason } => write!(f, "val={{{}}}", reason),
        }
    }
}
