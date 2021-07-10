use crate::common::error::NonFatalError;
use crate::workloads::acid::paramgen::AcidTransactionProfile;
use crate::workloads::acid::AcidTransaction;
use crate::workloads::smallbank::paramgen::SmallBankTransactionProfile;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::IsolationLevel;

use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents all messages types that can be sent in `spaghetti`.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Message {
    /// Indicates the client has requested to close its connection with the server.
    CloseConnection,

    /// Indicates ther server has succesfully closed the client's connection.
    ConnectionClosed,

    /// Transaction request.
    Request {
        request_no: u32,
        transaction: Transaction,
        parameters: Parameters,
        isolation: IsolationLevel,
    },

    /// Response to a transaction request.
    Response { request_no: u32, outcome: Outcome },
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Transaction {
    Acid(AcidTransaction),
    Tatp,
    SmallBank(SmallBankTransaction),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Parameters {
    Acid(AcidTransactionProfile),
    Tatp,
    SmallBank(SmallBankTransactionProfile),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct InternalResponse {
    pub request_no: u32,
    pub transaction: Transaction,
    pub outcome: Outcome,
}

/// Outcome of a transaction with associated value or abort reason.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Outcome {
    Committed { value: Option<String> },
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
