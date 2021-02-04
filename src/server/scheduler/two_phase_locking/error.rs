use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// An `Abort` error is returned when a read or write scheuler operation fails.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TwoPhaseLockingError {
    kind: TwoPhaseLockingErrorKind,
}

impl TwoPhaseLockingError {
    pub fn new(kind: TwoPhaseLockingErrorKind) -> TwoPhaseLockingError {
        TwoPhaseLockingError { kind }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TwoPhaseLockingErrorKind {
    NotRegisteredInActiveTransactions,
    AlreadyRegisteredInActiveTransactions,
    LockRequestDenied,
}

impl fmt::Display for TwoPhaseLockingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TwoPhaseLockingErrorKind::*;
        let err_msg = match self.kind {
            AlreadyRegisteredInActiveTransactions => {
                "Transaction already registered in active transaction table"
            }
            LockRequestDenied => "Lock request denied",
            NotRegisteredInActiveTransactions => {
                "Transaction not registered in active transaction table"
            }
        };
        write!(f, "{}", err_msg)
    }
}

impl Error for TwoPhaseLockingError {}
