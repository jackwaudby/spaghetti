use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SmallBankError {
    InsufficientFunds,
}

impl Error for SmallBankError {}

impl fmt::Display for SmallBankError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SmallBankError::*;
        let err_msg = match *self {
            InsufficientFunds => "insufficient funds",
        };
        write!(f, "{}", err_msg)
    }
}
