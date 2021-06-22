use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum WaitHitError {
    TransactionInHitList(u64),
    PredecessorAborted(u64),
    PredecessorActive(u64),
}

impl Error for WaitHitError {}

impl fmt::Display for WaitHitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use WaitHitError::*;
        match *self {
            TransactionInHitList(ref tid) => write!(f, "transaction {} in hit list", tid),
            PredecessorAborted(ref tid) => {
                write!(f, "transaction {} aborted due to predecessor aborting", tid)
            }
            PredecessorActive(ref tid) => write!(
                f,
                "transaction {} aborted due to predecessor being active",
                tid
            ),
        }
    }
}
