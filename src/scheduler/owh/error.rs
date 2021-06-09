use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum OptimisedWaitHitError {
    /// Transaction was hit whilst committing.
    Hit,

    /// Transaction aborted in wait-phase due to predecessor already having aborted.
    PredecessorAborted,

    /// Transaction aborted in wait-phase due to predecessor been active.
    PredecessorActive,
}

impl Error for OptimisedWaitHitError {}

impl fmt::Display for OptimisedWaitHitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use OptimisedWaitHitError::*;
        match *self {
            Hit => write!(f, "transaction was hit"),
            PredecessorAborted => {
                write!(f, "aborted due to predecessor aborting")
            }
            PredecessorActive => {
                write!(f, "aborted due to predecessor being active",)
            }
        }
    }
}
