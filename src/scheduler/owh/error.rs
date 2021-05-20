use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// Hit list error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum OptimisedWaitHitError {
    /// Transaction was hit whilst committing.
    Hit(String),

    /// Transaction aborted in wait-phase due to predecessor already having aborted.
    PredecessorAborted(String),

    /// Transaction aborted in wait-phase due to predecessor been active.
    PredecessorActive(String),
}

impl Error for OptimisedWaitHitError {}

impl fmt::Display for OptimisedWaitHitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use OptimisedWaitHitError::*;
        match *self {
            Hit(ref tid) => write!(f, "transaction {} was hit", tid),
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
