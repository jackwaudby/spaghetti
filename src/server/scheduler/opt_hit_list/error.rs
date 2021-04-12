use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// Hit list error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum OptimisedHitListError {
    /// Transaction was hit whilst committing.
    Hit(String),

    /// Transaction aborted in wait-phase due to predecessor already having aborted.
    PredecessorAborted(String),

    /// Transaction aborted in wait-phase due to predecessor been active.
    PredecessorActive(String),
}

impl Error for OptimisedHitListError {}

impl fmt::Display for OptimisedHitListError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use OptimisedHitListError::*;
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
