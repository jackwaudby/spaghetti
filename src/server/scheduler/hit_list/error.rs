use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// Hit list error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum HitListError {
    /// id already in use.
    IdAlreadyInUse(u64),

    /// Transaction exists in the hit list.
    TransactionInHitList(u64),

    /// Locking mutex failed.
    MutexLockFailed,

    /// Transaction aborted in wait-phase due to predecessor already having aborted.
    PredecessorAborted(u64),

    /// Transaction aborted in wait-phase due to predecessor been active.
    PredecessorActive(u64),
}

impl Error for HitListError {}

impl fmt::Display for HitListError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use HitListError::*;
        match *self {
            MutexLockFailed => write!(f, "locking mutex failed"),
            IdAlreadyInUse(ref tid) => write!(f, "transaction {} already in use", tid),
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
