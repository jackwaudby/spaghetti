use std::error::Error;
use std::fmt;

/// SGT error types.
#[derive(PartialEq, Debug, Clone)]
pub enum HitListError {
    /// ID already in use
    IdAlreadyInUse(u64),
    /// ID in hit list
    IdInHitList(u64),
    /// Locking mutex failed
    MutexLockFailed,
}

impl Error for HitListError {}

impl fmt::Display for HitListError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use HitListError::*;
        match *self {
            MutexLockFailed => write!(f, "locking mutex failed"),
            IdAlreadyInUse(ref tid) => write!(f, "Transaction ID: {} already in use", tid),
            IdInHitList(ref tid) => write!(f, "Transaction ID: {} in hit list", tid),
        }
    }
}
