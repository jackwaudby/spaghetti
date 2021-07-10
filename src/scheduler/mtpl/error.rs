use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MixedTwoPhaseLockingError {
    ReadLockRequestDenied(String),
    WriteLockRequestDenied(String),
}

impl fmt::Display for MixedTwoPhaseLockingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MixedTwoPhaseLockingError::*;
        match *self {
            ReadLockRequestDenied(ref key) => write!(f, "read lock for {} denied", key),
            WriteLockRequestDenied(ref key) => write!(f, "write lock for {} denied", key),
        }
    }
}

impl Error for MixedTwoPhaseLockingError {}
