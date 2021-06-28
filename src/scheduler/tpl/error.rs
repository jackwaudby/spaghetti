use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TwoPhaseLockingError {
    ReadLockRequestDenied(String),
    WriteLockRequestDenied(String),
}

impl fmt::Display for TwoPhaseLockingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TwoPhaseLockingError::*;
        match *self {
            ReadLockRequestDenied(ref key) => write!(f, "read lock for {} denied", key),
            WriteLockRequestDenied(ref key) => write!(f, "write lock for {} denied", key),
        }
    }
}

impl Error for TwoPhaseLockingError {}
