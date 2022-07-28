use crate::workloads::smallbank::error::SmallBankError;

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NonFatalError {
    NoccError,
    SmallBankError(SmallBankError),
    SerializationGraphError(SerializationGraphError),
    WaitHitError(WaitHitError),
    TwoPhaseLockingError(TwoPhaseLockingError),
    RowNotFound,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TwoPhaseLockingError {
    ReadLockDenied,
    WriteLockDenied,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SerializationGraphError {
    CycleFound,
    CascadingAbort,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum WaitHitError {
    RowDirty,
    PredecessorAborted,
    PredecessorActive,
    Hit,
}

impl std::error::Error for NonFatalError {}
impl std::error::Error for SerializationGraphError {}
impl std::error::Error for WaitHitError {}
impl std::error::Error for TwoPhaseLockingError {}

impl From<SerializationGraphError> for NonFatalError {
    fn from(error: SerializationGraphError) -> Self {
        NonFatalError::SerializationGraphError(error)
    }
}

impl From<WaitHitError> for NonFatalError {
    fn from(error: WaitHitError) -> Self {
        NonFatalError::WaitHitError(error)
    }
}

impl From<TwoPhaseLockingError> for NonFatalError {
    fn from(error: TwoPhaseLockingError) -> Self {
        NonFatalError::TwoPhaseLockingError(error)
    }
}

impl From<SmallBankError> for NonFatalError {
    fn from(error: SmallBankError) -> Self {
        NonFatalError::SmallBankError(error)
    }
}

impl fmt::Display for NonFatalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use NonFatalError::*;
        match *self {
            NoccError => write!(f, "nocc error"),
            RowNotFound => write!(f, "row not found error"),
            SmallBankError(ref e) => write!(f, "{}", e),
            SerializationGraphError(ref e) => write!(f, "{}", e),
            WaitHitError(ref e) => write!(f, "{}", e),
            TwoPhaseLockingError(ref e) => write!(f, "{}", e),
        }
    }
}

impl fmt::Display for SerializationGraphError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SerializationGraphError::*;
        let err_msg = match *self {
            CycleFound => "cycle found",
            CascadingAbort => "cascading abort",
        };
        write!(f, "{}", err_msg)
    }
}

impl fmt::Display for WaitHitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use WaitHitError::*;
        let err_msg = match *self {
            RowDirty => "row dirty",
            PredecessorActive => "predecessor active",
            PredecessorAborted => "predecessor abort",
            Hit => "hit",
        };
        write!(f, "{}", err_msg)
    }
}

impl fmt::Display for TwoPhaseLockingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TwoPhaseLockingError::*;
        let err_msg = match *self {
            ReadLockDenied => "read lock denied",
            WriteLockDenied => "write lock denied",
        };
        write!(f, "{}", err_msg)
    }
}
