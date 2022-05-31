use crate::scheduler::attendez::error::AttendezError;
use crate::scheduler::error::{MixedSerializationGraphError, SerializationGraphError};
use crate::scheduler::owh::error::OptimisedWaitHitError;
use crate::scheduler::wh::error::WaitHitError;
use crate::workloads::smallbank::error::SmallBankError;

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NonFatalError {
    RowNotFound(String, String),
    RowDirty(String),
    UnableToConvertFromDataType(String, String),
    NonSerializable,
    SmallBankError(SmallBankError),
    SerializationGraph(SerializationGraphError),
    MixedSerializationGraph(MixedSerializationGraphError),
    WaitHitError(WaitHitError),
    OptimisedWaitHitError(OptimisedWaitHitError),
    AttendezError(AttendezError),
}

impl fmt::Display for NonFatalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use NonFatalError::*;
        match *self {
            RowNotFound(ref key, ref index) => write!(f, "not found: {} in {}", key, index),
            RowDirty(ref tid) => write!(f, "row dirty, modified by: {}", tid),
            UnableToConvertFromDataType(ref spag_type, ref val) => {
                write!(f, "unable to convert: type {} to type {}", spag_type, val)
            }

            NonSerializable => write!(f, "non-serializable behaviour"),
            SmallBankError(ref e) => write!(f, "{}", e),
            SerializationGraph(ref e) => write!(f, "{}", e),
            MixedSerializationGraph(ref e) => write!(f, "{}", e),
            WaitHitError(ref e) => write!(f, "{}", e),
            AttendezError(ref e) => write!(f, "{}", e),
            OptimisedWaitHitError(ref e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for NonFatalError {}

impl From<SerializationGraphError> for NonFatalError {
    fn from(error: SerializationGraphError) -> Self {
        NonFatalError::SerializationGraph(error)
    }
}

impl From<MixedSerializationGraphError> for NonFatalError {
    fn from(error: MixedSerializationGraphError) -> Self {
        NonFatalError::MixedSerializationGraph(error)
    }
}

impl From<SmallBankError> for NonFatalError {
    fn from(error: SmallBankError) -> Self {
        NonFatalError::SmallBankError(error)
    }
}

impl From<WaitHitError> for NonFatalError {
    fn from(error: WaitHitError) -> Self {
        NonFatalError::WaitHitError(error)
    }
}

impl From<OptimisedWaitHitError> for NonFatalError {
    fn from(error: OptimisedWaitHitError) -> Self {
        NonFatalError::OptimisedWaitHitError(error)
    }
}

impl From<AttendezError> for NonFatalError {
    fn from(error: AttendezError) -> Self {
        NonFatalError::AttendezError(error)
    }
}
