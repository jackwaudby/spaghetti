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
    SerializationGraphError(SerializationGraphError),
    WaitHitError(WaitHitError),
    AttendezError(AttendezError),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SerializationGraphError {
    CycleFound,
    CascadingAbort,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum WaitHitError {
    Hit,
    PredecessorAborted,
    PredecessorActive,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum AttendezError {
    ExceededWatermark,
    PredecessorAborted,
    WriteOpExceededWatermark,
}

impl std::error::Error for NonFatalError {}
impl std::error::Error for SerializationGraphError {}
impl std::error::Error for WaitHitError {}
impl std::error::Error for AttendezError {}

impl From<SerializationGraphError> for NonFatalError {
    fn from(error: SerializationGraphError) -> Self {
        NonFatalError::SerializationGraphError(error)
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

impl From<AttendezError> for NonFatalError {
    fn from(error: AttendezError) -> Self {
        NonFatalError::AttendezError(error)
    }
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
            SerializationGraphError(ref e) => write!(f, "{}", e),
            WaitHitError(ref e) => write!(f, "{}", e),
            AttendezError(ref e) => write!(f, "{}", e),
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

impl fmt::Display for AttendezError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use AttendezError::*;
        match *self {
            PredecessorAborted => write!(f, "predecessor aborted"),
            ExceededWatermark => write!(f, "exceeded wait watermark"),
            WriteOpExceededWatermark => write!(f, "exceeded wait watermark"),
        }
    }
}
