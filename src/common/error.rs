use crate::workloads::smallbank::error::SmallBankError;

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NonFatalError {
    NoccError,
    SmallBankError(SmallBankError),
    SerializationGraphError(SerializationGraphError),
    RowNotFound,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SerializationGraphError {
    ReadOpCascasde,
    WriteOpCascasde,
    ReadOpCycleFound,
    WriteOpCycleFound,
    CycleFound,
    CascadingAbort,
}

impl std::error::Error for NonFatalError {}
impl std::error::Error for SerializationGraphError {}

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

impl fmt::Display for NonFatalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use NonFatalError::*;
        match *self {
            NoccError => write!(f, "nocc error"),
            RowNotFound => write!(f, "row not found error"),
            SmallBankError(ref e) => write!(f, "{}", e),
            SerializationGraphError(ref e) => write!(f, "{}", e),
        }
    }
}

impl fmt::Display for SerializationGraphError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SerializationGraphError::*;
        let err_msg = match *self {
            ReadOpCascasde => "",
            WriteOpCascasde => "",
            ReadOpCycleFound => "",
            WriteOpCycleFound => "",
            CycleFound => "cycle found",
            CascadingAbort => "cascading abort",
        };
        write!(f, "{}", err_msg)
    }
}
