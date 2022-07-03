use crate::workloads::smallbank::error::SmallBankError;

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NonFatalError {
    NoccError,
    SmallBankError(SmallBankError),
    SerializationGraphError(SerializationGraphError),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SerializationGraphError {
    ReadOpCascasde,
    ReadOpCycleFound,
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
            ReadOpCycleFound => "",
            CycleFound => "cycle found",
            CascadingAbort => "cascading abort",
        };
        write!(f, "{}", err_msg)
    }
}
