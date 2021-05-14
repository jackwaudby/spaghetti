use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// SGT error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SerializationGraphError {
    /// Cycle found.
    CycleFound,

    /// Cascading abort
    CascadingAbort,
}

impl Error for SerializationGraphError {}

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
