use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MixedSerializationGraphError {
    CycleFound,
    CascadingAbort,
}

impl Error for MixedSerializationGraphError {}

impl fmt::Display for MixedSerializationGraphError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MixedSerializationGraphError::*;
        let err_msg = match *self {
            CycleFound => "cycle found",
            CascadingAbort => "cascading abort",
        };
        write!(f, "{}", err_msg)
    }
}
