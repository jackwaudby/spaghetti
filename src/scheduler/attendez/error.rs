use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum AttendezError {
    ExceededWatermark,
    PredecessorAborted,
}

impl Error for AttendezError {}

impl fmt::Display for AttendezError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use AttendezError::*;
        match *self {
            PredecessorAborted => write!(f, "predecessor aborted"),
            ExceededWatermark => write!(f, "exceeded wait watermark"),
        }
    }
}
