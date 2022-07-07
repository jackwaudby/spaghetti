use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    Serializable,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use IsolationLevel::*;
        match &*self {
            ReadUncommitted => {
                write!(f, "ru")
            }
            ReadCommitted => {
                write!(f, "rc")
            }
            Serializable => {
                write!(f, "s")
            }
        }
    }
}
