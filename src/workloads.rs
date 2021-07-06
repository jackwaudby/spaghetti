use serde::{Deserialize, Serialize};

pub mod smallbank;

pub mod acid;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    Serializable,
}
