use serde::{Deserialize, Serialize};

pub mod tatp;

pub mod smallbank;

pub mod acid;

pub mod dummy;

// TODO: move elsewhere
#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    Serializable,
}
