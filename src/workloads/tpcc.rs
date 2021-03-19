use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

pub mod helper;

pub mod loader;

pub mod profiles;

pub mod generator;

pub mod procedures;

pub mod keys;

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TpccTransaction {
    Payment,
    NewOrder,
}
