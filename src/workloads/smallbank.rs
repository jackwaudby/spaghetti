use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

pub mod loader;

pub mod paramgen;

pub mod procedures;

pub mod keys;

pub mod records;

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SmallBankTransaction {
    Balance,
    DepositChecking,
    TransactSaving,
    Amalgamate,
    WriteCheck,
    SendPayment,
}
