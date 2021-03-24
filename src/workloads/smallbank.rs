use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

pub mod helper;

pub mod loader;

pub mod profiles;

pub mod generator;

pub mod procedures;

pub mod keys;

pub mod records;

pub const NUM_ACCOUNTS: u64 = 1000000;
pub const MIN_BALANCE: u32 = 10000;
pub const MAX_BALANCE: u32 = 50000;

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SmallBankTransaction {
    Balance,
    DepositChecking,
    TransactSaving,
    Amalgamate,
    WriteCheck,
    SendPayment,
}
