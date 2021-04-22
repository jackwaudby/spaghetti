use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::EnumIter;

pub mod loader;

pub mod paramgen;

pub mod procedures;

pub mod keys;

pub mod error;

lazy_static! {
    pub static ref SB_SF_MAP: HashMap<u64, u64> = {
        let mut m = HashMap::new();
        m.insert(0, 10);
        m.insert(1, 18000);
        m.insert(10, 180000);
        m.insert(100, 1800000);
        m
    };
}

pub static MIN_BALANCE: i64 = 10000000;
pub static MAX_BALANCE: i64 = 50000000;
pub static SEND_PAYMENT_AMOUNT: f64 = 5.0;
pub static DEPOSIT_CHECKING_AMOUNT: f64 = 1.3;
pub static TRANSACT_SAVINGS_AMOUNT: f64 = 20.20;
pub static WRITE_CHECK_AMOUNT: f64 = 5.0;
pub static HOTSPOT_PERCENTAGE: f64 = 0.25;
pub static HOTSPOT_FIXED_SIZE: u64 = 2;

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SmallBankTransaction {
    Balance,
    DepositChecking,
    TransactSaving,
    Amalgamate,
    WriteCheck,
    SendPayment,
}
