use crate::storage::datatype::Data;
use crate::storage::table::Table;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use strum_macros::EnumIter;

pub mod loader;

pub mod paramgen;

pub mod procedures;

pub mod error;

lazy_static! {
    pub static ref SB_SF_MAP: HashMap<u64, u64> = {
        let mut m = HashMap::new();
        m.insert(0, 10);
        m.insert(1, 100);
        m.insert(2, 1000);
        m.insert(3, 10000);
        m.insert(4, 100000);
        m.insert(5, 1000000);
        m
    };
}

pub static MIN_BALANCE: i64 = 10000;
pub static MAX_BALANCE: i64 = 50000;
pub static SEND_PAYMENT_AMOUNT: f64 = 5.0;
pub static DEPOSIT_CHECKING_AMOUNT: f64 = 1.3;
pub static TRANSACT_SAVINGS_AMOUNT: f64 = 20.20;
pub static WRITE_CHECK_AMOUNT: f64 = 5.0;
pub static HOTSPOT_PERCENTAGE: f64 = 0.25;
pub static HOTSPOT_FIXED_SIZE: u64 = 2;

#[derive(Debug)]
pub struct SmallBankDatabase([Table; 3]);

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SmallBankTransaction {
    Balance,
    DepositChecking,
    TransactSaving,
    Amalgamate,
    WriteCheck,
    SendPayment,
}

impl SmallBankDatabase {
    pub fn new(population: usize) -> Self {
        let array: [Table; 3] = [
            Table::new(population, 1),
            Table::new(population, 2),
            Table::new(population, 2),
        ];

        SmallBankDatabase(array)
    }

    pub fn insert_value(&mut self, table_id: usize, column_id: usize, offset: usize, value: Data) {
        self.get_mut_table(table_id)
            .get_tuple(column_id, offset)
            .get()
            .init_value(value)
            .unwrap();
    }

    pub fn get_table(&self, id: usize) -> &Table {
        &self.0[id]
    }

    pub fn get_mut_table(&mut self, id: usize) -> &mut Table {
        &mut self.0[id]
    }
}

impl fmt::Display for SmallBankDatabase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, table) in self.0.iter().enumerate() {
            write!(f, "table {}:\n", i).unwrap();
            write!(f, "{}", table).unwrap();
        }

        Ok(())
    }
}
