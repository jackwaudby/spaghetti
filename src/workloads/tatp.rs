use crate::storage::datatype::Data;
use crate::storage::table::Table;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::EnumIter;

pub mod helper;

pub mod loader;

pub mod paramgen;

pub mod procedures;

pub mod keys;

lazy_static! {
    pub static ref TATP_SF_MAP: HashMap<u64, u64> = {
        let mut m = HashMap::new();
        m.insert(0, 10);
        m.insert(1, 100000);
        m.insert(2, 200000);
        m.insert(3, 500000);
        m.insert(10, 1000000);
        m.insert(20, 2000000);
        m.insert(50, 5000000);
        m
    };
}

#[derive(Debug)]
pub struct TatpDatabase([Table; 4]);

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TatpTransaction {
    GetSubscriberData,
    GetNewDestination,
    GetAccessData,
    UpdateSubscriberData,
    UpdateLocationData,
    InsertCallForwarding,
}

impl TatpDatabase {
    pub fn new(population: usize) -> Self {
        // Note; subscriber has 34 columns which are largely redunant
        // Included: s_id; sub_nbr; bit_1; msc_location; vlr_location
        let array: [Table; 4] = [
            Table::new(population, 5),     // subscribers
            Table::new(population * 5, 6), // access info
            Table::new(population * 5, 6), // special facility
            Table::new(population * 5, 5), // call forwarding
        ];

        TatpDatabase(array)
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
