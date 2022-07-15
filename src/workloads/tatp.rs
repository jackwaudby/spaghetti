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
        m.insert(1, 100);
        m.insert(2, 100000);
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
}

impl TatpDatabase {
    pub fn new(population: usize) -> Self {
        // Note; subscriber has 34 columns which are largely redunant
        // Included: s_id; sub_nbr; bit_1; msc_location; vlr_location
        let array: [Table; 4] = [
            Table::new(population, 5),     // subscribers: 0
            Table::new(population * 5, 6), // access info: 1
            Table::new(population * 5, 6), // special facility: 2
            Table::new(population * 5, 5), // call forwarding: 3
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
