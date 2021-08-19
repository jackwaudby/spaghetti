use crate::storage::datatype::Data;
use crate::storage::table::Table;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use strum_macros::EnumIter;

pub mod loader;

pub mod paramgen;

pub mod procedures;

lazy_static! {
    pub static ref DUMMY_SF_MAP: HashMap<u64, usize> = {
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

#[derive(Debug)]
pub struct DummyDatabase([Table; 1]);

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DummyTransaction {
    Write,
    Read,
    ReadWrite,
}

impl DummyDatabase {
    pub fn new(population: usize) -> Self {
        let array: [Table; 1] = [Table::new(population, 1)]; // 1 table with 1 column

        Self(array)
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
