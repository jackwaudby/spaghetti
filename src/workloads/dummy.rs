use crate::storage::datatype::Data;
use crate::storage::flattable::FlatTable;

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
pub struct DummyDatabase([FlatTable; 1]);

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DummyTransaction {
    Write,
    WriteAbort,
    Read,
    ReadWrite,
}

impl DummyDatabase {
    pub fn new(population: usize) -> Self {
        let array: [FlatTable; 1] = [FlatTable::new(population)]; // 1 table with 1 column

        Self(array)
    }

    pub fn insert_value(&mut self, table_id: usize, offset: usize, value: Data) {
        self.get_mut_table(table_id)
            .get_row(offset)
            .get_tuple()
            .get()
            .init_value(value)
            .unwrap();
    }

    pub fn get_table(&self, id: usize) -> &FlatTable {
        &self.0[id]
    }

    pub fn get_mut_table(&mut self, id: usize) -> &mut FlatTable {
        &mut self.0[id]
    }
}
