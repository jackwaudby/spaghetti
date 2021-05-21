use crate::storage::row::Row;
use crate::storage::table::Table;
use crate::storage::utils::{LogSequenceNumber, RwTable};

use crossbeam_utils::CachePadded;
use std::sync::Arc;

pub mod datatype;

pub mod catalog;

pub mod utils;

pub mod table;

pub mod row;

#[derive(Debug)]
pub struct Database(Vec<Record>);

#[derive(Debug)]
pub struct Record {
    lsn: CachePadded<LogSequenceNumber>,
    rw_table: CachePadded<RwTable>,
    row: CachePadded<Option<Row>>,
}

impl Database {
    pub fn new(population: usize, index_cnt: usize) -> Self {
        let mut v = Vec::with_capacity(population * index_cnt);

        for _ in 0..(population * index_cnt) {
            v.push(Record::new());
        }

        Database(v)
    }

    pub fn get_record(&self, offset: usize) -> &Record {
        &self.0[offset]
    }

    pub fn get_row(&self, offset: usize) -> &Row {
        &self.0[offset].row.as_ref().unwrap()
    }

    pub fn set_row(&mut self, offset: usize, table: Arc<Table>) {
        self.0[offset].row = CachePadded::new(Some(Row::new(table)));
    }
}

pub fn calculate_offset(key: usize, index_id: usize, population: usize) -> usize {
    (index_id * population) + key
}

impl Record {
    fn new() -> Self {
        Record {
            lsn: CachePadded::new(LogSequenceNumber::new()),
            rw_table: CachePadded::new(RwTable::new()),
            row: CachePadded::new(None),
        }
    }

    pub fn get_row(&self) -> &Row {
        &self.row.as_ref().unwrap()
    }

    pub fn get_lsn(&self) -> &LogSequenceNumber {
        &self.lsn
    }

    pub fn get_rw_table(&self) -> &RwTable {
        &self.rw_table
    }
}
