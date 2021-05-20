use crate::storage::index::AccessHistory;
use crate::storage::index::LogSequenceNumber;
use crate::storage::index::RwTable;
use crate::storage::row::Row;
use crate::storage::row::Tuple;
use crate::storage::table::Table;

use parking_lot::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub mod datatype;

pub mod catalog;

pub mod index;

pub mod table;

pub mod row;

#[derive(Debug)]
pub struct Database(Vec<Record>);

#[derive(Debug)]
pub struct Record {
    lsn: LogSequenceNumber,
    rw_table: RwTable,
    row: Option<Row>,
}

impl Database {
    pub fn new(population: usize, index_cnt: usize) -> Self {
        let mut v = Vec::with_capacity(population * index_cnt);

        for i in 0..(population * index_cnt) {
            v.push(Record::new(i));
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
        self.0[offset].row = Some(Row::new(table));
    }
}

pub fn calculate_offset(key: usize, index_id: usize, population: usize) -> usize {
    (index_id * population) + key
}

impl Record {
    fn new(primary_key: usize) -> Self {
        Record {
            lsn: LogSequenceNumber::new(),
            rw_table: RwTable::new(),
            row: None,
        }
    }

    pub fn get_row(&self, offset: usize) -> &Row {
        &self.row.as_ref().unwrap()
    }

    pub fn get_lsn(&self, offset: usize) -> &LogSequenceNumber {
        &self.lsn
    }

    pub fn get_rw_table(&self, offset: usize) -> &RwTable {
        &self.rw_table
    }
}
