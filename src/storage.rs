use crate::common::ds::atomic_extent_vector::AtomicExtentVec;
use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::scheduler::TransactionInfo;
use crate::storage::catalog::Catalog;
use crate::storage::row::Row;

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod datatype;

pub mod catalog;

pub mod row;

#[derive(Debug)]
pub struct Database(Vec<Table>);

#[derive(Debug)]
pub struct Table {
    schema: Catalog,
    rows: AtomicExtentVec<Row>,
    //  lsns: AtomicExtentVec<u64>,
    lsns: Vec<AtomicU64>,
    rw_tables: Vec<AtomicLinkedList<Access>>,
    // TODO: hashmap
}

impl Database {
    pub fn new(tables: usize) -> Self {
        Database(Vec::with_capacity(tables))
    }

    pub fn add(&mut self, table: Table) {
        self.0.push(table);
    }

    pub fn get_table(&self, n: usize) -> &Table {
        &self.0[n]
    }

    pub fn get_mut_table(&mut self, n: usize) -> &mut Table {
        &mut self.0[n]
    }
}

impl Table {
    pub fn new(population: usize, schema: Catalog) -> Self {
        let rows = AtomicExtentVec::reserve(population);
        //    let mut lsns = AtomicExtentVec::reserve(population);
        let mut lsns = Vec::with_capacity(population);
        let mut rw_tables = Vec::with_capacity(population);

        for _ in 0..population {
            lsns.push(AtomicU64::new(0));
            rw_tables.push(AtomicLinkedList::new())
        }

        Table {
            schema,
            rows,
            lsns,
            rw_tables,
        }
    }

    pub fn get_schema(&self) -> &Catalog {
        &self.schema
    }

    pub fn get_rows(&self) -> &AtomicExtentVec<Row> {
        &self.rows
    }

    pub fn get_mut_rows(&mut self) -> &mut AtomicExtentVec<Row> {
        &mut self.rows
    }

    // pub fn get_lsn(&self) -> &AtomicExtentVec<u64> {
    //     &self.lsns
    // }

    // pub fn get_lsn_get<'g>(&self, offset: usize, guard: &'g Guard) -> &'g u64 {
    //     self.lsns.get(offset, guard)
    // }

    pub fn get_lsn(&self, offset: usize) -> u64 {
        self.lsns[offset].load(Ordering::Relaxed)
    }

    pub fn replace_lsn(&self, offset: usize, val: u64) {
        self.lsns[offset].store(val, Ordering::Relaxed);
    }

    pub fn get_rw_tables(&self) -> &Vec<AtomicLinkedList<Access>> {
        &self.rw_tables
    }

    pub fn get_rw_tables_get(&self, offset: usize) -> &AtomicLinkedList<Access> {
        &self.rw_tables[offset]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionInfo),
    Write(TransactionInfo),
}

/// Returns true if the access types are the same.
pub fn access_eq(a: &Access, b: &Access) -> bool {
    matches!(
        (a, b),
        (&Access::Read(..), &Access::Read(..)) | (&Access::Write(..), &Access::Write(..))
    )
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Access::*;
        match &self {
            Read(id) => write!(f, "r-{}", id),
            Write(id) => write!(f, "w-{}", id),
        }
    }
}
