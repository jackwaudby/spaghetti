use crate::common::ds::atomic_extent_vector::AtomicExtentVec;
use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::scheduler::TransactionInfo;
use crate::storage::catalog::Catalog;
use crate::storage::row::Row;

use std::fmt;

pub mod datatype;

pub mod catalog;

pub mod row;

#[derive(Debug)]
pub struct Database(Vec<Table>);

#[derive(Debug)]
pub struct Table {
    schema: Catalog,
    rows: AtomicExtentVec<Row>,
    lsns: AtomicExtentVec<u64>,
    rw_tables: AtomicExtentVec<AtomicLinkedList<Access>>,
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
        let mut lsns = AtomicExtentVec::reserve(population);
        let mut rw_tables = AtomicExtentVec::reserve(population);

        for _ in 0..population {
            lsns.push(0);
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

    pub fn get_lsn(&self) -> &AtomicExtentVec<u64> {
        &self.lsns
    }

    pub fn get_rw_tables(&self) -> &AtomicExtentVec<AtomicLinkedList<Access>> {
        &self.rw_tables
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
