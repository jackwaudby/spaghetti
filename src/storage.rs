use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::scheduler::TransactionInfo;
use crate::storage::row::Tuple;

use nohash_hasher::IntMap;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub mod datatype;

pub mod catalog;

pub mod row;

type Column = Vec<Tuple>;

#[derive(Debug)]
pub struct SmallBankDatabase(IntMap<usize, Table>);

#[derive(Debug)]
pub struct Table {
    columns: IntMap<usize, Column>,
    lsns: Vec<AtomicU64>,
    rw_tables: Vec<AtomicLinkedList<Access>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionInfo),
    Write(TransactionInfo),
}

impl SmallBankDatabase {
    pub fn new(population: usize) -> Self {
        let mut map = IntMap::default();

        map.insert(0, Table::new(population, 1)); // accounts
        map.insert(1, Table::new(population, 2)); // checking
        map.insert(2, Table::new(population, 2)); // saving

        SmallBankDatabase(map)
    }

    pub fn get_table(&self, id: usize) -> &Table {
        self.0.get(&id).unwrap()
    }

    pub fn get_mut_table(&mut self, id: usize) -> &mut Table {
        self.0.get_mut(&id).unwrap()
    }
}

impl Table {
    pub fn new(population: usize, column_cnt: usize) -> Self {
        let mut columns = IntMap::default();
        for id in 0..column_cnt {
            let mut column = Vec::with_capacity(population);
            for _ in 0..population {
                column.push(Tuple::new())
            }
            columns.insert(id, column);
        }

        let mut lsns = Vec::with_capacity(population);
        let mut rw_tables = Vec::with_capacity(population);

        for _ in 0..population {
            lsns.push(AtomicU64::new(0));
            rw_tables.push(AtomicLinkedList::new())
        }

        Table {
            columns,
            lsns,
            rw_tables,
        }
    }

    pub fn add_column(&mut self, id: usize, column: Column) {
        self.columns.insert(id, column);
    }

    pub fn get_tuple(&self, id: usize, offset: usize) -> &Tuple {
        &self.columns.get(&id).unwrap()[offset]
    }

    pub fn get_lsn(&self, offset: usize) -> &AtomicU64 {
        &self.lsns[offset]
    }

    pub fn get_rwtable(&self, offset: usize) -> &AtomicLinkedList<Access> {
        &self.rw_tables[offset]
    }
}

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
