use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::common::error::NonFatalError;
use crate::storage::access::Access;
use crate::storage::tuple::Tuple;
use crate::storage::PrimaryKey;

// use nohash_hasher::IntMap;
// use rustc_hash::FxHashMap;
// use std::fmt;
use std::sync::atomic::AtomicU64;

#[derive(Debug)]
pub struct FlatTable {
    rows: Vec<Row>,
}

#[derive(Debug)]
pub struct Row {
    tuple: Tuple,
    lsn: AtomicU64,
    accesses: AtomicLinkedList<Access>,
}

impl Row {
    fn new() -> Self {
        Row {
            tuple: Tuple::new(),
            lsn: AtomicU64::new(0),
            accesses: AtomicLinkedList::new(),
        }
    }

    pub fn get_tuple(&self) -> &Tuple {
        &self.tuple
    }

    pub fn get_lsn(&self) -> &AtomicU64 {
        &self.lsn
    }

    pub fn get_rwtable(&self) -> &AtomicLinkedList<Access> {
        &self.accesses
    }
}

impl FlatTable {
    pub fn new(population: usize) -> Self {
        let mut rows = Vec::with_capacity(population);

        for _ in 0..population {
            rows.push(Row::new());
        }

        FlatTable { rows }
    }

    pub fn get_row(&self, offset: usize) -> &Row {
        &self.rows[offset]
    }
}

// impl fmt::Display for Table {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "TODO").unwrap();

//         Ok(())
//     }
// }
