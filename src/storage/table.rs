use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::common::ds::locked_linked_list::LockedLinkedList;
use crate::common::error::NonFatalError;
use crate::storage::access::Access;
use crate::storage::tuple::Tuple;
use crate::storage::PrimaryKey;

use nohash_hasher::IntMap;
use rustc_hash::FxHashMap;
use std::sync::atomic::AtomicU64;

pub type Column = Vec<Tuple>;

#[derive(Debug)]
pub struct Table {
    columns: IntMap<usize, Column>,
    exists: FxHashMap<PrimaryKey, usize>,
    lsns: Vec<AtomicU64>,
    rw_tables: Vec<AtomicLinkedList<Access>>,
    locked_rw_tables: Vec<LockedLinkedList<Access>>,
}

impl Table {
    pub fn new(population: usize, column_cnt: usize) -> Self {
        let exists = FxHashMap::default();

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
        let mut locked_rw_tables = Vec::with_capacity(population);

        for _ in 0..population {
            lsns.push(AtomicU64::new(0));
            rw_tables.push(AtomicLinkedList::new());
            locked_rw_tables.push(LockedLinkedList::new());
        }

        Table {
            columns,
            exists,
            lsns,
            rw_tables,
            locked_rw_tables,
        }
    }

    pub fn add_column(&mut self, id: usize, column: Column) {
        self.columns.insert(id, column);
    }

    pub fn get_tuple(&self, id: usize, offset: usize) -> &Tuple {
        &self.columns.get(&id).unwrap()[offset]
    }

    pub fn get_mut_exists(&mut self) -> &mut FxHashMap<PrimaryKey, usize> {
        &mut self.exists
    }

    pub fn exists(&self, key: PrimaryKey) -> Result<usize, NonFatalError> {
        let offset = self.exists.get(&key);
        match offset {
            Some(offset) => Ok(*offset),
            None => Err(NonFatalError::RowNotFound(
                "todo".to_string(),
                "todo".to_string(),
            )),
        }
    }

    pub fn get_lsn(&self, offset: usize) -> &AtomicU64 {
        &self.lsns[offset]
    }

    pub fn get_rwtable(&self, offset: usize) -> &AtomicLinkedList<Access> {
        &self.rw_tables[offset]
    }

    pub fn get_locked_rwtable(&self, offset: usize) -> &LockedLinkedList<Access> {
        &self.locked_rw_tables[offset]
    }
}
