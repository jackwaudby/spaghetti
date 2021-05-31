use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::storage::access::Access;
use crate::storage::tuple::Tuple;

use nohash_hasher::IntMap;
use std::sync::atomic::AtomicU64;

pub type Column = Vec<Tuple>;

#[derive(Debug)]
pub struct Table {
    columns: IntMap<usize, Column>,
    lsns: Vec<AtomicU64>,
    rw_tables: Vec<AtomicLinkedList<Access>>,
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
