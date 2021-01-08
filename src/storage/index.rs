extern crate chashmap;

use std::fmt;

use crate::storage::row::Row;
use chashmap::CHashMap;
use chashmap::{ReadGuard, WriteGuard};

#[derive(Debug)]
pub struct Index {
    name: String,
    i: CHashMap<u64, Row>,
}

impl Index {
    pub fn init(name: &str) -> Self {
        Index {
            name: String::from(name),
            i: CHashMap::new(),
        }
    }
    pub fn index_exists(&self, key: u64) -> bool {
        self.i.contains_key(&key)
    }
    pub fn index_insert(&self, key: u64, row: Row) -> Option<Row> {
        self.i.insert(key, row)
    }
    pub fn index_read(&self, key: u64) -> Option<ReadGuard<u64, Row>> {
        self.i.get(&key)
    }

    pub fn index_read_mut(&self, key: u64) -> Option<WriteGuard<u64, Row>> {
        self.i.get_mut(&key)
    }
}

// [name,num_rows]
impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{},{}]", self.name, self.i.len())
    }
}
