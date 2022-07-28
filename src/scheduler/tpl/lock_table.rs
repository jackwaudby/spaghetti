use std::sync::Mutex;

use dashmap::DashMap;

use super::lock::Lock;

#[derive(Debug)]
pub struct LockTable {
    row_to_lock_map: DashMap<u64, Mutex<Lock>>,
    table_lock: Mutex<u8>,
}

impl LockTable {
    pub fn new(rows: usize) -> Self {
        let row_to_lock_map = DashMap::with_capacity(rows);
        Self {
            row_to_lock_map,
            table_lock: Mutex::new(0),
        }
    }

    pub fn get_row_to_lock_map(&self) -> &DashMap<u64, Mutex<Lock>> {
        &self.row_to_lock_map
    }

    pub fn get_table_lock(&self) -> &Mutex<u8> {
        &self.table_lock
    }
}
