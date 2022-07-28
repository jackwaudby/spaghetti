use std::collections::hash_map::RandomState;
use std::sync::Mutex;

use coarsetime::Clock;
use dashmap::DashMap;
use scc::HashMap;

use super::lock::Lock;
use super::lock_table::LockTable;

#[derive(Debug)]
struct RelationTable(DashMap<u64, LockTable>);

#[derive(Debug)]
struct TimestampTable(HashMap<(u64, u64), u64>);

#[derive(Debug)]
pub struct LockManager {
    timestamp_table: TimestampTable,
    table_to_table_locks: RelationTable,
    rows: usize,
    manager_lock: Mutex<u8>,
}

impl RelationTable {
    fn new(tables: usize) -> Self {
        let table_to_lock_table_map = DashMap::with_capacity(tables);

        Self(table_to_lock_table_map)
    }
}

impl TimestampTable {
    fn new(cores: usize) -> Self {
        let txn_to_ts_map: HashMap<(u64, u64), u64, RandomState> =
            HashMap::new(cores, RandomState::new());
        Self(txn_to_ts_map)
    }
}

impl LockManager {
    pub fn new(cores: usize, tables: usize, rows: usize) -> Self {
        let timestamp_table = TimestampTable::new(cores);
        let table_to_table_locks = RelationTable::new(tables);
        let manager_lock = Mutex::new(0);

        Self {
            timestamp_table,
            table_to_table_locks,
            rows,
            manager_lock,
        }
    }

    pub fn start(&self, transaction_id: (u64, u64)) -> u64 {
        let ts = Clock::now_since_epoch().as_nanos();
        let res = self.timestamp_table.0.insert(transaction_id, ts);

        match res {
            Ok(_) => {}
            Err(_) => {}
        }

        ts
    }

    pub fn lock(
        &self,
        transaction_id: (u64, u64),
        table_id: u64,
        row_id: u64,
        exclusive: bool,
    ) -> bool {
        let mut inserted = false;

        while !inserted {
            let lock_table = self.table_to_table_locks.0.get(&table_id);

            match lock_table {
                // lock table exists for this relation
                Some(lock_table) => {
                    while !inserted {
                        let found = lock_table.get_row_to_lock_map().contains_key(&row_id);

                        // case 2: no lock for this row in this relation so make one
                        if !found {
                            let guard = lock_table.get_table_lock().lock().unwrap();

                            let found = lock_table.get_row_to_lock_map().contains_key(&row_id);
                            if !found {
                                let new_lock = Lock::new();
                                lock_table
                                    .get_row_to_lock_map()
                                    .insert(row_id, Mutex::new(new_lock));
                            }

                            drop(guard);
                        } else {
                            // case 3: lock exists in lock table for this relation
                            let lock_ref = lock_table.get_row_to_lock_map().get(&row_id).unwrap();
                            let mut guard = lock_ref.lock().unwrap();
                            let inner = &*guard;

                            if inner.other_transaction_holds_write_lock(transaction_id) {
                                if self.wait_die(transaction_id, exclusive, inner) {
                                    drop(guard);

                                    continue;
                                } else {
                                    drop(guard);
                                    return false;
                                }
                            } else if exclusive && inner.multiple_read_locks_held() {
                                if self.wait_die(transaction_id, exclusive, &inner) {
                                    drop(guard);

                                    continue;
                                } else {
                                    drop(guard);
                                    return false;
                                }
                            } else if exclusive && inner.get_read_info().len() == 1 {
                                if !inner.get_read_info().contains(&transaction_id) {
                                    if self.wait_die(transaction_id, exclusive, &inner) {
                                        drop(guard);

                                        continue;
                                    } else {
                                        drop(guard);
                                        return false;
                                    }
                                }
                            }

                            let mut new_lock_ptr = inner.clone();

                            if exclusive {
                                new_lock_ptr.set_write_info(Some(transaction_id));
                            } else {
                                new_lock_ptr.get_mut_read_info().insert(transaction_id);
                            }

                            *guard = new_lock_ptr;
                            drop(guard);
                            inserted = true;
                        }
                    }

                    return true;
                }

                // case 1: no lock table for this relation so make one
                None => {
                    let guard = self.manager_lock.lock();
                    // double check nobody beat me to it
                    let found = self.table_to_table_locks.0.contains_key(&table_id);
                    if !found {
                        let lock_table = LockTable::new(self.rows);
                        self.table_to_table_locks.0.insert(table_id, lock_table);
                    }
                    drop(guard);
                }
            }
        }

        true
    }

    // true = wait, false = die
    fn wait_die(&self, req_id: (u64, u64), exclusive: bool, lock: &Lock) -> bool {
        let mut result = true;

        // get lock requester timestamp
        let req_ts = self.timestamp_table.0.read(&req_id, |_, v| *v).unwrap();

        match lock.get_write_info() {
            Some(hol_id) => {
                let hol_ts = self.timestamp_table.0.read(&hol_id, |_, v| *v).unwrap();

                if req_ts >= hol_ts {
                    result = false;
                }
            }
            None => {
                result = false;
            }
        }

        if exclusive {
            for reader_id in lock.get_read_info() {
                if let Some(that_ts) = self.timestamp_table.0.read(&reader_id, |_, v| *v) {
                    if req_ts >= that_ts {
                        result = false;
                    }
                }
            }
        }

        result
    }

    pub fn unlock(&self, transaction_id: (u64, u64), table_id: u64, row_id: u64) -> bool {
        let lock_table = self.table_to_table_locks.0.get(&table_id).unwrap();

        // protected in this loop
        let mut deleted = false;

        while !deleted {
            // get a ptr to the underlying lock
            let lock_ref = lock_table.get_row_to_lock_map().get(&row_id).unwrap();

            let mut guard = lock_ref.lock().unwrap();

            let lock_ptr = &*guard;

            // get a copy so we can edit it
            let mut new_lock_ptr = lock_ptr.clone();

            // if I held it in write mode then reset
            if let Some(id) = new_lock_ptr.get_write_info() {
                if *id == transaction_id {
                    new_lock_ptr.set_write_info(None);
                }
            }

            // remove any read locks I had
            new_lock_ptr.get_mut_read_info().remove(&transaction_id);

            // convert a ptr so can swap
            *guard = new_lock_ptr;

            drop(guard);
            deleted = true;
        }

        true
    }

    pub fn end(&self, transaction_id: (u64, u64)) {
        self.timestamp_table.0.remove(&transaction_id);
    }
}
