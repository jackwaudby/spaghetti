use crate::server::storage::index::Index;
use crate::server::storage::row::Row;
use crate::workloads::PrimaryKey;

use std::sync::Arc;

/// Represents the runtime information of a transaction.
#[derive(Debug)]
pub struct ActiveTransaction {
    /// Transaction ID.
    tid: String,

    /// Locks held by the transaction.
    locks_held: Vec<PrimaryKey>,

    /// Rows the transaction wishes to insert.
    rows_to_insert: Option<Vec<(Arc<Index>, Row)>>,

    /// Rows the transaction wishes to delete.
    rows_to_delete: Option<Vec<(Arc<Index>, PrimaryKey)>>,
}

impl ActiveTransaction {
    /// Create new runtime information tracker for a transaction.
    pub fn new(tid: &str) -> Self {
        ActiveTransaction {
            tid: tid.to_string(),
            locks_held: vec![],
            rows_to_insert: Some(vec![]),
            rows_to_delete: Some(vec![]),
        }
    }

    /// Get transaction id.
    pub fn get_tid(&self) -> String {
        self.tid.clone()
    }

    /// Get a shared reference to the locks held by this transaction.
    pub fn get_locks_held(&self) -> &Vec<PrimaryKey> {
        &self.locks_held
    }

    /// Add a lock to this transaction's list.
    pub fn add_lock(&mut self, lock_id: PrimaryKey) {
        self.locks_held.push(lock_id);
    }

    /// Get rows transaction wishes to insert.
    pub fn get_rows_to_insert(&mut self) -> Option<Vec<(Arc<Index>, Row)>> {
        self.rows_to_insert.take()
    }

    /// Add row to insert.
    pub fn add_row_to_insert(&mut self, index: Arc<Index>, row: Row) {
        self.rows_to_insert.as_mut().unwrap().push((index, row));
    }

    /// Get a shared reference to the rows this transaction wishes to delete.
    pub fn get_rows_to_delete(&mut self) -> Vec<(Arc<Index>, PrimaryKey)> {
        self.rows_to_delete.take().unwrap()
    }

    /// Add row to delete.
    pub fn add_row_to_delete(&mut self, index: Arc<Index>, key: PrimaryKey) {
        self.rows_to_delete.as_mut().unwrap().push((index, key));
    }
}
