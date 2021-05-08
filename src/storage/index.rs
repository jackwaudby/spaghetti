use crate::common::error::NonFatalError;
use crate::scheduler::TransactionInfo;
use crate::storage::datatype::Data;
use crate::storage::row::{OperationResult, Row};
use crate::workloads::PrimaryKey;

use nohash_hasher::IntMap;
//use parking_lot::Mutex;
use spin::Mutex;
use std::fmt;
use std::sync::Arc;

/// Each table has at least 1 index that owns all rows stored in that table.
#[derive(Debug)]
pub struct Index {
    /// Index name.
    name: String,

    /// Data.
    map: IntMap<PrimaryKey, Arc<Mutex<Row>>>,
}

impl Index {
    /// Create a new index.
    pub fn init(name: &str) -> Self {
        Index {
            name: String::from(name),
            map: IntMap::default(), // TODO: with_capacity()
        }
    }

    /// Get index name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Check if a key exists in the index.
    pub fn key_exists(&self, key: PrimaryKey) -> bool {
        self.map.contains_key(&key)
    }

    /// Insert a row with key into the index.
    pub fn insert(&mut self, key: &PrimaryKey, row: Row) {
        self.map.insert(key.clone(), Arc::new(Mutex::new(row)));
    }

    /// Get a handle to row with key.
    pub fn get_row(&self, key: &PrimaryKey) -> Result<&Arc<Mutex<Row>>, NonFatalError> {
        self.map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))
    }

    /// Read columns from a row with the given key.
    pub fn read(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        let res = row.get_values(columns, tid)?;
        Ok(res)
    }

    /// Write values to columns in a row with the given key.
    pub fn update<F>(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: F,
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError>
    where
        F: Fn(Option<Vec<Data>>, Option<&[Data]>) -> Result<Vec<Data>, NonFatalError>,
    {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        let current_values;
        if let Some(columns) = read {
            let mut res = row.get_values(columns, tid)?;
            current_values = Some(res.get_values());
        } else {
            current_values = None;
        }
        let new_values = f(current_values, params)?;

        let res = row.set_values(&columns, &new_values, tid)?;
        Ok(res)
    }

    /// Append value to column in a row with the given key.
    pub fn append(
        &self,
        key: &PrimaryKey,
        column: &str,
        value: Data,
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        let res = row.append_value(column, value, tid)?;
        Ok(res)
    }

    /// Set values in columns in a row with the given key, returning the old values.
    pub fn read_and_update(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        let res = row.get_and_set_values(columns, values, tid)?;
        Ok(res)
    }

    /// Commit modifications to a row - rows marked for delete are removed.
    pub fn commit(&self, key: &PrimaryKey, tid: &TransactionInfo) -> Result<(), NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;

        let mut row = rh.lock(); // lock row
        row.commit(tid); // commit

        Ok(())
    }

    /// Revert modifications to a row.
    pub fn revert(&self, key: &PrimaryKey, tid: &TransactionInfo) -> Result<(), NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        row.revert(tid);
        Ok(())
    }

    /// Revert reads to a `Row`.
    pub fn revert_read(
        &self,
        key: &PrimaryKey,
        tid: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        row.revert_read(tid);
        Ok(())
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.map).unwrap();
        Ok(())
    }
}
