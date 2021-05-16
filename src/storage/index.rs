use crate::common::error::NonFatalError;
use crate::storage::datatype::Data;
use crate::storage::row::{Access, OperationResult, Row};
use crate::workloads::PrimaryKey;

use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Each table has an index that owns all rows stored in that table.
#[derive(Debug)]
pub struct Index {
    /// Index name.
    name: String,

    /// Data.
    data: IntMap<PrimaryKey, Arc<Mutex<Row>>>,

    /// Log sequence number.
    lsns: IntMap<PrimaryKey, Arc<LogSequenceNumber>>,

    /// Accesses.
    rws: IntMap<PrimaryKey, Arc<Mutex<RwTable>>>,
}

/// List of access made on a row.
#[derive(Debug)]
pub struct RwTable {
    prv: u64,
    entries: VecDeque<(u64, Access)>,
}

/// Log sequence number of operations on a row.
#[derive(Debug)]
pub struct LogSequenceNumber {
    lsn: AtomicU64,
}

impl Index {
    /// Create a new index.
    pub fn init(name: &str) -> Self {
        Index {
            name: String::from(name),
            data: IntMap::default(),
            lsns: IntMap::default(),
            rws: IntMap::default(),
        }
    }

    /// Get index name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Check if a key exists in the index.
    pub fn key_exists(&self, key: PrimaryKey) -> bool {
        self.data.contains_key(&key)
    }

    /// Insert a row with key into the index.
    pub fn insert(&mut self, key: &PrimaryKey, row: Row) {
        self.data.insert(key.clone(), Arc::new(Mutex::new(row)));
        self.lsns
            .insert(key.clone(), Arc::new(LogSequenceNumber::new()));
        self.rws
            .insert(key.clone(), Arc::new(Mutex::new(RwTable::new())));
    }

    /// Get a handle to row with key.
    pub fn get_row(&self, key: &PrimaryKey) -> Result<&Arc<Mutex<Row>>, NonFatalError> {
        self.data
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))
    }

    pub fn get_lsn(&self, key: &PrimaryKey) -> Result<&Arc<LogSequenceNumber>, NonFatalError> {
        self.lsns
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))
    }

    pub fn get_rw_table(&self, key: &PrimaryKey) -> Result<&Arc<Mutex<RwTable>>, NonFatalError> {
        self.rws
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))
    }

    /// Read columns from a row with the given key.
    pub fn read(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .data
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        let res = row.get_values(columns)?;
        Ok(res)
    }

    /// Write values to columns in a row with the given key.
    // TODO: add get field
    pub fn update<F>(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: F,
    ) -> Result<OperationResult, NonFatalError>
    where
        F: Fn(Option<Vec<Data>>, Option<&[Data]>) -> Result<Vec<Data>, NonFatalError>,
    {
        let rh = self
            .data
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        let current_values;

        if let Some(columns) = read {
            let mut res = row.get_values(columns)?;
            current_values = Some(res.get_values());
        } else {
            current_values = None;
        }

        let new_values = f(current_values, params)?;

        let res = row.set_values(&columns, &new_values)?;
        Ok(res)
    }

    /// Append value to column in a row with the given key.
    pub fn append(
        &self,
        key: &PrimaryKey,
        column: &str,
        value: Data,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .data
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        let res = row.append_value(column, value)?;
        Ok(res)
    }

    /// Commit modifications to a row - rows marked for delete are removed.
    pub fn commit(&self, key: &PrimaryKey) -> Result<(), NonFatalError> {
        let rh = self
            .data
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;

        let mut row = rh.lock(); // lock row
        row.commit(); // commit

        Ok(())
    }

    /// Revert modifications to a row.
    pub fn revert(&self, key: &PrimaryKey) -> Result<(), NonFatalError> {
        let rh = self
            .data
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let mut row = rh.lock();
        row.revert();
        Ok(())
    }
}

impl LogSequenceNumber {
    fn new() -> Self {
        LogSequenceNumber {
            lsn: AtomicU64::new(0),
        }
    }

    pub fn get(&self) -> u64 {
        self.lsn.load(Ordering::SeqCst)
    }

    pub fn replace(&self, prv: u64) {
        self.lsn.store(prv, Ordering::SeqCst);
    }
}

impl RwTable {
    fn new() -> Self {
        RwTable {
            prv: 0,
            entries: VecDeque::new(),
        }
    }

    pub fn push_front(&mut self, access: Access) -> u64 {
        let id = self.prv;
        self.prv += 1;
        self.entries.push_front((id, access));
        id
    }

    pub fn snapshot(&self) -> VecDeque<(u64, Access)> {
        self.entries.clone()
    }

    pub fn erase(&mut self, entry: (u64, Access)) {
        let index = self.entries.iter().position(|r| r == &entry).unwrap();
        self.entries.remove(index);
    }

    pub fn erase_all(&mut self, entry: Access) {
        self.entries.retain(|(_, y)| y != &entry);
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.data).unwrap();
        Ok(())
    }
}

impl fmt::Display for RwTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut rw = String::new();
        let n = self.entries.len();

        if n > 0 {
            rw.push_str("[");

            for (prv, access) in &self.entries {
                rw.push_str(&format!("{}-{}", prv, access));
                rw.push_str(", ");
            }
            let len = rw.len();
            rw.truncate(len - 2);
            let (prv, access) = self.entries[n - 1].clone();
            rw.push_str(&format!("]"));
        } else {
            rw.push_str("[]");
        }

        write!(f, "prv: {}, rw: {}", self.prv, rw).unwrap();
        Ok(())
    }
}
