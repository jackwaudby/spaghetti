use crate::workloads::PrimaryKey;

use std::collections::HashSet;

/// Holds the runtime information of transaction in the HIT protocol.
pub struct ActiveTransaction {
    /// Transaction id.
    tid: u64,

    /// Start epoch.
    start_epoch: u64,

    /// List of predecessors upon read.
    pur: Option<HashSet<u64>>,

    /// List of predecessors upon write.
    puw: Option<HashSet<u64>>,

    /// List of keys inserted.
    keys_inserted: Option<Vec<(String, PrimaryKey)>>,

    /// List of keys updated.
    keys_updated: Option<Vec<(String, PrimaryKey)>>,

    /// List of keys deleted.
    keys_deleted: Option<Vec<(String, PrimaryKey)>>,

    /// List of keys read.
    keys_read: Option<Vec<(String, PrimaryKey)>>,
}

impl ActiveTransaction {
    /// Create runtime information for a transaction.
    pub fn new(tid: u64, start_epoch: u64) -> ActiveTransaction {
        ActiveTransaction {
            tid,
            start_epoch,
            pur: Some(HashSet::new()),
            puw: Some(HashSet::new()),
            keys_updated: Some(vec![]),
            keys_deleted: Some(vec![]),
            keys_read: Some(vec![]),
            keys_inserted: Some(vec![]),
        }
    }

    /// Get the epoch the transaction started in.
    pub fn get_start_epoch(&self) -> u64 {
        self.start_epoch
    }

    /// Get the transaction's id.
    pub fn get_tid(&self) -> u64 {
        self.tid
    }

    /// Get the transaction's predecessor upon read list.
    pub fn get_pur(&mut self) -> HashSet<u64> {
        self.pur.take().unwrap()
    }

    /// Get the transaction's predecessor upon write list.
    pub fn get_puw(&mut self) -> HashSet<u64> {
        self.puw.take().unwrap()
    }

    /// Register predecessor upon read.
    pub fn add_pur(&mut self, id: u64) {
        self.pur.as_mut().unwrap().insert(id);
    }

    /// Register predecessor upon write.
    pub fn add_puw(&mut self, id: u64) {
        self.puw.as_mut().unwrap().insert(id);
    }

    /// Get the list of keys updated/deleted by this transaction.
    pub fn get_keys_updated(&mut self) -> Vec<(String, PrimaryKey)> {
        self.keys_updated.take().unwrap()
    }

    /// Add key to list of those updated/deleted by this transaction.
    pub fn add_key_updated(&mut self, key: (String, PrimaryKey)) {
        self.keys_updated.as_mut().unwrap().push(key);
    }

    /// Get the list of keys updated/deleted by this transaction.
    pub fn get_keys_deleted(&mut self) -> Vec<(String, PrimaryKey)> {
        self.keys_deleted.take().unwrap()
    }

    /// Add key to list of those updated/deleted by this transaction.
    pub fn add_key_deleted(&mut self, key: (String, PrimaryKey)) {
        self.keys_deleted.as_mut().unwrap().push(key);
    }

    /// Get the list of keys read by this transaction.
    pub fn get_keys_read(&mut self) -> Vec<(String, PrimaryKey)> {
        self.keys_read.take().unwrap()
    }

    /// Add key to list of those read by this transaction.
    pub fn add_key_read(&mut self, key: (String, PrimaryKey)) {
        self.keys_read.as_mut().unwrap().push(key);
    }

    /// Get the list of keys inserted by this transaction.
    pub fn get_keys_inserted(&mut self) -> Vec<(String, PrimaryKey)> {
        self.keys_inserted.take().unwrap()
    }

    /// Add key to list of those read by this transaction.
    pub fn add_key_inserted(&mut self, key: (String, PrimaryKey)) {
        self.keys_inserted.as_mut().unwrap().push(key);
    }
}
