use crate::workloads::PrimaryKey;
use std::collections::HashSet;

pub struct ActiveTransaction {
    /// Transaction ID.
    tid: u64,

    /// Start epoch.
    start_epoch: u64,

    /// List of predecessors.
    predecessors: Option<HashSet<u64>>,

    /// List of keys written by transaction.
    keys_written: Option<Vec<(String, PrimaryKey)>>,

    /// List of keys read by transaction.
    keys_read: Option<Vec<(String, PrimaryKey)>>,

    /// Keys inserted
    keys_inserted: Option<Vec<(String, PrimaryKey)>>,
}

impl ActiveTransaction {
    /// Create new runtime information tracker for a transaction.
    pub fn new(tid: u64, start_epoch: u64) -> ActiveTransaction {
        ActiveTransaction {
            tid,
            start_epoch,
            predecessors: Some(HashSet::new()),
            keys_written: Some(vec![]),
            keys_read: Some(vec![]),
            keys_inserted: Some(vec![]),
        }
    }

    pub fn get_start_epoch(&self) -> u64 {
        self.start_epoch
    }

    /// Get transaction id.
    pub fn get_tid(&self) -> u64 {
        self.tid
    }

    /// Get predecessor list.
    pub fn get_predecessors(&mut self) -> HashSet<u64> {
        self.predecessors.take().unwrap()
    }

    /// Add predecessor
    pub fn add_predecessor(&mut self, id: u64) {
        self.predecessors.as_mut().unwrap().insert(id);
    }

    /// Get the list of keys updated/deleted by this transaction.
    pub fn get_keys_written(&mut self) -> Vec<(String, PrimaryKey)> {
        self.keys_written.take().unwrap()
    }

    /// Add key to list of those updated/deleted by this transaction.
    pub fn add_key_written(&mut self, key: (String, PrimaryKey)) {
        self.keys_written.as_mut().unwrap().push(key);
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
