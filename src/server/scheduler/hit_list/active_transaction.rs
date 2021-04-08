use crate::workloads::PrimaryKey;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct ActiveTransactionTracker {
    tracker: Arc<Vec<Mutex<Option<ActiveTransaction>>>>,
}

impl ActiveTransactionTracker {
    pub fn new(workers: usize) -> ActiveTransactionTracker {
        let mut at = Vec::with_capacity(workers);
        for _ in 0..workers {
            at.push(Mutex::new(None));
        }

        let tracker = Arc::new(at);

        ActiveTransactionTracker { tracker }
    }

    pub fn start_tracking(&self, worker_id: usize, tid: u64, start_epoch: u64) {
        let at = ActiveTransaction::new(tid, start_epoch);
        let mut state = self.tracker[worker_id].lock().unwrap();
        *state = Some(at);
    }

    pub fn get_start_epoch(&self, worker_id: usize) -> u64 {
        self.tracker[worker_id]
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .get_start_epoch()
    }

    pub fn clear(&self, worker_id: usize) {
        let mut state = self.tracker[worker_id].lock().unwrap();
        *state = None;
    }

    pub fn add_key(&self, worker_id: usize, key: (String, PrimaryKey), operation: Operation) {
        let mut state = self.tracker[worker_id].lock().unwrap();

        use Operation::*;
        match operation {
            Create => state.as_mut().unwrap().add_key_inserted(key),
            Read => state.as_mut().unwrap().add_key_read(key),
            Update => state.as_mut().unwrap().add_key_updated(key),
            Delete => state.as_mut().unwrap().add_key_deleted(key),
        }
    }

    pub fn get_keys(&self, worker_id: usize, operation: Operation) -> Vec<(String, PrimaryKey)> {
        let mut state = self.tracker[worker_id].lock().unwrap();

        use Operation::*;
        match operation {
            Create => state.as_mut().unwrap().get_keys_inserted(),
            Read => state.as_mut().unwrap().get_keys_read(),
            Update => state.as_mut().unwrap().get_keys_updated(),
            Delete => state.as_mut().unwrap().get_keys_deleted(),
        }
    }

    pub fn add_predecessor(&self, worker_id: usize, transaction_id: u64, predecessor: Predecessor) {
        let mut state = self.tracker[worker_id].lock().unwrap();
        use Predecessor::*;
        match predecessor {
            Read => state.as_mut().unwrap().add_pur(transaction_id),
            Write => state.as_mut().unwrap().add_puw(transaction_id),
        }
    }

    pub fn get_predecessors(&self, worker_id: usize, predecessor: Predecessor) -> HashSet<u64> {
        let mut state = self.tracker[worker_id].lock().unwrap();
        use Predecessor::*;
        match predecessor {
            Read => state.as_mut().unwrap().get_pur(),
            Write => state.as_mut().unwrap().get_puw(),
        }
    }
}

pub enum Predecessor {
    Read,
    Write,
}

pub enum Operation {
    Create,
    Read,
    Update,
    Delete,
}

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
