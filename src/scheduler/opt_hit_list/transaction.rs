use crate::scheduler::opt_hit_list::error::OptimisedHitListError;
use crate::scheduler::NonFatalError;
use crate::workloads::PrimaryKey;

use std::cell::UnsafeCell;
use std::sync::Mutex;

unsafe impl Sync for Transaction {}

/// Represents a transaction's state.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Aborted,
    Committed,
}

/// Type of predecessor
pub enum PredecessorUpon {
    Read,
    Write,
}

/// Represents a transaction.
#[derive(Debug)]
pub struct Transaction {
    /// Transaction id.
    id: u64,

    /// Start epoch.
    start_epoch: u64,

    /// Transaction state.
    state: Mutex<TransactionState>,

    /// Predecessors upon read.
    wait_list: UnsafeCell<Option<Vec<String>>>,

    /// Predecessors upon write.
    hit_list: UnsafeCell<Option<Vec<String>>>,

    /// List of keys inserted.
    keys_inserted: UnsafeCell<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys updated.
    keys_updated: UnsafeCell<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys deleted.
    keys_deleted: UnsafeCell<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys read.
    keys_read: UnsafeCell<Option<Vec<(String, PrimaryKey)>>>,
}

impl Transaction {
    /// Create new transaction
    pub fn new(id: u64, start_epoch: u64) -> Transaction {
        Transaction {
            id,
            start_epoch,
            state: Mutex::new(TransactionState::Active),
            wait_list: UnsafeCell::new(Some(vec![])),
            hit_list: UnsafeCell::new(Some(vec![])),
            keys_updated: UnsafeCell::new(Some(vec![])),
            keys_deleted: UnsafeCell::new(Some(vec![])),
            keys_read: UnsafeCell::new(Some(vec![])),
            keys_inserted: UnsafeCell::new(Some(vec![])),
        }
    }

    /// Get id of transaction.
    pub fn get_id(&self) -> u64 {
        self.id
    }

    /// Get start epoch of transaction.
    pub fn get_start_epoch(&self) -> u64 {
        self.start_epoch
    }

    /// Get transaction state.
    pub fn get_state(&self) -> TransactionState {
        self.state.lock().unwrap().clone()
    }

    /// Set transaction state.
    pub fn set_state(&self, new: TransactionState) {
        *self.state.lock().unwrap() = new;
    }

    pub fn try_commit(&self) -> Result<(), NonFatalError> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.clone();
        if state == TransactionState::Aborted {
            return Err(OptimisedHitListError::Hit(self.id.to_string()).into());
        } else {
            *guard = TransactionState::Committed;
        }
        Ok(())
    }

    /// Add predecessor.
    pub fn add_predecessor(&self, pid: String, predecessor_upon: PredecessorUpon) {
        use PredecessorUpon::*;
        unsafe {
            match predecessor_upon {
                Read => {
                    let v = &mut *self.wait_list.get(); // raw mutable pointer
                    v.as_mut().unwrap().push(pid)
                }
                Write => {
                    let v = &mut *self.hit_list.get(); // raw mutable pointer
                    v.as_mut().unwrap().push(pid)
                }
            }
        }
    }

    /// Get predecessors.
    pub fn get_predecessors(&self, predecessor_upon: PredecessorUpon) -> Vec<String> {
        use PredecessorUpon::*;
        unsafe {
            match predecessor_upon {
                Read => {
                    let v = &mut *self.wait_list.get();
                    v.take().unwrap()
                }
                Write => {
                    let v = &mut *self.hit_list.get();
                    v.take().unwrap()
                }
            }
        }
    }

    /// Get the list of keys updated/deleted by this transaction.
    pub fn get_keys_updated(&self) -> Vec<(String, PrimaryKey)> {
        unsafe {
            let v = &mut *self.keys_updated.get();
            v.take().unwrap()
        }
    }

    /// Add key to list of those updated/deleted by this transaction.
    pub fn add_key_updated(&self, key: (String, PrimaryKey)) {
        unsafe {
            let v = &mut *self.keys_updated.get(); // raw mutable pointer
            v.as_mut().unwrap().push(key);
        }
    }

    /// Get the list of keys updated/deleted by this transaction.
    pub fn get_keys_deleted(&self) -> Vec<(String, PrimaryKey)> {
        unsafe {
            let v = &mut *self.keys_deleted.get();
            v.take().unwrap()
        }
    }

    /// Add key to list of those updated/deleted by this transaction.
    pub fn add_key_deleted(&self, key: (String, PrimaryKey)) {
        unsafe {
            let v = &mut *self.keys_deleted.get(); // raw mutable pointer
            v.as_mut().unwrap().push(key);
        }
    }

    /// Get the list of keys read by this transaction.
    pub fn get_keys_read(&self) -> Vec<(String, PrimaryKey)> {
        unsafe {
            let v = &mut *self.keys_read.get();
            v.take().unwrap()
        }
    }

    /// Add key to list of those read by this transaction.
    pub fn add_key_read(&self, key: (String, PrimaryKey)) {
        unsafe {
            let v = &mut *self.keys_read.get(); // raw mutable pointer
            v.as_mut().unwrap().push(key);
        }
    }

    /// Get the list of keys inserted by this transaction.
    pub fn get_keys_inserted(&self) -> Vec<(String, PrimaryKey)> {
        unsafe {
            let v = &mut *self.keys_inserted.get();
            v.take().unwrap()
        }
    }

    /// Add key to list of those read by this transaction.
    pub fn add_key_inserted(&self, key: (String, PrimaryKey)) {
        unsafe {
            let v = &mut *self.keys_inserted.get(); // raw mutable pointer
            v.as_mut().unwrap().push(key);
        }
    }
}
