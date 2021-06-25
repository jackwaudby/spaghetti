use std::slice::Iter;
use std::sync::{Arc, Condvar, Mutex};

/// Represents the locking information for a given database record.
#[derive(Debug)]
pub struct LockInfo {
    /// Overall lock state.
    /// Prevents ransactions needing to traverse all `Entry`s to determine lock status.
    group_mode: Option<LockMode>,

    /// Whether transactions are waiting for the lock.
    waiting: bool,

    /// List of transactions that have acquired the lock or are waiting for it.
    list: Vec<Entry>,

    /// Latest timestamp of transactions that hold lock, used for deadlock detection.
    timestamp: Option<u64>,

    /// Number of locks concurrently granted for this record.
    granted: Option<u32>,
}

/// Represents an entry in the list of transactions that have requested a lock.
///
/// These transactions can either be waiting for the lock or be holding it.
/// For requests that are waiting for the lock the calling thread is blocked using a `Condvar`.
/// When a transaction releases the lock it notifies the thread to resume execution.
#[derive(Debug)]
pub struct Entry {
    lock_mode: LockMode,
    waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
    timestamp: u64,
}

/// Represents the different lock modes.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum LockMode {
    Read,
    Write,
}

impl LockInfo {
    pub fn new(group_mode: LockMode, timestamp: u64) -> Self {
        Self {
            group_mode: Some(group_mode),
            waiting: false,
            list: Vec::new(),
            timestamp: Some(timestamp),
            granted: Some(1),
        }
    }

    pub fn remove_entry(&mut self, i: usize) -> Entry {
        self.list.remove(i)
    }

    pub fn is_free(&self) -> bool {
        self.granted.is_some()
    }

    pub fn get_iter(&self) -> Iter<Entry> {
        self.list.iter()
    }

    pub fn get_list(&self) -> &Vec<Entry> {
        &self.list
    }

    pub fn get_mut_list(&mut self) -> &mut Vec<Entry> {
        &mut self.list
    }

    pub fn set_mode(&mut self, mode: LockMode) {
        self.group_mode = Some(mode);
    }

    pub fn get_mode(&self) -> LockMode {
        self.group_mode.unwrap().clone()
    }

    pub fn is_waiting(&self) -> bool {
        self.waiting
    }

    pub fn set_waiting(&mut self, val: bool) {
        self.waiting = val;
    }

    pub fn num_waiting(&self) -> usize {
        self.list.len()
    }

    pub fn inc_granted(&mut self) {
        let held = self.granted.unwrap() + 1;
        self.granted = Some(held);
    }

    pub fn dec_granted(&mut self) {
        let held = self.granted.unwrap() - 1;
        self.granted = Some(held);
    }

    pub fn set_granted(&mut self, val: u32) {
        self.granted = Some(val);
    }

    pub fn get_granted(&self) -> u32 {
        self.granted.unwrap()
    }

    pub fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = Some(timestamp);
    }

    pub fn get_timestamp(&mut self) -> u64 {
        self.timestamp.unwrap()
    }

    pub fn add_entry(&mut self, entry: Entry) {
        self.list.push(entry);
    }

    pub fn reset(&mut self) {
        self.group_mode = None; // reset group mode
        self.list.clear(); // remove from list
        self.granted = Some(0); // set granted to 0
        self.timestamp = None; // reset timestamp
    }
}

impl Entry {
    pub fn new(
        lock_mode: LockMode,
        waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
        timestamp: u64,
    ) -> Entry {
        Entry {
            lock_mode,
            waiting,
            timestamp,
        }
    }

    pub fn get_cond(&self) -> Arc<(Mutex<bool>, Condvar)> {
        Arc::clone(self.waiting.as_ref().unwrap())
    }

    pub fn get_mode(&self) -> LockMode {
        self.lock_mode.clone()
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}
