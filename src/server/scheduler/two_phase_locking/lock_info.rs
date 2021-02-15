use chrono::{DateTime, Utc};
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};

/// Represents the locking information for a given database record.
#[derive(Debug)]
pub struct LockInfo {
    /// Overall lock state.
    /// Prevents ransactions needing to traverse all `Entry`s to determine lock status.
    pub group_mode: Option<LockMode>,
    /// Whether transactions are waiting for the lock.
    pub waiting: bool,
    /// List of transactions that have acquired the lock or are waiting for it.
    pub list: Vec<Entry>,
    /// Latest timestamp of transactions that hold lock, used for deadlock detection.
    pub timestamp: Option<DateTime<Utc>>,
    /// Number of locks concurrently granted for this record.
    pub granted: Option<u32>,
    /// Name of index row resides in
    pub index: String,
}

/// Represents an entry in the list of transactions that have requested a lock.
///
/// These transactions can either be waiting for the lock or be holding it.
/// For requests that are waiting for the lock the calling thread is blocked using a `Condvar`.
/// When a transaction releases the lock it notifies the thread to resume execution.
#[derive(Debug)]
pub struct Entry {
    /// Transaction name.
    pub name: String,
    /// Lock request type.
    pub lock_mode: LockMode,
    /// Waiting for the lock or holding it.
    pub waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
    // TODO: Pointer to transaction's other `Entry`s for unlocking.
    // previous_entry: &Entry,
    /// Transaction timestamp.
    pub timestamp: DateTime<Utc>,
}

/// Represents the different lock modes.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum LockMode {
    Read,
    Write,
}

impl LockInfo {
    /// Create new locking information container.
    ///
    /// Takes the initial group mode and initial timestamp.
    pub fn new(group_mode: LockMode, timestamp: DateTime<Utc>, index: &str) -> LockInfo {
        LockInfo {
            group_mode: Some(group_mode),
            waiting: false,
            list: Vec::new(),
            timestamp: Some(timestamp),
            granted: Some(1),
            index: index.to_string(),
        }
    }

    /// Add an `Entry` to the lock information.
    pub fn add_entry(&mut self, entry: Entry) {
        self.list.push(entry);
    }
}

impl fmt::Display for LockInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\n Group mode: {:?}\n Waiting transactions: {}\n Requests: {:?}\n timestamp: {:?}\n granted: {:?}",
            self.group_mode, self.waiting,self.list,self.timestamp, self.granted
        )
    }
}

impl Entry {
    // Create new `Entry`.
    pub fn new(
        name: String,
        lock_mode: LockMode,
        waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
        timestamp: DateTime<Utc>,
    ) -> Entry {
        Entry {
            name,
            lock_mode,
            waiting,
            timestamp,
        }
    }
}
