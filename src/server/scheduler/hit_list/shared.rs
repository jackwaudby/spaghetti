use crate::server::scheduler::hit_list::epoch::EpochTracker;

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};

/// Represents the final state of a transaction.
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum TransactionOutcome {
    Aborted,
    Committed,
}

/// Represents an entry in the terminated list.
#[derive(Eq, Debug)]
pub struct TerminatedEntry {
    /// Transaction id.
    id: u64,

    /// Transaction outcome.
    outcome: Option<TransactionOutcome>,
}

/// Represents the global data structures in the protocol.
#[derive(Debug)]
pub struct SharedResources {
    /// List of active transactions that if allowed to commit may result in non-serializable
    /// behaviour.
    hit_list: HashSet<u64>,

    /// List of completed transactions and its outcome (aborted, committed).
    terminated_list: HashSet<TerminatedEntry>,

    /// List of epochs.
    epochs: EpochTracker,
}

/// Atomic wrapper around shared resources.
#[derive(Debug)]
pub struct AtomicSharedResources {
    /// Transaction id counter.
    ///
    /// # Safety
    ///
    /// Arc: shared across threads.
    /// Mutex: single read/writer at-a-time.
    id: Arc<Mutex<u64>>,

    /// Contains hit and terminated lists.
    ///
    /// # Safety
    ///
    /// Arc: shared across threads.
    /// Mutex: single read/writer at-a-time.
    resources: Arc<Mutex<SharedResources>>,
}

impl TerminatedEntry {
    /// Get outcome of terminated transaction.
    fn get_outcome(&self) -> TransactionOutcome {
        self.outcome.as_ref().unwrap().clone()
    }
}

impl SharedResources {
    /// Create new shared resources.
    fn new() -> SharedResources {
        let epochs = EpochTracker::new();

        SharedResources {
            hit_list: HashSet::new(),
            terminated_list: HashSet::new(),
            epochs,
        }
    }

    /// Add transaction to hit list
    pub fn add_to_hit_list(&mut self, id: u64) {
        self.hit_list.insert(id);
    }

    /// Returns `true` if transaction with `id` exists in the terminated list.
    pub fn has_terminated(&self, id: u64) -> bool {
        let pred = TerminatedEntry { id, outcome: None };
        self.terminated_list.contains(&pred)
    }

    /// Get outcome of terminated transaction.
    pub fn get_terminated_outcome(&self, id: u64) -> TransactionOutcome {
        let pred = TerminatedEntry { id, outcome: None };
        self.terminated_list.get(&pred).unwrap().get_outcome()
    }

    /// Remove transaction `id` from hit list.
    pub fn remove_from_hit_list(&mut self, id: u64) {
        self.hit_list.remove(&id);
    }

    /// Returns `true` if transaction is in the hit list.
    pub fn is_in_hit_list(&self, id: u64) -> bool {
        self.hit_list.contains(&id)
    }

    /// Add transaction to terminated list.
    pub fn add_to_terminated_list(&mut self, id: u64, outcome: TransactionOutcome) {
        let entry = TerminatedEntry {
            id,
            outcome: Some(outcome),
        };
        self.terminated_list.insert(entry);
    }

    /// Remove transaction from terminated list.
    pub fn remove_from_terminated_list(&mut self, id: u64) {
        let entry = TerminatedEntry { id, outcome: None };
        self.terminated_list.remove(&entry);
    }

    /// Get mutable reference to the epoch tracker.
    pub fn get_mut_epoch_tracker(&mut self) -> &mut EpochTracker {
        &mut self.epochs
    }

    // pub fn get_ref_epoch_tracker(&mut self) -> &EpochTracker {
    //     &self.epochs
    // }
}

impl AtomicSharedResources {
    /// Create new atomic shared resources.
    pub fn new() -> AtomicSharedResources {
        let id = Arc::new(Mutex::new(0)); // id generator
        let resources = Arc::new(Mutex::new(SharedResources::new()));
        AtomicSharedResources { id, resources }
    }

    /// Get next id
    pub fn get_next_id(&self) -> u64 {
        let mut lock = self.id.lock().unwrap(); // get mutex lock
        let id = *lock; // get copy of id
        *lock += 1; // increment
        id
    }

    /// Get lock on shared resources.
    pub fn get_lock(&self) -> MutexGuard<SharedResources> {
        self.resources.lock().unwrap()
    }

    /// Get handle to shared resources.
    pub fn get_ref(&self) -> Arc<Mutex<SharedResources>> {
        Arc::clone(&self.resources)
    }
}

impl PartialEq for TerminatedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for TerminatedEntry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.id.hash(hasher);
    }
}
