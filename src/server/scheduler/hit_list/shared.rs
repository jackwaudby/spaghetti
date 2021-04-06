use crate::server::scheduler::hit_list::epoch::Epoch;

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::debug;

/// Represents the final state of a transaction.
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum TransactionOutcome {
    Aborted,
    Committed,
}

/// Represents an entry in the terminated list.
#[derive(Eq, Debug)]
struct TerminatedEntry {
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

    /// Current epoch of the protocol.
    current_epoch: u64,

    /// List of epochs.
    epochs: Vec<Epoch>,

    /// Alpha.
    alpha: Option<u64>,
}

/// Atomic wrapper around shared resources.
#[derive(Debug)]
pub struct AtomicSharedResources {
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
        let mut epochs = Vec::new();
        let epoch = Epoch::new(0);
        epochs.push(epoch);

        SharedResources {
            hit_list: HashSet::new(),
            terminated_list: HashSet::new(),
            current_epoch: 0,
            epochs,
            alpha: None,
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

    /// Get current epoch.
    pub fn get_current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Start new epoch
    pub fn new_epoch(&mut self) {
        self.current_epoch += 1;
        debug!("Starting epoch {}", self.current_epoch);
        let epoch = Epoch::new(self.current_epoch);
        self.epochs.push(epoch);
    }

    /// Get position of epoch in the list of epochs
    fn get_epoch(&self, id: u64) -> usize {
        self.epochs
            .iter()
            .position(|epoch| epoch.get_epoch_num() == id)
            .unwrap()
    }

    /// Add started transaction to current epoch.
    pub fn add_started(&mut self, id: u64) {
        debug!("Transaction {} started in epoch {}", id, self.current_epoch);
        let ce = self.current_epoch;
        let pos = self.get_epoch(ce);

        self.epochs[pos].add_started(id);
    }

    /// Add terminated transaction to current epoch.
    pub fn add_terminated(&mut self, id: u64, start_epoch: u64) {
        debug!(
            "Add transaction {} to terminated in epoch {}",
            id, self.current_epoch
        );

        // Add to current epoch terminated
        let ce = self.current_epoch;
        let ce_pos = self.get_epoch(ce);
        self.epochs[ce_pos].add_terminated(id);

        // Remove from started epoch
        debug!(
            "Remove transaction {} from started in in epoch {}",
            id, start_epoch
        );
        let se_pos = self.get_epoch(start_epoch);
        self.epochs[se_pos].remove_started(id);
    }

    /// Update alpha
    pub fn update_alpha(&mut self) {
        debug!("Update alpha");
        // Get current epoch.
        let ce = self.current_epoch;

        // Assuming Vec maintains insertion order, epochs are GC'd in this order.
        let ep = self
            .epochs
            .iter()
            .find(|&epoch| !epoch.has_active_transactions())
            .unwrap()
            .get_epoch_num();

        if ep == ce {
            return;
        }

        let pos = self.get_epoch(ep);

        // Remove the epoch.
        let mut epoch = self.epochs.remove(pos);
        // Remove from terminated list
        let to_remove = epoch.get_terminated();

        for id in to_remove {
            let entry = TerminatedEntry { id, outcome: None };
            self.terminated_list.remove(&entry);
        }

        // Set alpha
        self.alpha = Some(epoch.get_epoch_num());
    }
}

impl AtomicSharedResources {
    /// Create new atomic shared resources.
    pub fn new() -> AtomicSharedResources {
        let resources = Arc::new(Mutex::new(SharedResources::new()));
        AtomicSharedResources { resources }
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

impl fmt::Display for SharedResources {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let top = "\n|---------|-----------|---------------|---|\n";
        let head = "|  epoch  |  started  |  terminated  | n |\n";
        let mut join = format!("{}{}", top, head);

        for epoch in &self.epochs {
            join = format!("{}{} \n", join, epoch);
        }
        join = format!("{}|---------|-----------|---------------|---|", join);
        write!(f, "{}", join)
    }
}
