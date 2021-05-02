use crate::server::scheduler::opt_hit_list::epoch::EpochTracker;
use crate::server::scheduler::opt_hit_list::terminated_list::TerminatedList;
use crate::server::scheduler::opt_hit_list::transaction::{
    PredecessorUpon, Transaction, TransactionState,
};
use crate::server::scheduler::NonFatalError;
use crate::workloads::PrimaryKey;

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

unsafe impl Sync for ThreadState {}

/// Per-thread state.
#[derive(Debug)]
pub struct ThreadState {
    /// Epoch tracker.
    epoch_tracker: Mutex<EpochTracker>,

    /// Transaction id generator.
    seq_num: UnsafeCell<u64>,

    /// Termination list.
    terminated_list: RwLock<TerminatedList>,

    /// Garbage collection.
    gc: bool,
}

impl ThreadState {
    /// Create new thread state.
    pub fn new(gc: bool) -> ThreadState {
        ThreadState {
            epoch_tracker: Mutex::new(EpochTracker::new()),
            seq_num: UnsafeCell::new(0),
            terminated_list: RwLock::new(TerminatedList::new()),
            gc,
        }
    }

    /// Returns the mutexguard around the epoch tracker.
    pub fn get_epoch_tracker(&self) -> MutexGuard<EpochTracker> {
        self.epoch_tracker.lock().unwrap()
    }

    /// Get a new transaction id.
    fn get_id(&self) -> u64 {
        // Safety: this method is only called by the same thread, from a single place in the code.
        unsafe {
            let id = *self.seq_num.get(); // cast raw pointer to mutable reference
            *self.seq_num.get() = id + 1; // increment
            id
        }
    }

    /// Register new transaction with thread.
    pub fn new_transaction(&self) -> u64 {
        let id = self.get_id(); // get id
        let mut wg = self.get_epoch_tracker(); // lock epoch tracker
        let se = wg.get_current_id(); // get start epoch for transaction
        wg.add_started(id); // register as started in this epoch
        let transaction = Arc::new(Transaction::new(id, se)); // create new list entry
        self.terminated_list.write().unwrap().list.push(transaction);
        id
    }

    /// Get start epoch of transaction.
    pub fn get_start_epoch(&self, id: u64) -> u64 {
        let rlock = self.terminated_list.read().unwrap(); // take read lock on terminated list
        let index = rlock.get_index(id); // calculate offset in list
        rlock.list[index].get_start_epoch() // get start epoch
    }

    pub fn get_transaction_id(&self, id: u64) -> u64 {
        let rlock = self.terminated_list.read().unwrap(); // take read lock on terminated list
        let index = rlock.get_index(id); // calculate offset in list
        rlock.list[index].get_id()
    }

    /// Add predecessor for transaction with `id`.
    pub fn add_predecessor(
        &self,
        id: u64,
        predecessor_id: String,
        predecessor_upon: PredecessorUpon,
    ) {
        let rlock = self.terminated_list.read().unwrap(); // take read lock on terminated list
        let index = rlock.get_index(id); // calculate offset in list
        rlock.list[index].add_predecessor(predecessor_id, predecessor_upon);
    }

    /// Get wait list for transaction `id`.
    pub fn get_wait_list(&self, id: u64) -> Vec<String> {
        let rlock = self.terminated_list.read().unwrap(); // take read lock on terminated list
        let index = rlock.get_index(id); // calculate offset in list
        rlock.list[index].get_predecessors(PredecessorUpon::Read)
    }

    /// Get hit list for transaction `id`.
    pub fn get_hit_list(&self, id: u64) -> Vec<String> {
        let rlock = self.terminated_list.read().unwrap(); // take read lock on terminated list
        let index = rlock.get_index(id); // calculate offset in list
        rlock.list[index].get_predecessors(PredecessorUpon::Write)
    }

    /// Set state for transaction `id`.
    pub fn set_state(&self, id: u64, state: TransactionState) {
        let rlock = self.terminated_list.read().unwrap(); // take read lock on terminated list
        let index = rlock.get_index(id); // calculate offset in list
        rlock.list[index].set_state(state);
    }

    /// Set state for transaction `id`.
    pub fn try_commit(&self, id: u64) -> Result<(), NonFatalError> {
        let rlock: &TerminatedList = &*self.terminated_list.read().unwrap(); // get read lock
        let index = rlock.get_index(id); // calculate offset in list
        let entry = &*rlock.list[index]; // get entry
        entry.try_commit()?;
        Ok(())
    }

    /// Get state for transaction `id`.
    pub fn get_state(&self, id: u64) -> TransactionState {
        let rlock = self.terminated_list.read().unwrap(); // take read lock on terminated list
        let index = rlock.get_index(id); // calculate offset in list
        rlock.list[index].get_state()
    }

    /// Add key to transaction.
    pub fn add_key(&self, id: u64, key: (String, PrimaryKey), operation: Operation) {
        let rlock: &TerminatedList = &*self.terminated_list.read().unwrap(); // get read lock
        let index = rlock.get_index(id); // calculate offset in list
        let entry = &*rlock.list[index]; // get entry

        use Operation::*;
        match operation {
            Create => entry.add_key_inserted(key),
            Read => entry.add_key_read(key),
            Update => entry.add_key_updated(key),
            Delete => entry.add_key_deleted(key),
        }
    }

    /// Get key to transaction.
    pub fn get_keys(&self, id: u64, operation: Operation) -> Vec<(String, PrimaryKey)> {
        let rlock: &TerminatedList = &*self.terminated_list.read().unwrap(); // get read lock
        let index = rlock.get_index(id); // calculate offset in list
        let entry = &*rlock.list[index]; // get entry

        use Operation::*;
        match operation {
            Create => entry.get_keys_inserted(),
            Read => entry.get_keys_read(),
            Update => entry.get_keys_updated(),
            Delete => entry.get_keys_deleted(),
        }
    }

    /// Remove transaction with `id`.
    pub fn remove_transactions(&self, transactions: Vec<u64>) {
        if !transactions.is_empty() {
            let mut wlock = self.terminated_list.write().unwrap(); // take write lock on terminated list

            let num_to_remove = transactions.len() as u64;
            let max_id = *transactions.iter().max().unwrap();

            let min_id = *transactions.iter().min().unwrap();
            assert!(
                (max_id + 1 - num_to_remove) == min_id,
                "max_id {}, min_id {}, num_to_remove {}",
                max_id,
                min_id,
                num_to_remove
            );

            let removed = wlock.removed;
            let max_offset = (max_id - removed) as usize;
            let min_offset = (min_id - removed) as usize;

            wlock.list.drain(min_offset..=max_offset);
            wlock.removed += num_to_remove;
        }
    }
}

pub enum Operation {
    Create,
    Read,
    Update,
    Delete,
}

impl fmt::Display for ThreadState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ep = self.epoch_tracker.lock().unwrap();
        let tl = self.terminated_list.read().unwrap().list.len();

        write!(
            f,
            "Thread state\nEpoch tracker:\n{}Terminated list:{}",
            ep, tl
        )
    }
}
