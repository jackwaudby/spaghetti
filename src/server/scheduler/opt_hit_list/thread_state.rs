use crate::server::scheduler::opt_hit_list::epoch::EpochTracker;
use crate::server::scheduler::opt_hit_list::transaction::{
    PredecessorUpon, Transaction, TransactionState,
};
use crate::server::scheduler::NonFatalError;
use crate::workloads::PrimaryKey;

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

/// Per-thread state.
#[derive(Debug)]
pub struct ThreadState {
    /// Epoch tracker.
    epoch_tracker: Mutex<EpochTracker>,

    /// Transaction id generator.
    seq_num: u64,

    /// Termination list.
    terminated_list: RwLock<Vec<Arc<Transaction>>>,
}

impl ThreadState {
    /// Create new thread state.
    pub fn new() -> ThreadState {
        ThreadState {
            epoch_tracker: Mutex::new(EpochTracker::new()),
            seq_num: 0,
            terminated_list: RwLock::new(vec![]),
        }
    }

    /// Get index in list of transaction with `id`.
    pub fn get_index(&self, id: u64) -> usize {
        self.terminated_list
            .read()
            .unwrap()
            .iter()
            .position(|x| x.get_id() == id)
            .unwrap()
    }

    /// Increment epoch.
    pub fn get_epoch_tracker(&self) -> MutexGuard<EpochTracker> {
        self.epoch_tracker.lock().unwrap()
    }

    /// Get start epoch of transaction.
    pub fn get_start_epoch(&self, id: u64) -> u64 {
        let index = self.get_index(id);
        self.terminated_list.read().unwrap()[index].get_start_epoch()
    }

    /// Get transaction id.
    fn get_id(&mut self) -> u64 {
        let id = self.seq_num;
        self.seq_num += 1;
        id
    }

    /// Register new transaction with thread.
    pub fn new_transaction(&mut self) -> u64 {
        let id = self.get_id(); // get id
        let mut wg = self.get_epoch_tracker();
        let se = wg.get_current_id(); // start epoch
        wg.add_started(id); // add to gc

        let transaction = Arc::new(Transaction::new(id, se)); // entry in TL
        self.terminated_list.write().unwrap().push(transaction);

        id
    }

    /// Add predecessor for transaction with `id`.
    pub fn add_predecessor(
        &self,
        id: u64,
        predecessor_id: String,
        predecessor_upon: PredecessorUpon,
    ) {
        let index = self.get_index(id);
        self.terminated_list.read().unwrap()[index]
            .add_predecessor(predecessor_id, predecessor_upon);
    }

    /// Get wait list for transaction `id`.
    pub fn get_wait_list(&self, id: u64) -> Vec<String> {
        let index = self.get_index(id);
        self.terminated_list.read().unwrap()[index].get_predecessors(PredecessorUpon::Read)
    }

    /// Get hit list for transaction `id`.
    pub fn get_hit_list(&self, id: u64) -> Vec<String> {
        let index = self.get_index(id);
        self.terminated_list.read().unwrap()[index].get_predecessors(PredecessorUpon::Write)
    }

    /// Set state for transaction `id`.
    pub fn set_state(&self, id: u64, state: TransactionState) {
        let index = self.get_index(id);
        self.terminated_list.read().unwrap()[index].set_state(state);
    }

    /// Set state for transaction `id`.
    pub fn try_commit(&self, id: u64) -> Result<(), NonFatalError> {
        let index = self.get_index(id);
        let tl: &Vec<Arc<Transaction>> = &*self.terminated_list.read().unwrap();
        let transaction = &*tl[index];

        transaction.try_commit()?;

        Ok(())
    }

    /// Get state for transaction `id`.
    pub fn get_state(&self, id: u64) -> TransactionState {
        let index = self.get_index(id);
        self.terminated_list.read().unwrap()[index].get_state()
    }

    /// Remove transaction with `id`.
    pub fn remove_transaction(&self, id: u64) {
        let index = self.get_index(id);
        self.terminated_list.write().unwrap().remove(index);
    }

    /// Add key to transaction.
    pub fn add_key(&self, tid: u64, key: (String, PrimaryKey), operation: Operation) {
        let index = self.get_index(tid);

        let tl: &Vec<Arc<Transaction>> = &*self.terminated_list.read().unwrap();
        let entry = &*tl[index];

        use Operation::*;
        match operation {
            Create => entry.add_key_inserted(key),
            Read => entry.add_key_read(key),
            Update => entry.add_key_updated(key),
            Delete => entry.add_key_deleted(key),
        }
    }

    /// Get key to transaction.
    pub fn get_keys(&self, tid: u64, operation: Operation) -> Vec<(String, PrimaryKey)> {
        let index = self.get_index(tid);
        let tl: &Vec<Arc<Transaction>> = &*self.terminated_list.read().unwrap();
        let entry = &*tl[index];

        use Operation::*;
        match operation {
            Create => entry.get_keys_inserted(),
            Read => entry.get_keys_read(),
            Update => entry.get_keys_updated(),
            Delete => entry.get_keys_deleted(),
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
        let tl = self.terminated_list.read().unwrap().len();

        write!(
            f,
            "Thread state\nEpoch tracker:\n{}Terminated list:{}",
            ep, tl
        )
    }
}
