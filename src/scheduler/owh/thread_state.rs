use crate::scheduler::owh::epoch::EpochTracker;
use crate::scheduler::owh::terminated_list::TerminatedList;
use crate::scheduler::owh::transaction::{
    Operation, OperationType, PredecessorUpon, Transaction, TransactionState,
};
use crate::scheduler::NonFatalError;
use crate::workloads::PrimaryKey;

use parking_lot::{Mutex, MutexGuard, RwLock};
use std::cell::UnsafeCell;
use std::fmt;
use std::sync::Arc;

unsafe impl Sync for ThreadState {}

#[derive(Debug)]
pub struct ThreadState {
    epoch_tracker: Mutex<EpochTracker>,
    seq_num: UnsafeCell<usize>,
    terminated_list: RwLock<TerminatedList>,
    gc: bool,
}

impl ThreadState {
    pub fn new(gc: bool) -> ThreadState {
        ThreadState {
            epoch_tracker: Mutex::new(EpochTracker::new()),
            seq_num: UnsafeCell::new(0),
            terminated_list: RwLock::new(TerminatedList::new()),
            gc,
        }
    }

    pub fn get_epoch_tracker(&self) -> MutexGuard<EpochTracker> {
        self.epoch_tracker.lock()
    }

    fn get_id(&self) -> usize {
        // Safety: this method is only called by the same thread, from a single place in the code.
        unsafe {
            let id = *self.seq_num.get(); // cast raw pointer to mutable reference
            *self.seq_num.get() = id + 1; // increment
            id
        }
    }

    pub fn new_transaction(&self) -> usize {
        let id = self.get_id(); // get id
        let mut wg = self.get_epoch_tracker(); // lock epoch tracker
        let se = wg.get_current_id(); // get start epoch for transaction
        wg.add_started(id); // register as started in this epoch
        let transaction = Arc::new(Transaction::new(id, se)); // create new list entry
        self.terminated_list.write().list.push(transaction);
        id as usize
    }

    pub fn get_start_epoch(&self, seq_num: usize) -> u64 {
        let rlock = self.terminated_list.read(); // take read lock on terminated list
        let index = rlock.get_index(seq_num); // calculate offset in list
        rlock.list[index].get_start_epoch() // get start epoch
    }

    pub fn get_transaction_id(&self, seq_num: usize) -> usize {
        let rlock = self.terminated_list.read(); // take read lock on terminated list
        let index = rlock.get_index(seq_num); // calculate offset in list
        rlock.list[index].get_id()
    }

    pub fn add_predecessor(
        &self,
        seq_num: usize,
        predecessor_id: (usize, usize),
        predecessor_upon: PredecessorUpon,
    ) {
        let rlock = self.terminated_list.read(); // take read lock on terminated list
        let index = rlock.get_index(seq_num); // calculate offset in list
        rlock.list[index].add_predecessor(predecessor_id, predecessor_upon);
    }

    pub fn get_wait_list(&self, seq_num: usize) -> Vec<(usize, usize)> {
        let rlock = self.terminated_list.read(); // take read lock on terminated list
        let index = rlock.get_index(seq_num); // calculate offset in list
        rlock.list[index].get_predecessors(PredecessorUpon::Read)
    }

    /// Get hit list for transaction `id`.
    pub fn get_hit_list(&self, seq_num: usize) -> Vec<(usize, usize)> {
        let rlock = self.terminated_list.read(); // take read lock on terminated list
        let index = rlock.get_index(seq_num); // calculate offset in list
        rlock.list[index].get_predecessors(PredecessorUpon::Write)
    }

    /// Set state for transaction `id`.
    pub fn set_state(&self, seq_num: usize, state: TransactionState) {
        let rlock = self.terminated_list.read(); // take read lock on terminated list
        let index = rlock.get_index(seq_num); // calculate offset in list
        rlock.list[index].set_state(state);
    }

    /// Set state for transaction `id`.
    pub fn try_commit(&self, seq_num: usize) -> Result<(), NonFatalError> {
        let rlock: &TerminatedList = &*self.terminated_list.read(); // get read lock
        let index = rlock.get_index(seq_num); // calculate offset in list
        let entry = &*rlock.list[index]; // get entry
        entry.try_commit()?;
        Ok(())
    }

    /// Get state for transaction `id`.
    pub fn get_state(&self, seq_num: usize) -> TransactionState {
        let rlock = self.terminated_list.read(); // take read lock on terminated list
        let index = rlock.get_index(seq_num); // calculate offset in list
        rlock.list[index].get_state()
    }

    pub fn add_key(
        &self,
        seq_num: usize,
        op_type: OperationType,
        key: PrimaryKey,
        index_id: usize,
    ) {
        let rlock: &TerminatedList = &*self.terminated_list.read(); // get read lock
        let index = rlock.get_index(seq_num); // calculate offset in list
        let entry = &*rlock.list[index]; // get entry

        entry.add_info(op_type, key, index_id);
    }

    pub fn get_info(&self, seq_num: usize) -> Vec<Operation> {
        let rlock: &TerminatedList = &*self.terminated_list.read(); // get read lock
        let index = rlock.get_index(seq_num); // calculate offset in list
        let entry = &*rlock.list[index]; // get entry

        entry.get_info()
    }

    /// Remove transaction with `id`.
    pub fn remove_transactions(&self, transactions: Vec<usize>) {
        if !transactions.is_empty() {
            let mut wlock = self.terminated_list.write(); // take write lock on terminated list

            let num_to_remove = transactions.len();
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

impl fmt::Display for ThreadState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ep = self.epoch_tracker.lock();
        let tl = self.terminated_list.read().list.len();

        write!(
            f,
            "Thread state\nEpoch tracker:\n{}Terminated list:{}",
            ep, tl
        )
    }
}
