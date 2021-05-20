use crate::scheduler::owh::error::OptimisedWaitHitError;
use crate::scheduler::NonFatalError;
use crate::workloads::PrimaryKey;

use parking_lot::Mutex;
use std::cell::UnsafeCell;

unsafe impl Sync for Transaction {}

#[derive(Debug)]
pub struct Transaction {
    id: usize,
    start_epoch: u64,
    state: Mutex<TransactionState>,
    wait_list: UnsafeCell<Option<Vec<(usize, usize)>>>,
    hit_list: UnsafeCell<Option<Vec<(usize, usize)>>>,
    info: UnsafeCell<TransactionInformation>,
}

#[derive(Debug)]
pub struct TransactionInformation {
    operations: Option<Vec<Operation>>,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub op_type: OperationType,
    pub key: PrimaryKey,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub enum OperationType {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Aborted,
    Committed,
}

#[derive(Debug)]
pub enum PredecessorUpon {
    Read,
    Write,
}

impl Transaction {
    pub fn new(id: usize, start_epoch: u64) -> Transaction {
        Transaction {
            id,
            start_epoch,
            state: Mutex::new(TransactionState::Active),
            wait_list: UnsafeCell::new(Some(vec![])),
            hit_list: UnsafeCell::new(Some(vec![])),
            info: UnsafeCell::new(TransactionInformation::new()),
        }
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub fn get_start_epoch(&self) -> u64 {
        self.start_epoch
    }

    pub fn get_state(&self) -> TransactionState {
        self.state.lock().clone()
    }

    pub fn set_state(&self, new: TransactionState) {
        *self.state.lock() = new;
    }

    pub fn try_commit(&self) -> Result<(), NonFatalError> {
        let mut guard = self.state.lock();
        let state = guard.clone();
        if state == TransactionState::Aborted {
            return Err(OptimisedWaitHitError::Hit(self.id.to_string()).into());
        } else {
            *guard = TransactionState::Committed;
        }
        Ok(())
    }

    pub fn add_predecessor(&self, id: (usize, usize), predecessor_upon: PredecessorUpon) {
        use PredecessorUpon::*;

        unsafe {
            match predecessor_upon {
                Read => {
                    let v = &mut *self.wait_list.get();
                    v.as_mut().unwrap().push(id)
                }
                Write => {
                    let v = &mut *self.hit_list.get();
                    v.as_mut().unwrap().push(id)
                }
            }
        }
    }

    pub fn get_predecessors(&self, predecessor_upon: PredecessorUpon) -> Vec<(usize, usize)> {
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

    pub fn get_info(&self) -> Vec<Operation> {
        unsafe {
            let v = &mut *self.info.get();
            v.get()
        }
    }

    pub fn add_info(&self, op_type: OperationType, key: PrimaryKey, index: usize) {
        unsafe {
            let v = &mut *self.info.get();
            v.add(op_type, key, index);
        }
    }
}

impl TransactionInformation {
    pub fn new() -> Self {
        TransactionInformation {
            operations: Some(Vec::with_capacity(8)),
        }
    }

    pub fn add(&mut self, op_type: OperationType, key: PrimaryKey, index: usize) {
        self.operations
            .as_mut()
            .unwrap()
            .push(Operation::new(op_type, key, index));
    }

    pub fn get(&mut self) -> Vec<Operation> {
        self.operations.take().unwrap()
    }
}

impl Operation {
    pub fn new(op_type: OperationType, key: PrimaryKey, index: usize) -> Self {
        Operation {
            op_type,
            key,
            index,
        }
    }
}
