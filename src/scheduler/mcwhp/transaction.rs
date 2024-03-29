use crate::common::error::{NonFatalError, WaitHitError};

use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use std::cell::UnsafeCell;
use std::fmt;
use std::hash::{Hash, Hasher};

pub fn from_usize<'a>(address: usize) -> &'a Transaction<'a> {
    // Safety: finding an address in some access history implies the corresponding transaction is either:
    // (i) pinned on another thread, so it is save to give out reference to it.
    // (ii) scheduled for deletion by another thread, again we can safely give out a reference, as it won't be destroyed
    // until after this thread is unpinned.
    unsafe { &*(address as *const Transaction<'a>) }
}

pub fn to_usize<'a>(txn: Box<Transaction<'a>>) -> usize {
    let raw: *mut Transaction = Box::into_raw(txn);
    raw as usize
}

pub fn to_box<'a>(address: usize) -> Box<Transaction<'a>> {
    // Safety: a node is owned by a single thread, so this method is only called once in order to pass the node to the
    // epoch based garbage collector.
    unsafe {
        let raw = address as *mut Transaction<'a>;
        Box::from_raw(raw)
    }
}

pub fn ref_to_usize<'a>(txn: &'a Transaction<'a>) -> usize {
    let ptr: *const Transaction<'a> = txn;
    ptr as usize
}

pub type PredecessorSet<'a> = FxHashSet<&'a Transaction<'a>>;

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Aborted,
    Committed,
}

#[derive(Debug)]
pub struct Transaction<'a> {
    state: Mutex<TransactionState>,

    // Predecessors found by reading entered here.
    wait_list: UnsafeCell<Option<PredecessorSet<'a>>>,

    // Predecessors found by writing entered here.
    hit_list: UnsafeCell<Option<PredecessorSet<'a>>>,
}

#[derive(Debug)]
pub enum PredecessorUpon {
    Read,
    Write,
}

unsafe impl<'a> Send for Transaction<'a> {}
unsafe impl<'a> Sync for Transaction<'a> {}

impl<'a> Transaction<'a> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TransactionState::Active),
            wait_list: UnsafeCell::new(Some(FxHashSet::default())),
            hit_list: UnsafeCell::new(Some(FxHashSet::default())),
        }
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
            return Err(WaitHitError::Hit.into());
        } else {
            *guard = TransactionState::Committed;
        }
        Ok(())
    }

    pub fn add_predecessor(
        &self,
        predecessor: &'a Transaction<'a>,
        predecessor_upon: PredecessorUpon,
    ) {
        use PredecessorUpon::*;

        unsafe {
            match predecessor_upon {
                Read => {
                    let v = &mut *self.wait_list.get();
                    v.as_mut().unwrap().insert(predecessor);
                }
                Write => {
                    let v = &mut *self.hit_list.get();
                    v.as_mut().unwrap().insert(predecessor);
                }
            }
        }
    }

    pub fn get_predecessors(&self, predecessor_upon: PredecessorUpon) -> PredecessorSet<'a> {
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
}

impl<'a> PartialEq for &'a Transaction<'a> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl<'a> Eq for &'a Transaction<'a> {}

impl<'a> Hash for &'a Transaction<'a> {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        let id = ref_to_usize(self);
        id.hash(hasher)
    }
}

impl<'a> fmt::Display for Transaction<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TXN").unwrap();
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        // println!("DROPPED");
    }
}
