use crate::common::transaction_information::OperationType;
use crate::storage::access::TransactionId;

use std::cell::UnsafeCell;
use std::fmt;

unsafe impl Sync for VersionHistory {}

#[derive(Debug)]
pub struct VersionHistory {
    data: UnsafeCell<Vec<Version>>,
    size: UnsafeCell<usize>,
}

#[derive(Debug)]
pub struct Version {
    tid: TransactionId,
    optype: OperationType,
    state: TransactionState,
}

#[derive(Debug)]
pub enum TransactionState {
    Aborted,

    Committed,
    Active,
}

impl VersionHistory {
    pub fn new() -> Self {
        VersionHistory {
            data: UnsafeCell::new(Vec::new()),
            size: UnsafeCell::new(0),
        }
    }

    pub fn add_version(&self, tid: TransactionId, optype: OperationType, state: TransactionState) {
        unsafe {
            let dat = &mut *self.data.get();
            dat.push(Version::new(tid, optype, state));
            let s = &mut *self.size.get();
            *s += 1;
        }
    }

    pub fn update_state(&self, tid: TransactionId, state: TransactionState) {
        unsafe {
            let dat = &mut *self.data.get();

            let index = dat.iter().position(|r| r.tid == tid).unwrap();

            dat[index].state = state;

            //            dat[s - 1].state = state;
        }
    }
}

impl Version {
    pub fn new(tid: TransactionId, optype: OperationType, state: TransactionState) -> Self {
        Self { tid, optype, state }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "(id:{},type:{},tstate:{})",
            self.tid, self.optype, self.state
        )
    }
}

impl fmt::Display for VersionHistory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe {
            let dat = &mut *self.data.get();
            // print most 5 recent entries

            let mut comma_separated = String::new();

            let si = dat.len();

            comma_separated.push_str("\n");
            comma_separated.push_str("version history:");
            comma_separated.push_str("\n");

            for (i, entry) in dat[0..si].iter().rev().enumerate() {
                comma_separated.push_str(&format!("{}: {}", i, &entry.to_string()));
                comma_separated.push_str("\n");
            }

            write!(f, "{}", comma_separated)
        }
    }
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionState::*;
        match &self {
            Aborted => write!(f, "aborted"),
            Committed => write!(f, "committed"),
            Active => write!(f, "active"),
        }
    }
}
