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
    //    state: TransactionState,
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

    // pub fn update_state(&self, state: TransactionState) {
    //     unsafe {
    //         let s = *self.size.get();
    //         let dat = &mut *self.data.get();
    //         dat[s - 1].state = state;
    //     }
    // }
}

impl Version {
    pub fn new(tid: TransactionId, optype: OperationType, _state: TransactionState) -> Self {
        Self { tid, optype }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({},{})", self.tid, self.optype)
    }
}

impl fmt::Display for VersionHistory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe {
            let dat = &mut *self.data.get();
            // print most 10 recent entries

            let mut comma_separated = String::new();

            let si = dat.len();

            for entry in &dat[si - 10..si - 1] {
                comma_separated.push_str(&entry.to_string());
                comma_separated.push_str(", \n");
            }

            comma_separated.push_str(&dat[si - 1].to_string());
            write!(f, "{}", comma_separated)
        }
    }
}
