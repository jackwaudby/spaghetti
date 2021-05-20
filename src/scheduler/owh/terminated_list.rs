use crate::scheduler::owh::transaction::Transaction;

use std::sync::Arc;

#[derive(Debug)]
pub struct TerminatedList {
    pub list: Vec<Arc<Transaction>>,
    pub removed: usize,
}

impl TerminatedList {
    /// Create new terminated list.
    pub fn new() -> Self {
        TerminatedList {
            list: vec![],
            removed: 0,
        }
    }

    /// Get position in the terminated list of transaction with `id`.
    pub fn get_index(&self, seq_num: usize) -> usize {
        if self.removed == 0 {
            seq_num as usize
        } else {
            assert!(
                seq_num >= self.removed,
                "seq_num {} should be larger than removed {}",
                seq_num,
                self.removed
            );

            let offset = seq_num - self.removed;
            offset as usize
        }
    }
}
