use crate::server::scheduler::opt_hit_list::transaction::Transaction;

use std::sync::Arc;

#[derive(Debug)]
pub struct TerminatedList {
    pub list: Vec<Arc<Transaction>>,
    pub removed: u64,
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
    pub fn get_index(&self, id: u64) -> usize {
        if self.removed == 0 {
            id as usize
        } else {
            assert!(
                id >= self.removed,
                "id {} should be larger than removed {}",
                id,
                self.removed
            );

            let offset = id - self.removed;
            offset as usize
        }
    }
}
