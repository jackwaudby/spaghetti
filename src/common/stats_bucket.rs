use crate::storage::access::TransactionId;

use std::collections::HashSet;

pub struct StatsBucket {
    tid: TransactionId,
    problem_transactions: HashSet<TransactionId>,
    abort_through: usize,
}

impl StatsBucket {
    pub fn new(tid: TransactionId) -> Self {
        Self {
            tid,
            problem_transactions: HashSet::new(),
            abort_through: 0,
        }
    }

    pub fn get_transaction_id(&self) -> TransactionId {
        self.tid.clone()
    }

    pub fn get_abort_through(&self) -> usize {
        self.abort_through
    }

    pub fn set_abort_through(&mut self, x: usize) {
        self.abort_through = x;
    }

    pub fn get_problem_transactions(&self) -> HashSet<TransactionId> {
        self.problem_transactions.clone()
    }

    pub fn add_problem_transaction(&mut self, id: TransactionId) {
        self.problem_transactions.insert(id);
    }
}
