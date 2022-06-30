use crate::storage::access::TransactionId;

use std::collections::HashSet;

pub struct StatsBucket {
    tid: TransactionId,
    problem_transactions: HashSet<TransactionId>,
}

impl StatsBucket {
    pub fn new(tid: TransactionId) -> Self {
        Self {
            tid,
            problem_transactions: HashSet::new(),
        }
    }

    pub fn get_transaction_id(&self) -> TransactionId {
        self.tid.clone()
    }

    pub fn get_problem_transactions(&self) -> HashSet<TransactionId> {
        self.problem_transactions.clone()
    }

    pub fn add_problem_transaction(&mut self, id: TransactionId) {
        self.problem_transactions.insert(id);
    }
}
