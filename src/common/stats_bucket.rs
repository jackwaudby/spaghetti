use crate::{scheduler::msgt::Cycle, storage::access::TransactionId};

use std::collections::HashSet;

pub struct StatsBucket {
    tid: TransactionId,
    problem_transactions: HashSet<usize>,
    abort_through: usize,
    cycle_type: Option<Cycle>,
    path_len: Option<usize>,
    ww_conflict_detected: u64,
}

impl StatsBucket {
    pub fn new(tid: TransactionId) -> Self {
        Self {
            tid,
            problem_transactions: HashSet::new(),
            abort_through: 0,
            cycle_type: None,
            path_len: None,
            ww_conflict_detected: 0,
        }
    }

    pub fn inc_ww_conflict_detected(&mut self) {
        self.ww_conflict_detected += 1;
    }

    pub fn get_ww_conflict_detected(&self) -> u64 {
        self.ww_conflict_detected
    }

    pub fn set_cycle_type(&mut self, cycle: Cycle) {
        self.cycle_type.replace(cycle);
    }

    pub fn get_cycle_type(&mut self) -> Option<Cycle> {
        self.cycle_type.clone()
    }

    pub fn set_path_len(&mut self, len: usize) {
        self.path_len.replace(len);
    }

    pub fn get_path_len(&mut self) -> &Option<usize> {
        &self.path_len
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

    pub fn get_problem_transactions(&self) -> HashSet<usize> {
        self.problem_transactions.clone()
    }

    pub fn add_problem_transaction(&mut self, id: usize) {
        self.problem_transactions.insert(id);
    }
}
