use crate::{scheduler::msgt::Cycle, storage::access::TransactionId};

use std::collections::HashSet;

pub struct StatsBucket {
    tid: TransactionId,
    problem_transactions: HashSet<usize>,
    abort_through: usize,
    cycle_type: Option<Cycle>,
    path_len: Option<usize>,
    ww_conflicts: u64,
    wr_conflicts: u64,
    rw_conflicts: u64,
}

impl StatsBucket {
    pub fn new(tid: TransactionId) -> Self {
        Self {
            tid,
            problem_transactions: HashSet::new(),
            abort_through: 0,
            cycle_type: None,
            path_len: None,
            ww_conflicts: 0,
            wr_conflicts: 0,
            rw_conflicts: 0,
        }
    }

    pub fn inc_ww_conflicts(&mut self) {
        self.ww_conflicts += 1;
    }

    pub fn get_ww_conflicts(&self) -> u64 {
        self.ww_conflicts
    }

    pub fn inc_wr_conflicts(&mut self) {
        self.wr_conflicts += 1;
    }

    pub fn get_wr_conflicts(&self) -> u64 {
        self.wr_conflicts
    }

    pub fn inc_rw_conflicts(&mut self) {
        self.rw_conflicts += 1;
    }

    pub fn get_rw_conflicts(&self) -> u64 {
        self.rw_conflicts
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
