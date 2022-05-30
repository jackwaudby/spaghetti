use crate::scheduler::attendez::transaction::{PredecessorSet, TransactionState};

pub struct PredecessorSummary {
    aborted: u64,
    _active: u64,
    committed: u64,
}

impl PredecessorSummary {
    pub fn has_aborted_predecessor(&self) -> bool {
        self.aborted > 0
    }

    pub fn get_committed(&self) -> u64 {
        self.committed
    }
}

pub fn scan_predecessors<'a>(predecessors: &PredecessorSet<'a>) -> PredecessorSummary {
    let mut aborted = 0;
    let mut active = 0;
    let mut committed = 0;

    for predecessor in predecessors {
        match predecessor.get_state() {
            TransactionState::Active => {
                aborted += 1;
            }

            TransactionState::Aborted => {
                active += 1;
            }

            TransactionState::Committed => {
                committed += 1;
            }
        }
    }

    PredecessorSummary {
        aborted,
        _active: active,
        committed,
    }
}
