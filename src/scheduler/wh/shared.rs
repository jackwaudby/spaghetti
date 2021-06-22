use parking_lot::{Mutex, MutexGuard};
use rustc_hash::FxHashSet;
use std::hash::{Hash, Hasher};

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum TransactionOutcome {
    Aborted,
    Committed,
}

#[derive(Eq, Debug)]
pub struct TerminatedEntry {
    id: u64,
    outcome: TransactionOutcome,
}

#[derive(Debug)]
pub struct Lists {
    hit_list: FxHashSet<u64>,
    terminated_list: FxHashSet<TerminatedEntry>,
}
#[derive(Debug)]
pub struct Shared {
    txn_ctr: Mutex<u64>,
    lists: Mutex<Lists>,
}

impl TerminatedEntry {
    fn get_outcome(&self) -> TransactionOutcome {
        self.outcome.clone()
    }
}

impl Lists {
    fn new() -> Self {
        Self {
            hit_list: FxHashSet::default(),
            terminated_list: FxHashSet::default(),
        }
    }

    pub fn add_to_hit_list(&mut self, id: u64) {
        self.hit_list.insert(id);
    }

    pub fn remove_from_hit_list(&mut self, id: u64) {
        self.hit_list.remove(&id);
    }

    pub fn is_in_hit_list(&self, id: u64) -> bool {
        self.hit_list.contains(&id)
    }

    pub fn add_to_terminated_list(&mut self, id: u64, outcome: TransactionOutcome) {
        self.terminated_list.insert(TerminatedEntry { id, outcome });
    }

    pub fn has_terminated(&self, id: u64) -> bool {
        let pred = TerminatedEntry {
            id,
            outcome: TransactionOutcome::Aborted, // safe to use any outcome as hash by id
        };
        self.terminated_list.contains(&pred)
    }

    pub fn get_terminated_outcome(&self, id: u64) -> TransactionOutcome {
        let pred = TerminatedEntry {
            id,
            outcome: TransactionOutcome::Aborted, // safe to use any outcome as hash by id
        };
        self.terminated_list.get(&pred).unwrap().get_outcome()
    }

    pub fn remove_from_terminated_list(&mut self, id: u64) {
        let entry = TerminatedEntry {
            id,
            outcome: TransactionOutcome::Aborted,
        };
        self.terminated_list.remove(&entry);
    }
}

impl Shared {
    pub fn new() -> Self {
        Self {
            txn_ctr: Mutex::new(0),
            lists: Mutex::new(Lists::new()),
        }
    }

    pub fn get_id(&self) -> u64 {
        let mut lock = self.txn_ctr.lock(); // get mutex lock
        let id = *lock; // get copy of id
        *lock += 1; // increment
        id
    }

    pub fn get_lock(&self) -> MutexGuard<Lists> {
        self.lists.lock()
    }
}

impl PartialEq for TerminatedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for TerminatedEntry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.id.hash(hasher);
    }
}
