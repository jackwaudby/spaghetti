use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::sync::MutexGuard;
// use crate::common::ds::spin_mutex::{Mutex, MutexGuard};

// use spin::{Mutex, MutexGuard};

use crate::storage::access::TransactionId;

#[derive(Debug)]
pub struct WaitManager {
    locks: Vec<Mutex<u8>>,
    // locks: Vec<Mutex>,
    cores: usize,
}

impl WaitManager {
    pub fn new(cores: usize) -> Self {
        let mut locks = Vec::with_capacity(cores);

        for i in 0..cores {
            locks.push(Mutex::new(i as u8));
            // locks.push(Mutex::new());
        }

        Self { locks, cores }
    }

    pub fn wait(&self, tid: u64, neighbours: HashSet<usize>) -> Vec<MutexGuard<'_, u8>> {
        let mut oset = BTreeSet::new();
        for nid in &neighbours {
            let e = *nid as u64;
            // if e != 0 {
            let offset = self.calculate_offset(&e);
            oset.insert(offset);
            // }
        }
        let offset = self.calculate_offset(&tid);
        oset.insert(offset);

        let mut guards = Vec::new();
        for t in oset {
            guards.push(self.locks[t].lock().unwrap());
            // guards.push(self.locks[t].lock());
            // println!("id: {} got lock on offset: {}", tid, t);
        }
        guards
    }

    pub fn release(&self, guards: Vec<MutexGuard<'_, u8>>) {
        for guard in guards {
            drop(guard);
        }
    }
    fn calculate_hash<T: Hash>(&self, t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }
    fn calculate_offset(&self, tid: &u64) -> usize {
        (self.calculate_hash(tid) % self.cores as u64) as usize
    }
}
