use std::collections::{hash_map::DefaultHasher, BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
pub struct WaitManager {
    locks: Vec<Mutex<u8>>,
    cores: usize,
}

impl WaitManager {
    pub fn new(cores: usize) -> Self {
        let mut locks = Vec::with_capacity(cores);

        for i in 0..cores {
            locks.push(Mutex::new(i as u8));
        }

        Self { locks, cores }
    }

    pub fn wait(&self, tid: u64, neighbours: HashSet<usize>) -> Vec<MutexGuard<'_, u8>> {
        let mut oset = BTreeSet::new();
        for nid in &neighbours {
            let e = *nid as u64;

            let offset = self.calculate_offset(&e);
            oset.insert(offset);
        }
        let offset = self.calculate_offset(&tid);
        oset.insert(offset);

        let mut guards = Vec::new();
        for t in oset {
            guards.push(self.locks[t].lock().unwrap());
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
