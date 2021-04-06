use std::collections::HashSet;
use std::fmt;

#[derive(Debug)]
pub struct Epoch {
    /// ID of this epoch.
    id: u64,

    /// Transactions that started in this epoch.
    started: HashSet<u64>,

    /// Transactions that finished in this epoch.
    terminated: Option<HashSet<u64>>,

    /// Counter of transactions started in this epoch that are still active.
    active_counter: u64,
}

impl Epoch {
    /// Create new epoch.
    pub fn new(id: u64) -> Epoch {
        Epoch {
            id,
            started: HashSet::new(),
            terminated: Some(HashSet::new()),
            active_counter: 0,
        }
    }

    /// Get epoch number.
    pub fn get_epoch_num(&self) -> u64 {
        self.id
    }

    /// Add a transaction that was started in this epoch.
    pub fn add_started(&mut self, id: u64) {
        self.started.insert(id);
        self.active_counter += 1;
    }

    /// Remove a transaction that was started in this epoch.
    pub fn remove_started(&mut self, id: u64) {
        self.started.remove(&id);
        self.active_counter -= 1;
    }

    /// Add a transaction that was terminated in this epoch.
    pub fn add_terminated(&mut self, id: u64) {
        self.terminated.as_mut().unwrap().insert(id);
    }

    /// Get terminated transaction in his epoch.
    pub fn get_terminated(&mut self) -> HashSet<u64> {
        self.terminated.take().unwrap()
    }

    /// Epoch has no active transactions.
    pub fn has_active_transactions(&self) -> bool {
        self.active_counter != 0
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "| {} | {:?} | {:?} | {} |",
            self.id,
            self.started,
            self.terminated.as_ref().unwrap(),
            self.active_counter
        )
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn epoch_test() {
//         let mut epoch = Epoch::new(6);

//         assert_eq!(epoch.get_epoch_num(), 6);
//         assert_eq!(epoch.has_active_transactions(), false);

//         epoch.add_started(3);
//         epoch.add_terminated(7);

//         assert_eq!(epoch.has_active_transactions(), true);

//         epoch.remove_started(3);

//         assert_eq!(epoch.has_active_transactions(), false);

//         let set: HashSet<_> = [7].iter().cloned().collect();

//         assert_eq!(epoch.get_terminated(), set);
//     }

//     #[test]
//     fn epoch_tracker_test() {
//         let hit_list = Arc::new(Mutex::new((HashSet::new(), HashSet::new())));

//         let mut et = EpochTracker::new(hit_list);

//         assert_eq!(et.current_epoch, 0);
//         assert_eq!(et.alpha, None);

//         // Epoch 0
//         et.add_started(1);
//         et.add_started(2);
//         et.add_started(3);
//         et.add_terminated(2, 0);

//         let pos = et.get_epoch(0);
//         let s0: HashSet<_> = [1, 3].iter().cloned().collect();
//         assert_eq!(et.epochs[pos].started, s0);

//         let t0: HashSet<_> = [2].iter().cloned().collect();
//         assert_eq!(et.epochs[pos].terminated.as_ref().unwrap(), &t0, "{:?}", et);
//         assert_eq!(et.alpha, None);

//         // Epoch 1
//         assert_eq!(et.new_epoch(), ());
//         assert_eq!(et.update_alpha(), ());
//         assert_eq!(et.current_epoch, 1);
//         assert_eq!(et.alpha, None);

//         et.add_started(4);
//         et.add_started(5);
//         et.add_terminated(1, 0);
//         et.add_terminated(3, 0);

//         let pos = et.get_epoch(1);
//         let s1: HashSet<_> = [4, 5].iter().cloned().collect();
//         assert_eq!(et.epochs[pos].started, s1);

//         let t1: HashSet<_> = [1, 3].iter().cloned().collect();
//         assert_eq!(et.epochs[pos].terminated.as_ref().unwrap(), &t1);
//         assert_eq!(et.alpha, None);

//         // Epoch 2
//         assert_eq!(et.new_epoch(), ());
//         assert_eq!(et.update_alpha(), ());
//         assert_eq!(et.current_epoch, 2);
//         assert_eq!(et.alpha, Some(0), "{:?}", et);

//         et.add_started(6);
//         et.add_started(7);
//         et.add_terminated(4, 1);
//         et.add_terminated(5, 1);

//         let pos = et.get_epoch(2);
//         let s2: HashSet<_> = [6, 7].iter().cloned().collect();
//         assert_eq!(et.epochs[pos].started, s2);

//         let t2: HashSet<_> = [4, 5].iter().cloned().collect();

//         assert_eq!(et.epochs[pos].terminated.as_ref().unwrap(), &t2);
//         assert_eq!(et.alpha, Some(0));

//         // Epoch 3
//         assert_eq!(et.new_epoch(), ());
//         assert_eq!(et.update_alpha(), ());
//         assert_eq!(et.current_epoch, 3);
//         assert_eq!(et.alpha, Some(1), "{:?}", et);

//         et.add_started(8);
//         et.add_terminated(7, 2);

//         let pos = et.get_epoch(3);
//         let s3: HashSet<_> = [8].iter().cloned().collect();
//         assert_eq!(et.epochs[pos].started, s3);

//         let t3: HashSet<_> = [7].iter().cloned().collect();
//         assert_eq!(et.epochs[pos].terminated.as_ref().unwrap(), &t3);
//         assert_eq!(et.alpha, Some(1));
//     }
// }
