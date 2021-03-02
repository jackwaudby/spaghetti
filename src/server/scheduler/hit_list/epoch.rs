use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::debug;

#[derive(Debug)]
pub struct AtomicSharedResources {
    resources: Arc<Mutex<SharedResources>>,
}

impl AtomicSharedResources {
    pub fn new() -> AtomicSharedResources {
        let resources = Arc::new(Mutex::new(SharedResources::new()));
        AtomicSharedResources { resources }
    }

    pub fn get_lock(&self) -> MutexGuard<SharedResources> {
        self.resources.lock().unwrap()
    }

    pub fn get_ref(&self) -> Arc<Mutex<SharedResources>> {
        Arc::clone(&self.resources)
    }
}

#[derive(Debug)]
pub struct SharedResources {
    pub hit_list: HashSet<u64>,
    pub terminated_list: HashSet<u64>,
    current_epoch: u64,
    epochs: Vec<Epoch>,
    alpha: Option<u64>,
}

impl fmt::Display for SharedResources {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let top = "\n|---------|-----------|---------------|---|\n";
        let head = "|  epoch  |  started  |  terminated  | n |\n";
        let mut join = format!("{}{}", top, head);

        for epoch in &self.epochs {
            join = format!("{}{} \n", join, epoch);
        }
        join = format!("{}|---------|-----------|---------------|---|", join);
        write!(f, "{}", join)
    }
}

impl SharedResources {
    /// Create new shared resources.
    fn new() -> SharedResources {
        let mut epochs = Vec::new();
        let epoch = Epoch::new(0);
        epochs.push(epoch);

        SharedResources {
            hit_list: HashSet::new(),
            terminated_list: HashSet::new(),
            current_epoch: 0,
            epochs,
            alpha: None,
        }
    }

    pub fn get_current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Start new epoch
    pub fn new_epoch(&mut self) {
        self.current_epoch += 1;
        debug!("Starting epoch {}", self.current_epoch);
        let epoch = Epoch::new(self.current_epoch);
        self.epochs.push(epoch);
    }

    /// Get position of epoch in the list of epochs
    fn get_epoch(&self, id: u64) -> usize {
        self.epochs.iter().position(|epoch| epoch.id == id).unwrap()
    }

    /// Add started transaction to current epoch.
    pub fn add_started(&mut self, id: u64) {
        debug!("Transaction {} started in epoch {}", id, self.current_epoch);
        let ce = self.current_epoch;
        let pos = self.get_epoch(ce);

        self.epochs[pos].add_started(id);
    }

    /// Add terminated transaction to current epoch.
    pub fn add_terminated(&mut self, id: u64, start_epoch: u64) {
        debug!(
            "Add transaction {} to terminated in epoch {}",
            id, self.current_epoch
        );

        // Add to current epoch terminated
        let ce = self.current_epoch;
        let ce_pos = self.get_epoch(ce);
        self.epochs[ce_pos].add_terminated(id);

        // Remove from started epoch
        debug!(
            "Remove transaction {} from started in in epoch {}",
            id, start_epoch
        );
        let se_pos = self.get_epoch(start_epoch);
        self.epochs[se_pos].remove_started(id);
    }

    /// Update alpha
    pub fn update_alpha(&mut self) {
        debug!("Update alpha");
        // Get current epoch.
        let ce = self.current_epoch;

        // Assuming Vec maintains insertion order, epochs are GC'd in this order.
        let ep = self
            .epochs
            .iter()
            .find(|&epoch| !epoch.has_active_transactions())
            .unwrap()
            .get_epoch_num();

        if ep == ce {
            return;
        }

        let pos = self.get_epoch(ep);

        // Remove the epoch.
        let mut epoch = self.epochs.remove(pos);
        // Remove from terminated list
        let to_remove = epoch.get_terminated();

        for id in to_remove {
            self.terminated_list.remove(&id);
        }

        // Set alpha
        self.alpha = Some(epoch.id);
    }
}

#[derive(Debug)]
struct Epoch {
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
    fn new(id: u64) -> Epoch {
        Epoch {
            id,
            started: HashSet::new(),
            terminated: Some(HashSet::new()),
            active_counter: 0,
        }
    }

    /// Get epoch number.
    fn get_epoch_num(&self) -> u64 {
        self.id
    }

    /// Add a transaction that was started in this epoch.
    pub fn add_started(&mut self, id: u64) {
        self.started.insert(id);
        self.active_counter += 1;
    }

    /// Remove a transaction that was started in this epoch.
    fn remove_started(&mut self, id: u64) {
        self.started.remove(&id);
        self.active_counter -= 1;
    }

    /// Add a transaction that was terminated in this epoch.
    pub fn add_terminated(&mut self, id: u64) {
        self.terminated.as_mut().unwrap().insert(id);
    }

    /// Get terminated transaction in his epoch.
    fn get_terminated(&mut self) -> HashSet<u64> {
        self.terminated.take().unwrap()
    }

    /// Epoch has no active transactions.
    fn has_active_transactions(&self) -> bool {
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
