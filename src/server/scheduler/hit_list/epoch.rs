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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_test() {
        let mut epoch = Epoch::new(6);
        assert_eq!(epoch.get_epoch_num(), 6);
        assert_eq!(epoch.has_active_transactions(), false);
        assert_eq!(epoch.add_started(3), ());
        assert_eq!(epoch.add_terminated(7), ());
        assert_eq!(epoch.has_active_transactions(), true);
        assert_eq!(epoch.remove_started(3), ());
        assert_eq!(epoch.has_active_transactions(), false);
        let set: HashSet<_> = [7].iter().cloned().collect();
        assert_eq!(epoch.get_terminated(), set);
    }
}
