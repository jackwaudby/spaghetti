use std::collections::HashSet;
use std::fmt;

/// Represents an epoch in the protocol.
#[derive(Debug)]
pub struct Epoch {
    /// ID of this epoch.
    id: u64,

    /// Transactions that started in this epoch.
    started: HashSet<u64>,

    /// Transactions that finished in this epoch.
    terminated: Option<HashSet<u64>>,

    /// Counter of transactions started in this epoch that are still active.
    active: u64,
}

/// Used to track epochs in the protocol.
#[derive(Debug)]
pub struct EpochTracker {
    /// Current epoch of the protocol.
    current_epoch: u64,

    /// List of epochs.
    epochs: Vec<Epoch>,

    /// Smallest epoch with at least 1 active transaction.
    alpha: Option<u64>,
}

impl Epoch {
    /// Create new epoch.
    pub fn new(id: u64) -> Epoch {
        Epoch {
            id,
            started: HashSet::new(),
            terminated: Some(HashSet::new()),
            active: 0,
        }
    }

    /// Get epoch id
    pub fn get_id(&self) -> u64 {
        self.id
    }

    /// Add a transaction that was started in this epoch.
    pub fn add_started(&mut self, id: u64) {
        self.started.insert(id);
        self.active += 1;
    }

    /// Add a transaction that was terminated in this epoch.
    pub fn add_terminated(&mut self, id: u64) {
        self.terminated.as_mut().unwrap().insert(id);
    }

    /// Take terminated transactions in his epoch.
    pub fn take_terminated(&mut self) -> HashSet<u64> {
        self.terminated.take().unwrap()
    }

    /// Epoch has no active transactions.
    pub fn has_active(&self) -> bool {
        self.active != 0
    }

    /// Decrement active transactions by 1.
    pub fn decrement_active(&mut self) {
        self.active -= 1;
    }
}

impl EpochTracker {
    /// Create new epoch tracker.
    pub fn new() -> EpochTracker {
        let mut epochs = Vec::new();
        let epoch = Epoch::new(0);
        epochs.push(epoch);

        EpochTracker {
            current_epoch: 0,
            epochs,
            alpha: None,
        }
    }

    /// Get current epoch id.
    pub fn get_current_id(&mut self) -> u64 {
        self.current_epoch
    }

    /// Start new epoch
    pub fn new_epoch(&mut self) {
        self.current_epoch += 1;
        let epoch = Epoch::new(self.current_epoch);
        self.epochs.push(epoch);
    }

    /// Get position of epoch in the list of epochs
    fn get_epoch_pos(&self, id: u64) -> usize {
        self.epochs
            .iter()
            .position(|epoch| epoch.get_id() == id)
            .unwrap()
    }

    /// Add transaction to started in current epoch.
    pub fn add_started(&mut self, transaction_id: u64) {
        let ce = self.current_epoch; // current epoch
        let pos = self.get_epoch_pos(ce); // get position in list
        self.epochs[pos].add_started(transaction_id); // add to started
    }

    /// Add transaction to terminated current epoch and decrement active in its start epoch.
    pub fn add_terminated(&mut self, transaction_id: u64, start_epoch: u64) {
        let ce = self.current_epoch; // current epoch
        let ce_pos = self.get_epoch_pos(ce); // get position in list of current epoch
        self.epochs[ce_pos].add_terminated(transaction_id); // add to terminated

        let se_pos = self.get_epoch_pos(start_epoch); // transaction's start epoch
        self.epochs[se_pos].decrement_active(); // decrement active
    }

    /// Update alpha (lowest epoch with at least 1 active transaction).
    ///
    /// This is called immediately after a new epoch is started.
    ///
    /// # Examples
    ///
    /// 0 0 0 0 -> current epoch (4th)
    /// 0 1 3 0 ->  2nd
    /// 0 1 0 0 -> 2nd
    pub fn update_alpha(&mut self) -> u64 {
        // assumption: Vec maintains insertion order
        // find lowest epoch with at least 1 active transction
        let alpha = match self.epochs.iter().find(|&epoch| epoch.has_active()) {
            Some(epoch) => epoch.get_id(),
            // no epoch has active transactions - set to current epoch
            None => self.current_epoch,
        };

        // Set alpha
        self.alpha = Some(alpha);
        alpha
    }

    /// Remove epochs with id less than alpha.
    pub fn get_transactions_to_garbage_collect(&mut self, alpha: u64) -> Vec<u64> {
        let pos = self.get_epoch_pos(alpha); // position of alpha epoch

        // iterate until alpha - 1
        let mut to_remove: Vec<u64> = vec![]; //
        for i in 0..pos {
            assert!(self.epochs[i].get_id() < alpha);
            let mut tids: Vec<u64> = self.epochs[i].take_terminated().into_iter().collect();
            to_remove.append(&mut tids);
        }

        self.epochs.retain(|epoch| epoch.get_id() >= alpha); // remove epoch less than alpha
        to_remove
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "| {:<8}| {:<61}| {:<14}| {} |",
            self.id,
            format!("{:?}", self.started),
            self.terminated.as_ref().unwrap().len(),
            self.active,
        )
    }
}

impl fmt::Display for EpochTracker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let top =    "|---------|--------------------------------------------------------------|---------------|---|\n";
        let head = "|  epoch  |  started                                                     |  terminated   | n |\n";
        let mid =   "|---------|--------------------------------------------------------------|---------------|---|\n";
        let mut join = format!("{}{}{}", top, head, mid);

        for epoch in &self.epochs {
            join = format!("{}{} \n", join, epoch);
        }
        join = format!(
            "{}|---------|--------------------------------------------------------------|---------------|---|\n",
            join
        );
        write!(f, "{}", join)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_test() {
        let mut epoch = Epoch::new(6);
        assert_eq!(epoch.get_id(), 6);

        assert_eq!(epoch.has_active(), false);
        assert_eq!(epoch.add_started(3), ());
        assert_eq!(epoch.add_terminated(7), ());
        assert_eq!(epoch.has_active(), true);
        epoch.decrement_active();
        assert_eq!(epoch.has_active(), false);
    }

    #[test]
    fn epoch_tracker_test() {
        let mut tracker = EpochTracker::new();
        // Epoch 0
        assert_eq!(tracker.get_current_id(), 0);
        assert_eq!(tracker.alpha, None);
        tracker.add_started(1);
        tracker.add_started(2);
        tracker.add_started(3);
        tracker.add_terminated(3, 0);

        // Epoch 1
        tracker.new_epoch();
        assert_eq!(tracker.get_current_id(), 1);
        tracker.update_alpha();
        assert_eq!(tracker.alpha, Some(0));
        let v: Vec<u64> = vec![];
        assert_eq!(tracker.get_transactions_to_garbage_collect(), v);

        tracker.add_started(4);
        tracker.add_started(5);
        tracker.add_terminated(2, 0);
        tracker.add_terminated(4, 1);

        // Epoch 2
        tracker.new_epoch();
        assert_eq!(tracker.get_current_id(), 2);
        tracker.update_alpha();
        assert_eq!(tracker.alpha, Some(0));
        let v: Vec<u64> = vec![];
        assert_eq!(tracker.get_transactions_to_garbage_collect(), v);

        tracker.add_started(6);
        tracker.add_started(7);
        tracker.add_terminated(1, 0);
        tracker.add_terminated(5, 1);

        // Epoch 3
        tracker.new_epoch();
        assert_eq!(tracker.get_current_id(), 3);
        tracker.update_alpha();
        assert_eq!(tracker.alpha, Some(2));
        let exp3: Vec<u64> = vec![2, 3, 4];
        let mut actual3 = tracker.get_transactions_to_garbage_collect();
        actual3.sort();
        assert_eq!(actual3, exp3);

        tracker.add_terminated(6, 2);
        tracker.add_terminated(7, 2);

        // Epoch 4
        tracker.new_epoch();
        assert_eq!(tracker.get_current_id(), 4);
        tracker.update_alpha();
        assert_eq!(tracker.alpha, Some(4));
        let exp4: Vec<u64> = vec![1, 5, 6, 7];
        let mut actual4 = tracker.get_transactions_to_garbage_collect();
        actual4.sort();
        assert_eq!(actual4, exp4);

        assert_eq!(tracker.epochs.len(), 1);
    }
}
