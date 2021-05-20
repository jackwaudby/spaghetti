use std::collections::HashSet;
use std::fmt;

/// Represents an epoch in the protocol.
#[derive(Debug)]
pub struct Epoch {
    /// ID of this epoch.
    id: u64,

    /// Transactions that started in this epoch.
    started: HashSet<usize>,

    /// Transactions that finished in this epoch.
    terminated: Option<HashSet<usize>>,

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
    pub fn add_started(&mut self, id: usize) {
        self.started.insert(id);
        self.active += 1;
    }

    /// Add a transaction that was terminated in this epoch.
    pub fn add_terminated(&mut self, id: usize) {
        self.terminated.as_mut().unwrap().insert(id);
    }

    /// Take terminated transactions in his epoch.
    pub fn take_terminated(&mut self) -> HashSet<usize> {
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
    pub fn add_started(&mut self, transaction_id: usize) {
        let ce = self.current_epoch; // current epoch
        let pos = self.get_epoch_pos(ce); // get position in list
        self.epochs[pos].add_started(transaction_id); // add to started
    }

    /// Add transaction to terminated current epoch and decrement active in its start epoch.
    pub fn add_terminated(&mut self, transaction_id: usize, start_epoch: u64) {
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
    pub fn get_transactions_to_garbage_collect(&mut self, alpha: u64) -> Vec<usize> {
        let pos = self.get_epoch_pos(alpha); // position of alpha epoch

        // iterate until alpha - 1
        let mut to_remove: Vec<usize> = vec![]; //
        for i in 0..pos {
            assert!(self.epochs[i].get_id() < alpha);
            let mut tids: Vec<usize> = self.epochs[i].take_terminated().into_iter().collect();
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
