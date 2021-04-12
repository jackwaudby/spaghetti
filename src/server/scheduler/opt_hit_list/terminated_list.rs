use crate::server::scheduler::opt_hit_list::error::OptimisedHitListError;
use crate::server::scheduler::NonFatalError;
use crate::workloads::PrimaryKey;
use std::sync::{Mutex, RwLock};

/// Represents a transaction's state.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Aborted,
    Committed,
}

/// Type of predecessor
pub enum PredecessorUpon {
    Read,
    Write,
}

/// Represents a transaction.
#[derive(Debug)]
pub struct Transaction {
    /// Transaction id.
    id: u64,

    /// Transaction state.
    state: Mutex<TransactionState>,

    /// Predecessors upon read.
    wait_list: Mutex<Option<Vec<String>>>,

    /// Predecessors upon write.
    hit_list: Mutex<Option<Vec<String>>>,

    /// List of keys inserted.
    keys_inserted: Mutex<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys updated.
    keys_updated: Mutex<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys deleted.
    keys_deleted: Mutex<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys read.
    keys_read: Mutex<Option<Vec<(String, PrimaryKey)>>>,
}

/// Per-thread state.
#[derive(Debug)]
pub struct ThreadState {
    /// Transaction id generator.
    seq_num: Mutex<u64>,

    /// Termination list.
    terminated_list: Vec<RwLock<Transaction>>,
}

impl Transaction {
    /// Create new transaction
    fn new(id: u64) -> Transaction {
        Transaction {
            id,
            state: Mutex::new(TransactionState::Active),
            wait_list: Mutex::new(Some(vec![])),
            hit_list: Mutex::new(Some(vec![])),
            keys_updated: Mutex::new(Some(vec![])),
            keys_deleted: Mutex::new(Some(vec![])),
            keys_read: Mutex::new(Some(vec![])),
            keys_inserted: Mutex::new(Some(vec![])),
        }
    }

    /// Get id of transaction.
    fn get_id(&self) -> u64 {
        self.id
    }

    /// Get transaction state.
    fn get_state(&self) -> TransactionState {
        self.state.lock().unwrap().clone()
    }

    /// Set transaction state.
    fn set_state(&mut self, new: TransactionState) {
        *self.state.lock().unwrap() = new;
    }

    /// Add predecessor.
    fn add_predecessor(&self, pid: String, predecessor_upon: PredecessorUpon) {
        use PredecessorUpon::*;
        match predecessor_upon {
            Read => self.wait_list.lock().unwrap().as_mut().unwrap().push(pid),
            Write => self.hit_list.lock().unwrap().as_mut().unwrap().push(pid),
        }
    }

    /// Get predecessors.
    fn get_predecessors(&self, predecessor_upon: PredecessorUpon) -> Vec<String> {
        use PredecessorUpon::*;
        match predecessor_upon {
            Read => self.wait_list.lock().unwrap().take().unwrap(),
            Write => self.hit_list.lock().unwrap().take().unwrap(),
        }
    }

    /// Get the list of keys updated/deleted by this transaction.
    pub fn get_keys_updated(&self) -> Vec<(String, PrimaryKey)> {
        self.keys_updated.lock().unwrap().take().unwrap()
    }

    /// Add key to list of those updated/deleted by this transaction.
    pub fn add_key_updated(&self, key: (String, PrimaryKey)) {
        self.keys_updated
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .push(key);
    }

    /// Get the list of keys updated/deleted by this transaction.
    pub fn get_keys_deleted(&self) -> Vec<(String, PrimaryKey)> {
        self.keys_deleted.lock().unwrap().take().unwrap()
    }

    /// Add key to list of those updated/deleted by this transaction.
    pub fn add_key_deleted(&self, key: (String, PrimaryKey)) {
        self.keys_deleted
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .push(key);
    }

    /// Get the list of keys read by this transaction.
    pub fn get_keys_read(&self) -> Vec<(String, PrimaryKey)> {
        self.keys_read.lock().unwrap().take().unwrap()
    }

    /// Add key to list of those read by this transaction.
    pub fn add_key_read(&self, key: (String, PrimaryKey)) {
        self.keys_read.lock().unwrap().as_mut().unwrap().push(key);
    }

    /// Get the list of keys inserted by this transaction.
    pub fn get_keys_inserted(&self) -> Vec<(String, PrimaryKey)> {
        self.keys_inserted.lock().unwrap().take().unwrap()
    }

    /// Add key to list of those read by this transaction.
    pub fn add_key_inserted(&self, key: (String, PrimaryKey)) {
        self.keys_inserted
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .push(key);
    }
}

impl ThreadState {
    /// Create new thread state.
    pub fn new() -> ThreadState {
        ThreadState {
            seq_num: Mutex::new(0),
            terminated_list: vec![],
        }
    }

    /// Get transaction id.
    fn get_id(&mut self) -> u64 {
        let id = *self.seq_num.lock().unwrap();
        *self.seq_num.lock().unwrap() += 1;
        id
    }

    /// Register new transaction with thread.
    pub fn new_transaction(&mut self) -> u64 {
        let id = self.get_id();
        let transaction = RwLock::new(Transaction::new(id));
        self.terminated_list.push(transaction);
        id
    }

    /// Get index in list of transaction with `id`.
    pub fn get_index(&self, id: u64) -> usize {
        self.terminated_list
            .iter()
            .position(|x| x.read().unwrap().get_id() == id)
            .unwrap()
    }

    /// Add predecessor for transaction with `id`.
    pub fn add_predecessor(
        &self,
        id: u64,
        predecessor_id: String,
        predecessor_upon: PredecessorUpon,
    ) {
        let index = self.get_index(id);
        self.terminated_list[index]
            .read()
            .unwrap()
            .add_predecessor(predecessor_id, predecessor_upon);
    }

    /// Get wait list for transaction `id`.
    pub fn get_wait_list(&self, id: u64) -> Vec<String> {
        let index = self.get_index(id);
        self.terminated_list[index]
            .read()
            .unwrap()
            .get_predecessors(PredecessorUpon::Read)
    }

    /// Get hit list for transaction `id`.
    pub fn get_hit_list(&self, id: u64) -> Vec<String> {
        let index = self.get_index(id);
        self.terminated_list[index]
            .read()
            .unwrap()
            .get_predecessors(PredecessorUpon::Write)
    }

    /// Set state for transaction `id`.
    pub fn set_state(&self, id: u64, state: TransactionState) {
        let index = self.get_index(id);
        self.terminated_list[index]
            .write()
            .unwrap()
            .set_state(state);
    }

    /// Set state for transaction `id`.
    pub fn try_commit(&self, id: u64) -> Result<(), NonFatalError> {
        let index = self.get_index(id);
        let mut wlock = self.terminated_list[index].write().unwrap();
        let state = wlock.get_state();
        match state {
            TransactionState::Active => {
                wlock.set_state(TransactionState::Committed);
            }
            TransactionState::Aborted => {
                return Err(OptimisedHitListError::Hit(id.to_string()).into());
            }

            TransactionState::Committed => {}
        }
        Ok(())
    }

    /// Get state for transaction `id`.
    pub fn get_state(&self, id: u64) -> TransactionState {
        let index = self.get_index(id);
        self.terminated_list[index].read().unwrap().get_state()
    }

    /// Remove transaction with `id`.
    fn remove_transaction(&mut self, id: u64) {
        let index = self.get_index(id);
        self.terminated_list.remove(index);
    }

    /// Add key to transaction.
    pub fn add_key(&self, tid: u64, key: (String, PrimaryKey), operation: Operation) {
        let index = self.get_index(tid);
        let entry = self.terminated_list[index].read().unwrap();

        use Operation::*;
        match operation {
            Create => entry.add_key_inserted(key),
            Read => entry.add_key_read(key),
            Update => entry.add_key_updated(key),
            Delete => entry.add_key_deleted(key),
        }
    }

    /// Get key to transaction.
    pub fn get_keys(&self, tid: u64, operation: Operation) -> Vec<(String, PrimaryKey)> {
        let index = self.get_index(tid);
        let entry = self.terminated_list[index].read().unwrap();

        use Operation::*;
        match operation {
            Create => entry.get_keys_inserted(),
            Read => entry.get_keys_read(),
            Update => entry.get_keys_updated(),
            Delete => entry.get_keys_deleted(),
        }
    }
}

pub enum Operation {
    Create,
    Read,
    Update,
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opt_transaction() {
        let mut t = Transaction::new(5);
        assert_eq!(t.get_id(), 5);
        assert_eq!(t.add_predecessor(3.to_string(), PredecessorUpon::Read), ());
        assert_eq!(t.add_predecessor(2.to_string(), PredecessorUpon::Write), ());
        assert_eq!(t.get_state(), TransactionState::Active);

        assert_eq!(t.set_state(TransactionState::Aborted), ());
        assert_eq!(t.get_state(), TransactionState::Aborted);

        assert_eq!(t.set_state(TransactionState::Committed), ());
        assert_eq!(t.get_state(), TransactionState::Committed);
    }

    #[test]
    fn test_opt_thread_state() {
        let mut ts = ThreadState::new();
        assert_eq!(ts.new_transaction(), 0);

        ts.add_predecessor(0, 2.to_string(), PredecessorUpon::Read);
        ts.add_predecessor(0, 3.to_string(), PredecessorUpon::Write);

        assert_eq!(ts.get_wait_list(0), vec![2.to_string()]);
        assert_eq!(ts.get_hit_list(0), vec![3.to_string()]);

        ts.set_state(0, TransactionState::Committed);

        ts.remove_transaction(0);

        assert_eq!(ts.terminated_list.len(), 0)
    }
}
