use crate::workloads::PrimaryKey;

use parking_lot::Mutex;
use std::fmt;

use std::thread;
use tracing::debug;

/// A `Node` represents a transaction in the serialization graph.
///
/// At creation each node is assigned to a thread. `Node`s can be
/// accessed in shared mode or exculsive mode.
///
/// # Safety
///
/// The fields of a `Node` are each wrapping in a mutexso that multiple
/// threads holding shared locks can concurrently modify these fields.
#[derive(Debug)]
pub struct Node {
    /// Node's position in the graph.
    id: usize,

    /// Node status
    state: Mutex<Option<State>>,

    /// List of outgoing edges.
    /// (this_node) --> (other_node)
    outgoing: Mutex<Option<Vec<usize>>>,

    /// List of incoming edges.
    /// (other_node) --> (this_node)
    incoming: Mutex<Option<Vec<usize>>>,

    /// Keys inserted
    keys_inserted: Mutex<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys read by transaction.
    keys_read: Mutex<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys updated by transaction.
    keys_updated: Mutex<Option<Vec<(String, PrimaryKey)>>>,

    /// List of keys deleted by transaction.
    keys_deleted: Mutex<Option<Vec<(String, PrimaryKey)>>>,
}

/// Represents states a `Node` can be in.
#[derive(Debug, PartialEq, Clone)]
pub enum State {
    /// Active transaction resides in node.
    Active,
    /// Transaction in node has aborted.
    Aborted,
    /// Transaction in node has committed.
    Committed,
}

/// Type of edge.
pub enum EdgeType {
    Incoming,
    Outgoing,
}

/// Type of operation.
pub enum OperationType {
    Insert,
    Read,
    Update,
    Delete,
}

impl Node {
    /// Create a new `Node`.
    pub fn new(id: usize) -> Node {
        Node {
            id,
            outgoing: Mutex::new(Some(vec![])),
            incoming: Mutex::new(Some(vec![])),
            state: Mutex::new(None),
            keys_inserted: Mutex::new(Some(vec![])),
            keys_read: Mutex::new(Some(vec![])),
            keys_updated: Mutex::new(Some(vec![])),
            keys_deleted: Mutex::new(Some(vec![])),
        }
    }

    /// Reset the fields of a `Node`.
    ///
    /// # Safety
    ///
    /// This method is only called by the thread on which the
    /// node belongs.
    pub fn reset(&self) {
        let mut outgoing = self.outgoing.lock();
        let mut incoming = self.incoming.lock();
        let mut inserted = self.keys_inserted.lock();
        let mut read = self.keys_read.lock();
        let mut updated = self.keys_updated.lock();
        let mut deleted = self.keys_deleted.lock();
        *outgoing = Some(vec![]);
        *incoming = Some(vec![]);
        *inserted = Some(vec![]);
        *read = Some(vec![]);
        *updated = Some(vec![]);
        *deleted = Some(vec![]);
    }

    /// Insert edge into a `Node`.
    ///
    /// No duplicate edges are inserted.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn insert_edge(&self, id: usize, edge_type: EdgeType) {
        match edge_type {
            EdgeType::Incoming => {
                if let Some(incoming) = self.incoming.lock().as_mut() {
                    if !incoming.contains(&id) {
                        incoming.push(id);
                    }
                }
            }
            EdgeType::Outgoing => {
                if let Some(outgoing) = self.outgoing.lock().as_mut() {
                    if !outgoing.contains(&id) {
                        outgoing.push(id);
                    }
                }
            }
        }
    }

    /// Remove edge from `Node`.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn delete_edge(&self, id: usize, edge_type: EdgeType) {
        match edge_type {
            EdgeType::Incoming => {
                self.incoming.lock().as_mut().unwrap().retain(|&x| x != id);
            }
            EdgeType::Outgoing => {
                self.outgoing.lock().as_mut().unwrap().retain(|&x| x != id);
            }
        }
    }

    /// Clones the incoming edges from a `Node` leaving a `None`.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn get_incoming(&self) -> Vec<usize> {
        self.incoming.lock().clone().unwrap()
    }

    /// Clones the outgoing edges from a `Node` leaving a `None`.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn get_outgoing(&self) -> Vec<usize> {
        self.outgoing.lock().clone().unwrap()
    }

    /// Takes the list of keys inserted/read/updated/deleted by the transaction in the node.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn get_keys(&self, operation_type: OperationType) -> Vec<(String, PrimaryKey)> {
        use OperationType::*;
        match operation_type {
            Insert => {
                let mut data = self.keys_inserted.lock();
                data.take().unwrap()
            }

            Read => self.keys_read.lock().take().unwrap(),
            Update => self.keys_updated.lock().take().unwrap(),
            Delete => self.keys_deleted.lock().take().unwrap(),
        }
    }

    /// Add key to the list of keys inserted/read/updated/deleted by the transaction in the node.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn add_key(&self, index: &str, key: PrimaryKey, operation_type: OperationType) {
        let handle = thread::current();
        debug!(
            "Thread {}: Recorded {} operation",
            handle.name().unwrap(),
            operation_type
        );
        let pair = (index.to_string(), key);
        use OperationType::*;
        match operation_type {
            Insert => {
                let mut data = self.keys_inserted.lock();
                data.as_mut().unwrap().push(pair)
            }
            Read => self.keys_read.lock().as_mut().unwrap().push(pair),
            Update => self.keys_updated.lock().as_mut().unwrap().push(pair),
            Delete => self.keys_deleted.lock().as_mut().unwrap().push(pair),
        }
    }

    /// Get the status of the `Node`.
    ///
    /// Clones the value behind the mutex.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn get_state(&self) -> State {
        let data = self.state.lock();
        data.as_ref().unwrap().clone()
    }

    /// Set the status of the `Node`.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn set_state(&self, state: State) {
        let mut data = self.state.lock();
        *data = Some(state);
    }

    /// Check if `Node` has any incoming edges.
    ///
    /// # Panics
    ///
    /// Fails to acquire mutex.
    pub fn has_incoming(&self) -> bool {
        let data = self.incoming.lock();
        !data.as_ref().unwrap().is_empty()
    }
}

impl fmt::Display for OperationType {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use OperationType::*;
        match *self {
            Insert => write!(f, "insert"),
            Update => write!(f, "update"),
            Read => write!(f, "read"),
            Delete => write!(f, "delete"),
        }
    }
}
#[cfg(test)]
mod tests {

    use super::*;
    use crate::workloads::tatp::keys::TatpPrimaryKey;

    #[test]
    fn node_test() {
        // Create node.
        let n = Node::new(0);

        // Set state
        assert_eq!(n.set_state(State::Active), ());
        assert_eq!(n.get_state(), State::Active);

        // Insert outgoing edges.
        assert_eq!(n.insert_edge(1, EdgeType::Outgoing), ());
        assert_eq!(n.insert_edge(2, EdgeType::Outgoing), ());

        // Check incoming
        assert_eq!(n.has_incoming(), false, "{:?}", n);
        assert_eq!(n.insert_edge(2, EdgeType::Incoming), ());
        assert_eq!(n.has_incoming(), true);
        assert_eq!(n.delete_edge(2, EdgeType::Incoming), ());
        assert_eq!(n.has_incoming(), false);

        // Take outgoing.
        assert_eq!(n.delete_edge(1, EdgeType::Outgoing), ());
        assert_eq!(n.get_outgoing(), vec![2]);

        // Set state
        n.set_state(State::Committed);
        assert_eq!(n.get_state(), State::Committed);
        n.set_state(State::Aborted);
        assert_eq!(n.get_state(), State::Aborted);

        // Add key
        n.add_key(
            "test",
            PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
            OperationType::Read,
        );

        assert_eq!(
            n.get_keys(OperationType::Read),
            vec![(
                "test".to_string(),
                PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1))
            )]
        );
    }
}
