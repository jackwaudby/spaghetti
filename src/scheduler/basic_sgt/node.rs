use crate::storage::index::Index;
use crate::workloads::PrimaryKey;

use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub struct NodeSet {
    id: usize,
    counter: u64,
    transactions: IntMap<u64, Node>,
}

impl NodeSet {
    /// Create a new `NodeSet`.
    pub fn new(id: usize) -> Self {
        NodeSet {
            id,
            counter: 0,
            transactions: IntMap::default(),
        }
    }

    /// Get a shared reference to a Node in the set of Node's on this thread.
    pub fn get_transaction(&self, id: u64) -> &Node {
        match self.transactions.get(&id) {
            Some(node) => node,
            None => panic!(
                "txn {} does not exist on {}, counter: {}",
                id, self.id, self.counter
            ),
        }
    }

    pub fn create_node(&mut self) -> (usize, u64) {
        let thread_id = self.id;
        let txn_id = self.counter;
        self.counter += 1;
        let node = Node::new(thread_id, txn_id);
        self.transactions.insert(txn_id, node);
        (thread_id, txn_id)
    }
}

/// A `Node` represents a transaction in the serialization graph.
///
/// At creation each node is assigned to a thread.
#[derive(Debug)]
pub struct Node {
    /// Thread id
    thread_id: usize,

    /// Node's position in the graph.
    id: u64,

    /// Node status
    state: Mutex<Option<State>>,

    /// List of outgoing edges.
    /// (this_node) --> (other_node)
    outgoing: Mutex<Option<Vec<(usize, u64)>>>,

    /// List of incoming edges.
    /// (other_node) --> (this_node)
    incoming: Mutex<Option<Vec<(usize, u64)>>>,

    /// List of keys read by transaction.
    keys_read: Mutex<Option<Vec<(Arc<Index>, PrimaryKey)>>>,

    /// List of keys updated by transaction.
    keys_updated: Mutex<Option<Vec<(Arc<Index>, PrimaryKey)>>>,
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
    Read,
    Update,
}

impl Node {
    /// Create a new `Node`.
    pub fn new(thread_id: usize, id: u64) -> Node {
        Node {
            thread_id,
            id,
            outgoing: Mutex::new(Some(vec![])),
            incoming: Mutex::new(Some(vec![])),
            state: Mutex::new(Some(State::Active)),
            keys_read: Mutex::new(Some(vec![])),
            keys_updated: Mutex::new(Some(vec![])),
        }
    }

    /// Get transaction id
    pub fn get_transaction_id(&self) -> u64 {
        self.id
    }

    /// Reset the fields of a `Node`.
    pub fn reset(&self) {
        let mut outgoing = self.outgoing.lock();
        let mut incoming = self.incoming.lock();
        let mut read = self.keys_read.lock();
        let mut updated = self.keys_updated.lock();

        *outgoing = Some(vec![]);
        *incoming = Some(vec![]);
        *read = Some(vec![]);
        *updated = Some(vec![]);
    }

    /// Insert edge into a `Node`.
    ///
    /// No duplicate edges are inserted.
    pub fn insert_edge(&self, id: (usize, u64), edge_type: EdgeType) {
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
    pub fn delete_edge(&self, id: (usize, u64), edge_type: EdgeType) {
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
    pub fn get_incoming(&self) -> Vec<(usize, u64)> {
        self.incoming.lock().clone().unwrap()
    }

    /// Clones the outgoing edges from a `Node` leaving a `None`.
    pub fn get_outgoing(&self) -> Vec<(usize, u64)> {
        self.outgoing.lock().clone().unwrap()
    }

    pub fn get_keys2(&self, operation_type: OperationType) -> Vec<(Arc<Index>, PrimaryKey)> {
        use OperationType::*;
        match operation_type {
            Read => self.keys_read.lock().take().unwrap(),
            Update => self.keys_updated.lock().take().unwrap(),
        }
    }

    pub fn add_key2(&self, index: Arc<Index>, key: &PrimaryKey, operation_type: OperationType) {
        let pair = (index, key.clone());
        use OperationType::*;
        match operation_type {
            Read => self.keys_read.lock().as_mut().unwrap().push(pair),
            Update => self.keys_updated.lock().as_mut().unwrap().push(pair),
        }
    }

    /// Get the status of the `Node`.
    pub fn get_state(&self) -> State {
        let data = self.state.lock();
        data.as_ref().unwrap().clone()
    }

    /// Set the status of the `Node`.
    pub fn set_state(&self, state: State) {
        let mut data = self.state.lock();
        *data = Some(state);
    }

    /// Check if `Node` has any incoming edges.
    pub fn has_incoming(&self) -> bool {
        let data = self.incoming.lock();
        !data.as_ref().unwrap().is_empty()
    }

    /// Check if edge exists
    pub fn incoming_edge_from_exists(&self, id: (usize, u64)) -> bool {
        self.incoming.lock().as_ref().unwrap().contains(&id)
    }
}

impl fmt::Display for NodeSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.transactions)
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "id: {} | state: {} | incoming: {:?} | outgoing: {:?}",
            self.id,
            self.state.lock().as_ref().unwrap(),
            self.incoming.lock().as_ref().unwrap(),
            self.outgoing.lock().as_ref().unwrap(),
        )
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use State::*;
        match *self {
            Committed => write!(f, "committed"),
            Aborted => write!(f, "aborted"),
            Active => write!(f, "active"),
        }
    }
}

impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use OperationType::*;
        match *self {
            Update => write!(f, "update"),
            Read => write!(f, "read"),
        }
    }
}
