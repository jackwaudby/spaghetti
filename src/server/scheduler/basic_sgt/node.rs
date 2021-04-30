use crate::workloads::PrimaryKey;

use std::fmt;
use std::sync::Mutex;

/// A `Node` represents a transaction in the serialization graph.
///
/// At creation each node is assigned to a thread.
#[derive(Debug)]
pub struct Node {
    /// Node's position in the graph.
    id: usize,

    /// Transaction counter
    counter: Mutex<u64>,

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
            counter: Mutex::new(0),
            outgoing: Mutex::new(Some(vec![])),
            incoming: Mutex::new(Some(vec![])),
            state: Mutex::new(None),
            keys_inserted: Mutex::new(Some(vec![])),
            keys_read: Mutex::new(Some(vec![])),
            keys_updated: Mutex::new(Some(vec![])),
            keys_deleted: Mutex::new(Some(vec![])),
        }
    }

    /// Get transaction id
    pub fn get_transaction_id(&self) -> (usize, u64) {
        let thread_id = self.id;
        let transaction_id = *self.counter.lock().unwrap();
        (thread_id, transaction_id)
    }

    /// A transaction has terminated if the current transaction has a larger id.
    pub fn has_terminated(&self, id: u64) -> bool {
        *self.counter.lock().unwrap() > id
    }

    /// Reset the fields of a `Node`.
    pub fn reset(&self) {
        let mut counter = self.counter.lock().unwrap();
        let mut outgoing = self.outgoing.lock().unwrap();
        let mut incoming = self.incoming.lock().unwrap();
        let mut inserted = self.keys_inserted.lock().unwrap();
        let mut read = self.keys_read.lock().unwrap();
        let mut updated = self.keys_updated.lock().unwrap();
        let mut deleted = self.keys_deleted.lock().unwrap();

        *counter += 1;
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
    pub fn insert_edge(&self, id: usize, edge_type: EdgeType) {
        match edge_type {
            EdgeType::Incoming => {
                if let Some(incoming) = self.incoming.lock().unwrap().as_mut() {
                    if !incoming.contains(&id) {
                        incoming.push(id);
                    }
                }
            }
            EdgeType::Outgoing => {
                if let Some(outgoing) = self.outgoing.lock().unwrap().as_mut() {
                    if !outgoing.contains(&id) {
                        outgoing.push(id);
                    }
                }
            }
        }
    }

    /// Remove edge from `Node`.
    pub fn delete_edge(&self, id: usize, edge_type: EdgeType) {
        match edge_type {
            EdgeType::Incoming => {
                self.incoming
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .retain(|&x| x != id);
            }
            EdgeType::Outgoing => {
                self.outgoing
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .retain(|&x| x != id);
            }
        }
    }

    /// Clones the incoming edges from a `Node` leaving a `None`.
    pub fn get_incoming(&self) -> Vec<usize> {
        self.incoming.lock().unwrap().clone().unwrap()
    }

    /// Clones the outgoing edges from a `Node` leaving a `None`.
    pub fn get_outgoing(&self) -> Vec<usize> {
        self.outgoing.lock().unwrap().clone().unwrap()
    }

    /// Takes the list of keys inserted/read/updated/deleted by the transaction in the node.
    pub fn get_keys(&self, operation_type: OperationType) -> Vec<(String, PrimaryKey)> {
        use OperationType::*;
        match operation_type {
            Insert => {
                let mut data = self.keys_inserted.lock().unwrap();
                data.take().unwrap()
            }

            Read => self.keys_read.lock().unwrap().take().unwrap(),
            Update => self.keys_updated.lock().unwrap().take().unwrap(),
            Delete => self.keys_deleted.lock().unwrap().take().unwrap(),
        }
    }

    /// Add key to the list of keys inserted/read/updated/deleted by the transaction in the node.
    pub fn add_key(&self, index: &str, key: PrimaryKey, operation_type: OperationType) {
        let pair = (index.to_string(), key);
        use OperationType::*;
        match operation_type {
            Insert => self
                .keys_inserted
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .push(pair),
            Read => self.keys_read.lock().unwrap().as_mut().unwrap().push(pair),
            Update => self
                .keys_updated
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .push(pair),
            Delete => self
                .keys_deleted
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .push(pair),
        }
    }

    /// Get the status of the `Node`.
    pub fn get_state(&self) -> State {
        let data = self.state.lock().unwrap();
        data.as_ref().unwrap().clone()
    }

    /// Set the status of the `Node`.
    pub fn set_state(&self, state: State) {
        let mut data = self.state.lock().unwrap();
        *data = Some(state);
    }

    /// Check if `Node` has any incoming edges.
    pub fn has_incoming(&self) -> bool {
        let data = self.incoming.lock().unwrap();
        !data.as_ref().unwrap().is_empty()
    }
}

// impl fmt::Display for Node {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "\n id: {} | incoming: {:?} | outgoing: {:?}",
//             self.id,
//             self.incoming.lock().as_ref().unwrap(),
//             self.outgoing.lock().as_ref().unwrap()
//         )
//     }
// }

impl fmt::Display for OperationType {
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
        //    assert_eq!(n.has_incoming(), false, "{:?}", n);
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
