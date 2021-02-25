use crate::server::scheduler::serialization_graph_testing::error::SerializationGraphTestingError;
use crate::workloads::PrimaryKey;

use std::sync::Mutex;

/// A `Node` represents a transaction in the serialization graph.
///
/// # Safety
/// status, outgoing, and incoming are wrapped in a mutex as multiple threads that hold read locks can concurrently modify these fields.
#[derive(Debug)]
pub struct Node {
    /// Node's position in the graph.
    id: usize,

    /// Node status
    state: Mutex<State>,

    /// List of outgoing edges.
    outgoing: Mutex<Vec<usize>>, // (this_node)->(other_node)

    /// List of incoming edges.
    incoming: Mutex<Vec<usize>>, // (other_node)->(this_node)

    /// List of keys written by transaction.
    pub keys_written: Mutex<Vec<(String, PrimaryKey)>>,

    /// List of keys read by transaction.
    pub keys_read: Mutex<Vec<(String, PrimaryKey)>>,

    /// Keys inserted
    pub keys_inserted: Mutex<Vec<(String, PrimaryKey)>>,
}

impl Node {
    /// Create a new `Node`.
    pub fn new(id: usize) -> Node {
        Node {
            id,
            outgoing: Mutex::new(vec![]),
            incoming: Mutex::new(vec![]),
            state: Mutex::new(State::Free),
            keys_inserted: Mutex::new(vec![]),
            keys_read: Mutex::new(vec![]),
            keys_written: Mutex::new(vec![]),
        }
    }

    /// Reset the fields of a `Node`.
    pub fn reset(&self) {
        *self.outgoing.lock().unwrap() = vec![];
        *self.incoming.lock().unwrap() = vec![];
        // *self.state.lock().unwrap() = State::Free;
        *self.keys_written.lock().unwrap() = vec![];
        *self.keys_read.lock().unwrap() = vec![];
        *self.keys_inserted.lock().unwrap() = vec![];
    }

    /// Insert edge into a `Node`.
    ///
    /// Does nothing if edge already exists.
    ///
    /// # Errors
    /// - Mutex lock failed.
    pub fn insert_edge(
        &self,
        id: usize,
        edge_type: EdgeType,
    ) -> Result<(), SerializationGraphTestingError> {
        match edge_type {
            EdgeType::Incoming => {
                let mut incoming = self
                    .incoming
                    .lock()
                    .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
                if !incoming.contains(&id) {
                    incoming.push(id);
                }
            }
            EdgeType::Outgoing => {
                let mut outgoing = self
                    .outgoing
                    .lock()
                    .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
                if !outgoing.contains(&id) {
                    outgoing.push(id);
                }
            }
        }
        Ok(())
    }

    /// Delete edge from a `Node`.
    ///
    /// # Errors
    /// - Lock error
    pub fn delete_edge(
        &self,
        id: usize,
        edge_type: EdgeType,
    ) -> Result<(), SerializationGraphTestingError> {
        match edge_type {
            EdgeType::Incoming => {
                let mut incoming = self
                    .incoming
                    .lock()
                    .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
                incoming.retain(|&x| x != id);
            }
            EdgeType::Outgoing => {
                let mut outgoing = self
                    .outgoing
                    .lock()
                    .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
                outgoing.retain(|&x| x != id);
            }
        }
        Ok(())
    }

    /// Get the incoming edges from a `Node`.
    ///
    /// Clones the list behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_incoming(&self) -> Result<Vec<usize>, SerializationGraphTestingError> {
        let incoming = self
            .incoming
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
        Ok(incoming.clone())
    }

    /// Get the outgoing edges from a `Node`.
    ///
    /// Clones the list behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_outgoing(&self) -> Result<Vec<usize>, SerializationGraphTestingError> {
        let outgoing = self
            .outgoing
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
        Ok(outgoing.clone())
    }

    /// Get the list of keys written (updated/deleted) by this transaction.
    ///
    /// Clones the list behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_keys_written(
        &self,
    ) -> Result<Vec<(String, PrimaryKey)>, SerializationGraphTestingError> {
        let written = self
            .keys_written
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
        Ok(written.clone())
    }

    /// Get the list of keys read by this transaction.
    ///
    /// Clones the list behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_keys_read(
        &self,
    ) -> Result<Vec<(String, PrimaryKey)>, SerializationGraphTestingError> {
        let read = self
            .keys_read
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
        Ok(read.clone())
    }

    /// Get the list of keys inserted by this transaction.
    ///
    /// Clones the list behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_rows_inserted(
        &self,
    ) -> Result<Vec<(String, PrimaryKey)>, SerializationGraphTestingError> {
        let inserted = self
            .keys_inserted
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
        Ok(inserted.clone())
    }

    /// Get the status of the `Node`.
    ///
    /// Currently this just clones the value behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_state(&self) -> Result<State, SerializationGraphTestingError> {
        let mg = self
            .state
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
        Ok(mg.clone())
    }

    /// Set the status of the `Node`.
    ///
    /// # Errors
    /// - Lock error
    pub fn set_state(&self, state: State) -> Result<(), SerializationGraphTestingError> {
        let mut mg = self
            .state
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?;
        *mg = state;
        Ok(())
    }

    /// Check if `Node` has any incoming edges.
    ///
    /// # Errors
    /// - Lock error
    pub fn has_incoming(&self) -> Result<bool, SerializationGraphTestingError> {
        Ok(!self
            .incoming
            .lock()
            .map_err(|_| SerializationGraphTestingError::MutexLockFailed)?
            .is_empty())
    }
}

/// Type of edge.
pub enum EdgeType {
    Incoming,
    Outgoing,
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
    /// Node has no transaction.
    Free,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn node_test() {
        // Create node.
        let n = Node::new(0);

        // Insert edge.
        assert_eq!(n.insert_edge(1, EdgeType::Outgoing).unwrap(), ());
        assert_eq!(n.insert_edge(2, EdgeType::Outgoing).unwrap(), ());

        // Get outgoing.
        assert_eq!(n.get_outgoing().unwrap(), vec![1, 2]);
        assert_eq!(n.has_incoming().unwrap(), false);

        // Incoming.
        assert_eq!(n.insert_edge(2, EdgeType::Incoming).unwrap(), ());

        assert_eq!(n.has_incoming().unwrap(), true);
        assert_eq!(n.delete_edge(2, EdgeType::Incoming).unwrap(), ());
        assert_eq!(n.has_incoming().unwrap(), false);

        // Delete edge.
        assert_eq!(n.delete_edge(1, EdgeType::Outgoing).unwrap(), ());

        // Node state.
        assert_eq!(n.get_state().unwrap(), State::Free);
        n.set_state(State::Active).unwrap();
        assert_eq!(n.get_state().unwrap(), State::Active);
        n.set_state(State::Committed).unwrap();
        assert_eq!(n.get_state().unwrap(), State::Committed);
        n.set_state(State::Aborted).unwrap();
        assert_eq!(n.get_state().unwrap(), State::Aborted);
    }
}
