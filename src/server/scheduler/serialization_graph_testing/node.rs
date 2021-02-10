use crate::server::scheduler::serialization_graph_testing::error::{
    SerializationGraphTestingError, SerializationGraphTestingErrorKind,
};
use crate::Result;

use std::sync::Mutex;

/// A `Node` represents a transaction in the serialization graph.
///
/// # Safety
/// status, outgoing, and incoming are wrapped in a mutex as multiple threads that hold read locks can concurrently modify these fields.
#[derive(Debug)]
pub struct Node {
    /// Node's position in the graph.
    pub id: usize,

    /// ID of transaction currently residing in the node.
    pub transaction_id: Mutex<Option<String>>,

    /// Node status
    pub state: Mutex<State>,

    /// List of outgoing edges.
    pub outgoing: Mutex<Vec<usize>>, // (this_node)->(other_node)

    /// List of incoming edges.
    pub incoming: Mutex<Vec<usize>>, // (other_node)->(this_node)
}

impl Node {
    /// Create a new `Node`
    pub fn new(id: usize) -> Node {
        Node {
            id,
            transaction_id: Mutex::new(None),
            outgoing: Mutex::new(vec![]),
            incoming: Mutex::new(vec![]),
            state: Mutex::new(State::Free),
        }
    }

    /// Set transaction id of `Node`.
    ///
    /// # Errors
    /// - Transaction id already set.
    pub fn set_transaction_id(&self, tid: &str) -> Result<()> {
        let mut mg = self.transaction_id.lock().map_err(|_| {
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::MutexLockFailed)
        })?;

        match &*mg {
            Some(_) => {
                return Err(Box::new(SerializationGraphTestingError::new(
                    SerializationGraphTestingErrorKind::TransactionIdAlreadySet,
                )))
            }
            None => {
                *mg = Some(tid.to_string());
            }
        }
        Ok(())
    }

    pub fn get_transaction_id(&self) -> Result<String> {
        let tid = self
            .transaction_id
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        Ok(tid)
        // TODO: error handling.
    }

    /// Insert edge into a `Node`.
    ///
    /// # Errors
    /// - Attempting to insert a self edge.
    /// - Edge already exists between two nodes.
    pub fn insert_edge(&self, id: usize, edge_type: EdgeType) -> Result<()> {
        // check for self edges, these are not added to the graph, but does not result in an error
        if id == self.id {
            return Err(SerializationGraphTestingError::new(
                SerializationGraphTestingErrorKind::SelfEdge,
            )
            .into());
        }

        match edge_type {
            EdgeType::Incoming => {
                let mut incoming = self.incoming.lock().map_err(|_| {
                    SerializationGraphTestingError::new(
                        SerializationGraphTestingErrorKind::MutexLockFailed,
                    )
                })?;
                if !incoming.contains(&id) {
                    incoming.push(id);
                } else {
                    return Err(SerializationGraphTestingError::new(
                        SerializationGraphTestingErrorKind::EdgeExists,
                    )
                    .into());
                }
            }
            EdgeType::Outgoing => {
                let mut outgoing = self.outgoing.lock().unwrap();
                if !outgoing.contains(&id) {
                    outgoing.push(id);
                } else {
                    return Err(Box::new(SerializationGraphTestingError::new(
                        SerializationGraphTestingErrorKind::EdgeExists,
                    )));
                }
            }
        }
        Ok(())
    }

    /// Delete edge from a `Node`.
    ///
    /// # Errors
    /// - Lock error
    pub fn delete_edge(&self, id: usize, edge_type: EdgeType) -> Result<()> {
        match edge_type {
            EdgeType::Incoming => {
                let mut incoming = self.incoming.lock().map_err(|_| {
                    SerializationGraphTestingError::new(
                        SerializationGraphTestingErrorKind::MutexLockFailed,
                    )
                })?;
                incoming.retain(|&x| x != id);
            }
            EdgeType::Outgoing => {
                let mut outgoing = self.outgoing.lock().map_err(|_| {
                    SerializationGraphTestingError::new(
                        SerializationGraphTestingErrorKind::MutexLockFailed,
                    )
                })?;
                outgoing.retain(|&x| x != id);
            }
        }
        Ok(())
    }

    /// Get the outgoing edges from a `Node`
    ///
    /// Currently this just clones the list behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_outgoing(&self) -> Result<Vec<usize>> {
        let outgoing = self.outgoing.lock().map_err(|_| {
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::MutexLockFailed)
        })?;
        Ok(outgoing.clone())
    }

    /// Get the status of the `Node`.
    ///
    /// Currently this just clones the value behind the mutex.
    ///
    /// # Errors
    /// - Lock error
    pub fn get_state(&self) -> Result<State> {
        let mg = self.state.lock().map_err(|_| {
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::MutexLockFailed)
        })?;
        Ok(mg.clone())
    }

    /// Set the status of the `Node`.
    ///
    /// # Errors
    /// - Lock error
    pub fn set_state(&self, state: State) -> Result<()> {
        let mut mg = self.state.lock().map_err(|_| {
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::MutexLockFailed)
        })?;
        *mg = state;
        Ok(())
    }

    /// Check if `Node` has any incoming edges.
    ///
    /// # Errors
    /// - Lock error
    pub fn has_incoming(&self) -> Result<bool> {
        Ok(!self
            .incoming
            .lock()
            .map_err(|_| {
                SerializationGraphTestingError::new(
                    SerializationGraphTestingErrorKind::MutexLockFailed,
                )
            })?
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
        let n = Node::new(0);
        n.insert_edge(1, EdgeType::Outgoing).unwrap();
        n.insert_edge(2, EdgeType::Outgoing).unwrap();
        assert_eq!(
            format!("{}", n.insert_edge(1, EdgeType::Outgoing).unwrap_err()),
            format!("edge already exists between two nodes")
        );
        assert_eq!(
            format!("{}", n.insert_edge(0, EdgeType::Outgoing).unwrap_err()),
            format!("attempted to insert self edge")
        );

        assert_eq!(n.get_outgoing().unwrap(), vec![1, 2]);
        assert_eq!(n.has_incoming().unwrap(), false);

        n.insert_edge(2, EdgeType::Incoming).unwrap();
        assert_eq!(
            format!("{}", n.insert_edge(2, EdgeType::Incoming).unwrap_err()),
            format!("edge already exists between two nodes")
        );

        assert_eq!(n.has_incoming().unwrap(), true);
        n.delete_edge(2, EdgeType::Incoming).unwrap();
        assert_eq!(n.has_incoming().unwrap(), false);
        assert_eq!(n.get_state().unwrap(), State::Free);
        n.set_state(State::Active).unwrap();
        assert_eq!(n.get_state().unwrap(), State::Active);
        n.set_state(State::Committed).unwrap();
        assert_eq!(n.get_state().unwrap(), State::Committed);
        n.set_state(State::Aborted).unwrap();
        assert_eq!(n.get_state().unwrap(), State::Aborted);
    }
}
