use crate::server::scheduler::serialization_graph_testing::error::{
    SerializationGraphTestingError, SerializationGraphTestingErrorKind,
};
use crate::server::scheduler::serialization_graph_testing::node::{EdgeType, Node, State};
use crate::server::scheduler::Scheduler;
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Access;
use crate::server::storage::row::Row;
use crate::workloads::{PrimaryKey, Workload};
use crate::Result;

use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use tracing::info;

pub mod node;

pub mod error;

/// Serialization Graph.
///
/// Contains a fixed number of `Node`s each wrapped in a `RwLock`.
///
/// # Safety
/// Graph can have multiple owners and shared across thread boundaries.
/// Each `Node` is wrapped in a `RwLock`.
#[derive(Debug)]
pub struct SerializationGraphTesting {
    nodes: Vec<RwLock<Node>>,
    /// Handle to storage layer.
    pub data: Arc<Workload>,
}

impl Scheduler for SerializationGraphTesting {
    /// Register a transaction with the serialization graph.
    fn register(&self, transaction_name: &str) -> Result<()> {
        // Find free node in graph.
        for node in &self.nodes {
            // Take exculsive access of node.
            let node = node.write().map_err(|_| {
                SerializationGraphTestingError::new(
                    SerializationGraphTestingErrorKind::RwLockFailed,
                )
            })?;
            if node.get_state()? == State::Free {
                // Set to active.
                node.set_state(State::Active)?;
                // Set transaction id.
                node.set_transaction_id(transaction_name)?;
                return Ok(());
            } else {
                continue;
            }
        }
        Err(Box::new(SerializationGraphTestingError::new(
            SerializationGraphTestingErrorKind::NoSpaceInGraph,
        )))
    }

    fn read(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        transaction_name: &str,
        _transaction_ts: DateTime<Utc>,
    ) -> Result<Vec<Data>> {
        // Get index.
        let index = self.data.get_internals().indexes.get(index).unwrap();
        // Do read
        match index.index_read(key, columns, "sgt", transaction_name) {
            Ok(op_res) => {
                // Detect conflicts.
                let access_history = op_res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = self.get_node_position(transaction_name).unwrap();
                for access in access_history {
                    if let Access::Write(transaction_id) = access {
                        // Find the node for conflicting transaction
                        let from_node = self.get_node_position(&transaction_id).unwrap();
                        // Insert edges
                        self.add_edge(from_node, this_node, false)?;
                    }
                }
                return Ok(op_res.get_values().unwrap());
            }
            Err(e) => {
                // TODO: debug!("Abort transaction {:?}: {:?}", transaction_name, e);
                // TODO: self.cleanup(transaction_name);
                return Err(e);
            }
        };
    }

    fn write(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        transaction_name: &str,
        _transaction_ts: DateTime<Utc>,
    ) -> Result<()> {
        // Get index.
        let index = self.data.get_internals().indexes.get(index).unwrap();
        // Do write
        match index.index_write(key, columns, values, "sgt", transaction_name) {
            Ok(op_res) => {
                // Detect conflicts.
                let access_history = op_res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = self.get_node_position(transaction_name).unwrap();
                for access in access_history {
                    match access {
                        Access::Write(transaction_id) => {
                            // Find the node for conflicting transaction
                            let from_node = self.get_node_position(&transaction_id).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, false)?;
                        }
                        Access::Read(transaction_id) => {
                            // Find the node for conflicting transaction
                            let from_node = self.get_node_position(&transaction_id).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, true)?;
                        }
                    }
                }
                return Ok(());
            }
            Err(e) => {
                // TODO: debug!("Abort transaction {:?}: {:?}", transaction_name, e);
                // TODO: self.cleanup(transaction_name);
                return Err(e);
            }
        };
    }

    fn insert(
        &self,
        table: &str,
        pk: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        _transaction_name: &str,
    ) -> Result<()> {
        // Initialise empty row.
        let table = self.data.get_internals().get_table(table)?;
        let protocol = self.data.get_internals().config.get_str("protocol")?;

        let mut row = Row::new(Arc::clone(&table), &protocol);
        // Set PK.
        row.set_primary_key(pk);
        // Set values.
        for (i, column) in columns.iter().enumerate() {
            row.set_value(column, &values[i].to_string())?;
        }
        // Get index.
        let index = table.get_primary_index()?;
        let index = self.data.get_internals().indexes.get(&index[..]).unwrap();
        // Attempt to insert row.
        match index.index_insert(pk, row) {
            Ok(_) => Ok(()),
            Err(e) => {
                // TODO:                self.cleanup(transaction_name);
                Err(e)
            }
        }
    }

    fn delete(&self, index: &str, pk: PrimaryKey, transaction_name: &str) -> Result<()> {
        // Get index.
        let index = self.data.get_internals().indexes.get(index).unwrap();
        // Do write
        match index.index_remove(pk, "sgt") {
            Ok(op_res) => {
                // Detect conflicts.
                let access_history = op_res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = self.get_node_position(transaction_name).unwrap();
                for access in access_history {
                    match access {
                        Access::Write(transaction_id) => {
                            // Find the node for conflicting transaction
                            let from_node = self.get_node_position(&transaction_id).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, false)?;
                        }
                        Access::Read(transaction_id) => {
                            // Find the node for conflicting transaction
                            let from_node = self.get_node_position(&transaction_id).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, true)?;
                        }
                    }
                }
                return Ok(());
            }
            Err(e) => {
                // TODO: debug!("Abort transaction {:?}: {:?}", transaction_name, e);
                // TODO: self.cleanup(transaction_name);
                return Err(e);
            }
        };
    }

    fn commit(&self, transaction_name: &str) -> Result<()> {
        // Get position of this transaction in the graph.
        let this_node_id = self.get_node_position(transaction_name).unwrap();
        // Take exculsive lock
        let this_node = self.nodes[this_node_id].write().unwrap();

        while let State::Active = this_node.get_state().unwrap() {
            // Get incoming
            let incoming = this_node.has_incoming().unwrap();
            if incoming {
                // Do cycle check.
                let is_cycle = self.reduced_depth_first_search(this_node_id).unwrap();
                if is_cycle {
                    this_node.set_state(State::Aborted).unwrap();
                }
            } else {
                // Set status to commit
                this_node.set_state(State::Committed).unwrap();
                break;
            }
        }

        match this_node.get_state().unwrap() {
            State::Aborted => {
                // TODO: clean up.
                return Err(Box::new(SerializationGraphTestingError::new(
                    SerializationGraphTestingErrorKind::SerializableError,
                )));
            }
            State::Committed => {
                // TODO: CLEAN UP

                return Ok(());
            }
            State::Free => panic!("trying to commit a free sloted"),

            State::Active => panic!("node should not be active"),
        }
    }
}

impl SerializationGraphTesting {
    /// Initialise serialization graph with `size` nodes.
    pub fn new(size: i32, workload: Arc<Workload>) -> Self {
        info!("initialise serialization graph with {} nodes", size);
        let mut nodes = vec![];
        for i in 0..size {
            let node = RwLock::new(Node::new(i as usize));
            nodes.push(node);
        }
        SerializationGraphTesting {
            nodes,
            data: workload,
        }
    }

    /// Get node from the serialization graph
    pub fn get_node(&self, id: usize) -> &RwLock<Node> {
        &self.nodes[id]
    }

    /// Get node position by transaction id.
    pub fn get_node_position(&self, transaction_id: &str) -> Result<usize> {
        for (pos, node) in self.nodes.iter().enumerate() {
            // Take shared access of node.
            let tid = node.read().unwrap().get_transaction_id().unwrap();
            if tid == transaction_id.to_string() {
                return Ok(pos);
            }
        }
        panic!("No node found with this transaction id");
    }

    /// Insert an edge into the serialization graph
    ///
    /// `(from_node)--->(this_node)`
    pub fn add_edge(&self, from_node: usize, this_node: usize, rw_edge: bool) -> Result<()> {
        // Acquire read locks on nodes.
        let this_node = self.nodes[this_node].read().map_err(|_| {
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::RwLockFailed)
        })?;
        let from_node = self.nodes[from_node].read().map_err(|_| {
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::RwLockFailed)
        })?;

        let from_node_state = from_node.get_state()?;

        match from_node_state {
            State::Aborted => match rw_edge {
                false => {
                    info!("from_node already aborted, edge is ww or wr so abort this_node");
                    return Err(Box::new(SerializationGraphTestingError::new(
                        SerializationGraphTestingErrorKind::SerializableError,
                    )));
                }
                true => {
                    info!("from_node already aborted, edge is rw so skip add edge");
                    return Ok(());
                }
            },
            State::Committed => {
                info!("skip add edge, from_node already committed");
                return Ok(());
            }
            State::Active => {
                if from_node.id != this_node.id {
                    info!("add ({})->({})", from_node.id, this_node.id);
                } else {
                    info!("self edge, so skip add edge");
                }
                from_node.insert_edge(this_node.id, EdgeType::Outgoing)?;
                this_node.insert_edge(from_node.id, EdgeType::Incoming)?;
                Ok(())
            }
            State::Free => panic!("node should not be free {:#?}, {:#?}", this_node, from_node),
        }
    }

    pub fn reduced_depth_first_search(&self, current_node: usize) -> Result<bool> {
        // Tracking nodes to visit.
        let mut stack = Vec::new();
        // Tracking visited nodes.
        let mut visited = HashSet::new();
        {
            // Get read lock on current_node.
            let current_node = self.nodes[current_node].read().unwrap();
            // Push children to stack.
            stack.append(&mut current_node.get_outgoing().unwrap());
            // Mark as visited.
            visited.insert(current_node.id);
        }
        while let Some(current_node_id) = stack.pop() {
            // Mark as visited.
            visited.insert(current_node_id);
            // Get read lock on current_node.
            let current_node = self.nodes[current_node_id as usize].read().unwrap();
            if current_node.get_state().unwrap() == State::Active {
                for child in current_node.get_outgoing().unwrap() {
                    if visited.contains(&child) {
                        // return early
                        return Ok(true);
                    } else {
                        // Get read lock on child_node.
                        let c = self.nodes[child as usize].read().unwrap();
                        stack.append(&mut c.get_outgoing().unwrap());
                    }
                }
            }
        }
        Ok(false)
    }

    fn clean_up_graph(&self, this_node_id: usize) {
        // Take shared lock.
        let this_node = self.nodes[this_node_id].read().unwrap();
        // Get state.
        let state = this_node.get_state().unwrap();
        match state {
            State::Committed => {
                // Get outgoing edges
                let outgoing_nodes = this_node.get_outgoing().unwrap();
                for out in outgoing_nodes {
                    // Get read lock on outgoing.
                    let outgoing_node = self.get_node(out).read().unwrap();
                    // Delete from edge sets.
                    outgoing_node
                        .delete_edge(this_node_id, EdgeType::Incoming)
                        .unwrap();
                    this_node
                        .delete_edge(outgoing_node.id, EdgeType::Outgoing)
                        .unwrap();
                }
            }
            State::Aborted => {
                // Get outgoing edges
                let outgoing_nodes = this_node.get_outgoing().unwrap();
                for out in outgoing_nodes {
                    let outgoing_node = self.get_node(out).read().unwrap();
                    // Abort children.
                    if outgoing_node.get_state().unwrap() == State::Active {
                        outgoing_node.set_state(State::Aborted).unwrap();
                    }
                    // Delete from edge sets
                    outgoing_node
                        .delete_edge(this_node_id, EdgeType::Incoming)
                        .unwrap();
                    this_node
                        .delete_edge(outgoing_node.id, EdgeType::Outgoing)
                        .unwrap();
                }
            }
            _ => panic!("Node should not be in this state"),
        }

        // tx.send(this_node).unwrap();
        // send id to garbage collector
    }

    // TODO: clean up database.
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::server::scheduler::Protocol;
    use chrono::Duration;
    use config::Config;
    use lazy_static::lazy_static;
    use std::sync::Once;
    use std::thread;
    use std::time;
    use std::time::SystemTime;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    static LOG: Once = Once::new();

    fn logging(on: bool) {
        if on {
            LOG.call_once(|| {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting default subscriber failed");
            });
        }
    }

    lazy_static! {
        static ref WORKLOAD: Arc<Workload> = {
            // Initialise configuration.
            let mut c = Config::default();
            c.merge(config::File::with_name("Test.toml")).unwrap();
            let config = Arc::new(c);
            // Initalise workload.
            let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());
            workload
        };
    }

    #[test]
    fn sgt_test() {
        logging(true);
        // Initialise scheduler.
        let sgt = SerializationGraphTesting::new(3, Arc::clone(&WORKLOAD));

        // Register.
        assert_eq!(sgt.nodes.len(), 3);
        assert_eq!(sgt.register("t1").unwrap(), ());
        assert_eq!(sgt.register("t2").unwrap(), ());
        assert_eq!(sgt.register("t3").unwrap(), ());
        assert_eq!(
            format!("{}", sgt.register("t4").unwrap_err()),
            format!("no nodes free in graph")
        );

        // Check status.
        for node in &sgt.nodes {
            assert_eq!(node.read().unwrap().get_state().unwrap(), State::Active);
        }

        // Add some edge.
        assert_eq!(sgt.add_edge(1, 2, true).unwrap(), ());
        assert_eq!(
            format!("{}", sgt.add_edge(1, 2, false).unwrap_err()),
            format!("edge already exists between two nodes")
        );

        // Change state.
        assert_eq!(
            sgt.get_node(2).read().unwrap().get_state().unwrap(),
            State::Active
        );
        assert_eq!(
            sgt.get_node(2)
                .write()
                .unwrap()
                .set_state(State::Aborted)
                .unwrap(),
            ()
        );
        assert_eq!(
            sgt.get_node(2).read().unwrap().get_state().unwrap(),
            State::Aborted
        );
    }
}
