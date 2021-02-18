use crate::server::scheduler::serialization_graph_testing::error::{
    SerializationGraphTestingError, SerializationGraphTestingErrorKind,
};
use crate::server::scheduler::serialization_graph_testing::node::{EdgeType, Node, State};
use crate::server::scheduler::{Aborted, Scheduler};
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::row::{Access, Row};
use crate::server::storage::table::Table;
use crate::workloads::{PrimaryKey, Workload};

use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use tracing::{debug, info};

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
    fn register(&self, tid: &str) -> Result<(), Aborted> {
        // Find free node in graph.
        for node in &self.nodes {
            // Take exculsive access of node.
            let node = node.write().map_err(|_| {
                SerializationGraphTestingError::new(
                    SerializationGraphTestingErrorKind::RwLockFailed,
                )
            });
            match node {
                Ok(node) => {
                    if node.get_state().unwrap() == State::Free {
                        // Set to active.
                        node.set_state(State::Active).unwrap();
                        // Set transaction id.
                        node.set_transaction_id(tid).unwrap();
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(Aborted {
                        reason: format!("{}", e),
                    })
                }
            }
        }
        // No space in the graph.
        let err =
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::NoSpaceInGraph);
        Err(Aborted {
            reason: format!("{}", err),
        })
    }

    /// Insert row into table.
    fn create(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        tid: &str,
        _tts: DateTime<Utc>,
    ) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, tid)?;
        // Init row.
        let mut row = Row::new(Arc::clone(&table), "sgt");
        // Set pk.
        row.set_primary_key(key);
        // Init values.
        for (i, column) in columns.iter().enumerate() {
            match row.init_value(column, &values[i].to_string()) {
                Ok(_) => {}
                Err(e) => {
                    self.abort(tid).unwrap();
                    return Err(Aborted {
                        reason: format!("{}", e),
                    });
                }
            }
        }
        // Get Index
        let index = self.get_index(table, tid)?;

        // Set values - Needed to make the row "dirty"
        match row.set_values(columns, values, "sgt", tid) {
            Ok(_) => {}
            Err(e) => {
                self.abort(tid).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        }

        // Get position of this transaction in the graph.
        let this_node = self.get_node_position(tid).unwrap();
        // Record insert
        self.get_node(this_node)
            .read()
            .unwrap()
            .keys_inserted
            .lock()
            .unwrap()
            .push((index.to_string(), key));

        // Attempt to insert row.
        match index.insert(key, row) {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!("Abort transaction {:?}: {:?}", tid, e);
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Execute a read operation.
    ///
    /// Adds an edge in the graph for each WR conflict.
    fn read(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        tid: &str,
        _tts: DateTime<Utc>,
    ) -> Result<Vec<Data>, Aborted> {
        // Get table.
        let table = self.get_table(table, tid)?;

        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), tid)?;

        // Execute read.
        match index.read(key, columns, "sgt", tid) {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = self.get_node_position(tid).unwrap();
                // Detect conflicts and insert edges.
                for access in access_history {
                    // WR conflict
                    if let Access::Write(tid) = access {
                        // Get position of the the conflicting transaction in the graph.
                        let from_node = self.get_node_position(&tid).unwrap();
                        // Insert edges
                        self.add_edge(from_node, this_node, false).unwrap();
                    }
                }
                let this_node = self.get_node(this_node);
                // Record read of this key.
                this_node
                    .read()
                    .unwrap()
                    .keys_read
                    .lock()
                    .unwrap()
                    .push((index.to_string(), key));
                // Get values
                let vals = res.get_values().unwrap();
                Ok(vals)
            }
            Err(e) => {
                debug!("Abort transaction {:?}: {:?}", tid, e);
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Execute a write operation.
    ///
    /// Adds an edge in the graph for each WW and RW conflict.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        tid: &str,
        _tts: DateTime<Utc>,
    ) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, tid)?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), tid)?;

        // Execute write operation.
        match index.update(key, columns, values, "sgt", tid) {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = self.get_node_position(tid).unwrap();
                // Detect conflicts.
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            // Get position of conflicting transaction in the graph.
                            let from_node = self.get_node_position(&tid).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, false).unwrap();
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            // Get position of conflicting transaction in the graph.
                            let from_node = self.get_node_position(&tid).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, true).unwrap();
                        }
                    }
                }
                // Record write
                self.get_node(this_node)
                    .read()
                    .unwrap()
                    .keys_written
                    .lock()
                    .unwrap()
                    .push((index.to_string(), key));
                Ok(())
            }
            Err(e) => {
                debug!("Abort transaction {:?}: {:?}", tid, e);
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Delete from row.
    fn delete(
        &self,
        table: &str,
        key: PrimaryKey,
        tid: &str,
        _tts: DateTime<Utc>,
    ) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, tid)?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), tid)?;

        // Execute remove op.
        match index.delete(key, "sgt") {
            Ok(res) => {
                // Get the access history
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = self.get_node_position(tid).unwrap();
                // Detect conflicts.
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            // Find the node for conflicting transaction
                            let from_node = self.get_node_position(&tid).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, false).unwrap();
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            // Find the node for conflicting transaction
                            let from_node = self.get_node_position(&tid).unwrap();
                            // Insert edges
                            self.add_edge(from_node, this_node, true).unwrap();
                        }
                    }
                }
                // Record write
                self.get_node(this_node)
                    .read()
                    .unwrap()
                    .keys_written
                    .lock()
                    .unwrap()
                    .push((index.to_string(), key));
                Ok(())
            }
            Err(e) => {
                debug!("Abort transaction {:?}: {:?}", tid, e);
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Abort a transaction.
    fn abort(&self, tid: &str) -> crate::Result<()> {
        // Get position of this transaction in the graph.
        let this_node_id = self.get_node_position(tid).unwrap();

        // (1) Set state to abort.
        {
            self.get_node(this_node_id)
                .write()
                .unwrap()
                .set_state(State::Aborted)
                .unwrap();
            // Drop write lock on node.
        }
        // (2) Revert updates/deletes.
        let written = self
            .get_node(this_node_id)
            .read()
            .unwrap()
            .get_keys_written()
            .unwrap();
        for (index, key) in &written {
            // Get index.
            let index = self.data.get_internals().get_index(&index).unwrap();
            // Revert.
            index.revert(*key, "sgt", tid).unwrap();
        }
        // (3) Remove read accesses from rows read.
        let read = self
            .get_node(this_node_id)
            .read()
            .unwrap()
            .get_keys_read()
            .unwrap();
        for (index, key) in read {
            // Get index.
            let index = self.data.get_internals().get_index(&index).unwrap();
            // Revert.
            index.revert_read(key, tid).unwrap();
        }
        // (4) Remove inserted values.
        let inserted = self
            .get_node(this_node_id)
            .read()
            .unwrap()
            .get_rows_inserted()
            .unwrap();
        for (index, key) in inserted {
            // Get index.
            let index = self.data.get_internals().get_index(&index).unwrap();
            // Revert.
            index.remove(key).unwrap();
        }

        // (5) Abort children.
        // Get outgoing node.
        let outgoing_nodes = self
            .get_node(this_node_id)
            .read()
            .unwrap()
            .get_outgoing()
            .unwrap();

        for out in outgoing_nodes {
            let this_node = self.get_node(this_node_id).read().unwrap();
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

        // (6) While there are incoming edges wait.
        while self
            .get_node(this_node_id)
            .read()
            .unwrap()
            .has_incoming()
            .unwrap()
        {
            // wait
        }

        // (7) Reset node information.
        self.get_node(this_node_id).write().unwrap().reset();
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, tid: &str) -> Result<(), Aborted> {
        // Get position of this transaction in the graph.
        let this_node_id = self.get_node_position(tid).unwrap();
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
                self.abort(tid).unwrap();
                let e = Box::new(SerializationGraphTestingError::new(
                    SerializationGraphTestingErrorKind::SerializableError,
                ));
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
            State::Committed => {
                self.clean_up_graph(this_node_id);
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
    pub fn get_node_position(&self, transaction_id: &str) -> crate::Result<usize> {
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
    pub fn add_edge(&self, from_node: usize, this_node: usize, rw_edge: bool) -> crate::Result<()> {
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

    pub fn reduced_depth_first_search(&self, current_node: usize) -> crate::Result<bool> {
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
    }

    // TODO: clean up database.
    /// Get shared reference to a table.
    fn get_table(&self, table: &str, tid: &str) -> Result<Arc<Table>, Aborted> {
        // Get table.
        let res = self.data.get_internals().get_table(table);
        match res {
            Ok(table) => Ok(table),
            Err(e) => {
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get primary index name on a table.
    fn get_index_name(&self, table: Arc<Table>, tid: &str) -> Result<String, Aborted> {
        let res = table.get_primary_index();
        match res {
            Ok(index_name) => Ok(index_name),
            Err(e) => {
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get shared reference to index for a table.
    fn get_index(&self, table: Arc<Table>, tid: &str) -> Result<Arc<Index>, Aborted> {
        // Get index name.
        let index_name = self.get_index_name(table, tid)?;

        // Get index for this key's table.
        let res = self.data.get_internals().get_index(&index_name);
        match res {
            Ok(index) => Ok(index),
            Err(e) => {
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     // use crate::server::scheduler::Protocol;
//     // use crate::server::storage::datatype;
//     use crate::workloads::tatp;
//     // use crate::workloads::tatp::keys::TatpPrimaryKey;
//     use crate::workloads::Internal;
//     // use chrono::Duration;
//     use config::Config;
//     use lazy_static::lazy_static;
//     use rand::rngs::StdRng;
//     use rand::SeedableRng;
//     use std::sync::Once;
//     // use std::thread;
//     // use std::time;
//     // use std::time::SystemTime;
//     use tracing::Level;s
//     use tracing_subscriber::FmtSubscriber;

//     static LOG: Once = Once::new();

//     fn logging(on: bool) {
//         if on {
//             LOG.call_once(|| {
//                 let subscriber = FmtSubscriber::builder()
//                     .with_max_level(Level::DEBUG)
//                     .finish();
//                 tracing::subscriber::set_global_default(subscriber)
//                     .expect("setting default subscriber failed");
//             });
//         }
//     }

//     lazy_static! {
//        static ref WORKLOAD: Arc<Workload> = {
//             // Initialise configuration.
//             let mut c = Config::default();
//             // Load from test file.
//             c.merge(config::File::with_name("Sgt.toml")).unwrap();
//             let config = Arc::new(c);
//            // Rng with fixed seed.
//            let mut rng = StdRng::seed_from_u64(42);
//            // Initialise internals.
//            let internals = Internal::new("tatp_schema.txt", config).unwrap();
//            // Load tables.
//            tatp::loader::populate_tables(&internals, &mut rng).unwrap();
//            // Create workload.
//            let workload = Arc::new(Workload::Tatp(internals));
//            workload
//         };
//     }

//     #[test]
//     fn sgt_test() {
//         logging(false);
//         // Initialise scheduler.
//         let sgt = SerializationGraphTesting::new(3, Arc::clone(&WORKLOAD));

//         // Register.
//         assert_eq!(sgt.nodes.len(), 3);
//         assert_eq!(sgt.register("t1").unwrap(), ());
//         assert_eq!(sgt.register("t2").unwrap(), ());
//         assert_eq!(sgt.register("t3").unwrap(), ());
//         assert_eq!(
//             format!("{}", sgt.register("t4").unwrap_err()),
//             format!("no nodes free in graph")
//         );

//         // Get index.
//         let _sub_idx = WORKLOAD.get_internals().get_index("sub_idx").unwrap();

//         // Values to read/write.
//         let _cols = vec!["bit_4", "byte_2_5"];
//         let _vals = vec!["0", "69"];

//         // Read
//     }
// }
