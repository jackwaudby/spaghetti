use crate::server::scheduler::serialization_graph_testing::error::{
    SerializationGraphTestingError, SerializationGraphTestingErrorKind,
};
use crate::server::scheduler::serialization_graph_testing::node::{EdgeType, Node, State};
use crate::server::scheduler::{Aborted, Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::row::{Access, Row};
use crate::server::storage::table::Table;
use crate::workloads::{PrimaryKey, Workload};

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::thread;
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
    data: Arc<Workload>,
}

impl Scheduler for SerializationGraphTesting {
    /// Register a transaction with the serialization graph.
    ///
    /// Transaction gets the ID of the thread it is executed on.
    fn register(&self) -> Result<TransactionInfo, Aborted> {
        // Get thread name.
        let tname = thread::current().name().unwrap().to_string();
        debug!("Thread name {}", tname);
        let node_id = tname.parse::<usize>().unwrap();
        debug!("On thread {}", tname);
        let node = self.get_node(node_id).write().unwrap();
        // Set state to active.
        node.set_state(State::Active).unwrap();
        // Set transaction id.
        node.set_transaction_id(&node_id.to_string()).unwrap();
        Ok(TransactionInfo::new(Some(node_id.to_string()), None))
    }

    /// Insert row into table.
    fn create(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Init row.
        let mut row = Row::new(Arc::clone(&table), "sgt");
        // Set pk.
        row.set_primary_key(key);
        // Init values.
        for (i, column) in columns.iter().enumerate() {
            match row.init_value(column, &values[i].to_string()) {
                Ok(_) => {}
                Err(e) => {
                    self.abort(meta.clone()).unwrap();
                    return Err(Aborted {
                        reason: format!("{}", e),
                    });
                }
            }
        }
        // Get Index
        let index = self.get_index(table, meta.clone())?;

        // Set values - Needed to make the row "dirty"
        match row.set_values(columns, values, "sgt", &meta.get_id().unwrap()) {
            Ok(_) => {}
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        }

        // Get position of this transaction in the graph.
        let tid = meta.get_id().unwrap();
        let this_node = tid.parse::<usize>().unwrap();
        // Record insert - used if transaction is aborted.
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
                self.abort(meta.clone()).unwrap();
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
        meta: TransactionInfo,
    ) -> Result<Vec<Data>, Aborted> {
        debug!("Execute read operation");
        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), meta.clone())?;
        // Execute read.
        match index.read(key, columns, "sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let tid = meta.get_id().unwrap();
                let this_node = tid.parse::<usize>().unwrap();
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
                // Record read of this key - used for abort.
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
                debug!("Abort transaction {:?}: {:?}", meta.get_id().unwrap(), e);
                self.abort(meta.clone()).unwrap();
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
        meta: TransactionInfo,
    ) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Execute write operation.
        match index.update(key, columns, values, "sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let tid = meta.get_id().unwrap();
                let this_node = tid.parse::<usize>().unwrap();
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
                debug!("Abort transaction {:?}: {:?}", meta.get_id().unwrap(), e);
                self.abort(meta).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Delete from row.
    fn delete(&self, table: &str, key: PrimaryKey, meta: TransactionInfo) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Execute remove op.
        match index.delete(key, "sgt") {
            Ok(res) => {
                // Get the access history
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let tid = meta.get_id().unwrap();
                let this_node = tid.parse::<usize>().unwrap();
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
                debug!("Abort transaction {:?}: {:?}", meta.get_id().unwrap(), e);
                self.abort(meta).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Abort a transaction.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        // Get position of this transaction in the graph.
        let tid = meta.get_id().unwrap();
        let this_node_id = tid.parse::<usize>().unwrap();

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
            index.revert(*key, "sgt", &tid).unwrap();
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
            index.revert_read(key, &tid).unwrap();
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
    fn commit(&self, meta: TransactionInfo) -> Result<(), Aborted> {
        // Get position of this transaction in the graph.
        let tid = meta.get_id().unwrap();
        let this_node_id = tid.parse::<usize>().unwrap();
        debug!("Attempting to commit node: {}", this_node_id);

        // Take exculsive lock
        {
            let this_node = self.get_node(this_node_id).write().unwrap();
            while let State::Active = this_node.get_state().unwrap() {
                // Get incoming
                let incoming = this_node.has_incoming().unwrap();
                if incoming {
                    debug!("Execute cycle check");
                    // Do cycle check.
                    let is_cycle = self.reduced_depth_first_search(this_node_id).unwrap();
                    if is_cycle {
                        this_node.set_state(State::Aborted).unwrap();
                    }
                } else {
                    debug!("No incoming edges");
                    // Set status to commit
                    this_node.set_state(State::Committed).unwrap();
                    break;
                }
            }
        }

        let this_node = self.get_node(this_node_id).read().unwrap();
        match this_node.get_state().unwrap() {
            State::Aborted => {
                self.abort(meta.clone()).unwrap();
                let e = Box::new(SerializationGraphTestingError::new(
                    SerializationGraphTestingErrorKind::SerializableError,
                ));
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
            State::Committed => {
                self.clean_up_graph(this_node_id);
            }
            State::Free => panic!("trying to commit a free sloted"),

            State::Active => panic!("node should not be active"),
        }

        // {
        //     // (7) Reset node information.
        //     debug!("Reset node info");
        //     self.get_node(this_node_id).write().unwrap().reset();
        // }
        Ok(())
    }
}

impl SerializationGraphTesting {
    /// Initialise serialization graph with `size` nodes.
    ///
    /// The number nodes is equal to the number of cores.
    pub fn new(size: i32, workload: Arc<Workload>) -> Self {
        info!("Initialise serialization graph with {} nodes", size);
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

    /// Clean up the graph.
    fn clean_up_graph(&self, id: usize) {
        debug!("Cleaning up graph");

        // Take shared lock.
        let this_node = self.get_node(id).read().unwrap();
        // Get state.
        let state = this_node.get_state().unwrap();
        debug!("Node state: {:?}", state);
        match state {
            State::Committed => {
                // Get outgoing edges
                let outgoing_nodes = this_node.get_outgoing().unwrap();
                for out in outgoing_nodes {
                    // Get read lock on outgoing.
                    let outgoing_node = self.get_node(out).read().unwrap();
                    // Delete from edge sets.
                    outgoing_node.delete_edge(id, EdgeType::Incoming).unwrap();
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
                    outgoing_node.delete_edge(id, EdgeType::Incoming).unwrap();
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
    fn get_table(&self, table: &str, meta: TransactionInfo) -> Result<Arc<Table>, Aborted> {
        // Get table.
        let res = self.data.get_internals().get_table(table);
        match res {
            Ok(table) => Ok(table),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get primary index name on a table.
    fn get_index_name(&self, table: Arc<Table>, meta: TransactionInfo) -> Result<String, Aborted> {
        let res = table.get_primary_index();
        match res {
            Ok(index_name) => Ok(index_name),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get shared reference to index for a table.
    fn get_index(&self, table: Arc<Table>, meta: TransactionInfo) -> Result<Arc<Index>, Aborted> {
        // Get index name.
        let index_name = self.get_index_name(table, meta.clone())?;

        // Get index for this key's table.
        let res = self.data.get_internals().get_index(&index_name);
        match res {
            Ok(index) => Ok(index),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::message::Request;
    use crate::server::TransactionManager;
    use crate::workloads::tatp;
    use crate::workloads::Internal;
    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::mpsc::{Receiver, Sender};
    use std::sync::Once;
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
            // Load from test file.
            c.merge(config::File::with_name("Test-sgt.toml")).unwrap();
           let config = Arc::new(c);

            // Workload with fixed seed.
            let schema = config.get_str("schema").unwrap();
            let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
            let seed = config.get_int("seed").unwrap() as u64;
            let mut rng = StdRng::seed_from_u64(seed);
            tatp::loader::populate_tables(&internals, &mut rng).unwrap();
            Arc::new(Workload::Tatp(internals))
        };
    }

    #[test]
    fn sgt_test() {
        logging(false);

        // Shutdown channels.
        let (notify_tm_tx, tm_shutdown_rx) = std::sync::mpsc::channel();
        let (notify_wh_tx, _) = tokio::sync::broadcast::channel(1);

        // Work channels.
        let (_, work_rx): (Sender<Request>, Receiver<Request>) = std::sync::mpsc::channel();

        // Create transaction manager.
        let tm = TransactionManager::new(
            Arc::clone(&WORKLOAD),
            work_rx,
            tm_shutdown_rx,
            notify_wh_tx.clone(),
        );

        let scheduler1 = Arc::clone(&tm.get_scheduler());
        let scheduler2 = Arc::clone(&tm.get_scheduler());
        tm.get_pool().execute(move || {
            assert_eq!(
                format!(
                    "{}",
                    tatp::procedures::get_new_destination(
                        tatp::profiles::GetNewDestination {
                            s_id: 10,
                            sf_type: 1,
                            start_time: 0,
                            end_time: 1,
                        },
                        scheduler1
                    )
                    .unwrap_err()
                ),
                format!("Aborted: row does not exist in index.")
            );
            debug!("Here A");
        });

        tm.get_pool().execute(move || {
            assert_eq!(
                tatp::procedures::get_new_destination(
                    tatp::profiles::GetNewDestination {
                        s_id: 1,
                        sf_type: 1,
                        start_time: 8,
                        end_time: 12,
                    },
                    scheduler2
                )
                .unwrap(),
                "{number_x=\"993245295996111\"}"
            );

            debug!("Here B");
        });

        // // destructure TM
        // let TransactionManager { pool, .. } = tm;

        // drop(pool);
    }
}
