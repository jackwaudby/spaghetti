use crate::server::scheduler::serialization_graph_testing::error::SerializationGraphTestingError;
use crate::server::scheduler::serialization_graph_testing::node::{EdgeType, Node, State};
use crate::server::scheduler::{Aborted, Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::row::{Access, Row};
use crate::server::storage::table::Table;
use crate::workloads::{PrimaryKey, Workload};

use std::collections::HashSet;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
        let node_id = tname.parse::<usize>().unwrap();
        debug!("Registed transaction with node {}", tname);
        let node = self.get_exculsive_lock(node_id).unwrap();
        // Set state to active.
        node.set_state(State::Active).unwrap();
        // Set transaction id.
        Ok(TransactionInfo::new(Some(node_id.to_string()), None))
    }

    /// Create row in table. The row is immediately inserted into the table and marked as dirty.
    ///
    /// # Aborts
    /// - Table or index does not exist
    /// - Incorrect column or value
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
        // Record insert - used to rollback if transaction is aborted.
        self.get_shared_lock(this_node)
            .unwrap()
            .keys_inserted
            .lock()
            .unwrap()
            .push((index.get_name(), key));

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

        // Get row
        let read_guard = match index.get_lock_on_row(key) {
            Ok(rg) => rg,
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        };

        // Deref to row.
        let mut mg = read_guard.lock().unwrap();
        let row = &mut *mg;
        let res = row.get_values(columns, "sgt", &meta.get_id().unwrap());

        // Execute read.
        // match index.read(key, columns, "sgt", &meta.get_id().unwrap()) {
        match res {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap();
                // Detect conflicts and insert edges.
                for access in access_history {
                    // WR conflict
                    if let Access::Write(tid) = access {
                        // Get position of the the conflicting transaction in the graph.
                        let from_node: usize = tid.parse().unwrap();
                        // Insert edges
                        if let Err(e) = self.add_edge(from_node, this_node, false) {
                            self.abort(meta.clone()).unwrap();
                            return Err(Aborted {
                                reason: format!("{}", e),
                            });
                        }
                    }
                }
                let this_node = self.get_shared_lock(this_node).unwrap();
                // Record read of this key - used for abort.
                this_node
                    .keys_read
                    .lock()
                    .unwrap()
                    .push((index.get_name(), key));

                // Get values
                let vals = res.get_values().unwrap();
                Ok(vals)
            }
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
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

        // Get row
        let read_guard = match index.get_lock_on_row(key) {
            Ok(rg) => rg,
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        };

        // Deref to row.
        let mut mg = read_guard.lock().unwrap();
        let row = &mut *mg;
        let res = row.set_values(columns, values, "sgt", &meta.get_id().unwrap());

        // Execute write operation.
        // match index.update(key, columns, values, "sgt", &meta.get_id().unwrap()) {
        match res {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Get position of this transaction in the graph.
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap();
                // Detect conflicts.
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            // Get position of conflicting transaction in the graph.
                            let from_node: usize = tid.parse().unwrap();
                            // Insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, false) {
                                self.abort(meta.clone()).unwrap();
                                return Err(Aborted {
                                    reason: format!("{}", e),
                                });
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let from_node: usize = tid.parse().unwrap();
                            // Insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                self.abort(meta.clone()).unwrap();
                                return Err(Aborted {
                                    reason: format!("{}", e),
                                });
                            }
                        }
                    }
                }
                // Record write
                self.get_shared_lock(this_node)
                    .unwrap()
                    .keys_written
                    .lock()
                    .unwrap()
                    .push((index.get_name(), key));
                Ok(())
            }
            Err(e) => {
                self.abort(meta).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
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
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap();
                // Detect conflicts.
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            // Find the node for conflicting transaction
                            let from_node: usize = tid.parse().unwrap();
                            // Insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, false) {
                                self.abort(meta.clone()).unwrap();
                                return Err(Aborted {
                                    reason: format!("{}", e),
                                });
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            // Insert edges
                            let from_node: usize = tid.parse().unwrap();
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                self.abort(meta.clone()).unwrap();
                                return Err(Aborted {
                                    reason: format!("{}", e),
                                });
                            }
                        }
                    }
                }
                // Record write
                self.get_shared_lock(this_node)
                    .unwrap()
                    .keys_written
                    .lock()
                    .unwrap()
                    .push((index.get_name(), key));
                Ok(())
            }
            Err(e) => {
                self.abort(meta).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        let this_node_id = meta.get_id().unwrap().parse::<usize>().unwrap();
        debug!("Aborting transaction {:?}", meta.get_id().unwrap());
        // Set state to abort.
        {
            self.get_exculsive_lock(this_node_id)
                .unwrap()
                .set_state(State::Aborted)
                .unwrap();
            // Drop write lock on node.
        }
        // Revert updates/deletes.
        let written = self
            .get_shared_lock(this_node_id)
            .unwrap()
            .get_keys_written()
            .unwrap();
        for (index, key) in &written {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.revert(*key, "sgt", &meta.get_id().unwrap()).unwrap();
        }
        // Remove read accesses from rows read.
        let read = self
            .get_shared_lock(this_node_id)
            .unwrap()
            .get_keys_read()
            .unwrap();
        for (index, key) in read {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.revert_read(key, &meta.get_id().unwrap()).unwrap();
        }
        // Remove inserted values.
        let inserted = self
            .get_shared_lock(this_node_id)
            .unwrap()
            .get_rows_inserted()
            .unwrap();
        for (index, key) in inserted {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.remove(key).unwrap();
        }

        // Abort outgoing nodes.
        self.clean_up_graph(this_node_id);

        // Reset node information.
        self.get_exculsive_lock(this_node_id).unwrap().reset();
        debug!("Node {} aborted", meta.get_id().unwrap());
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), Aborted> {
        let id = meta.get_id().unwrap().parse::<usize>().unwrap();
        loop {
            let sl = self.get_shared_lock(id).unwrap();
            let state = sl.get_state().unwrap();
            drop(sl);
            if let State::Active = state {
                let commit_check = self.commit_check(id);
                if !commit_check {
                    debug!("Execute cycle check for node {}", id);
                    let cycle_check = self.reduced_depth_first_search(id);
                    if cycle_check {
                        debug!("Cycle found for node {}", id);
                        self.get_shared_lock(id)
                            .unwrap()
                            .set_state(State::Aborted)
                            .unwrap();
                    } else {
                        debug!("No cycle found for node {}", id);
                    }
                }
            } else {
                break;
            }
        }

        let sl = self.get_shared_lock(id).unwrap();
        let state = sl.get_state().unwrap();
        drop(sl);

        match state {
            State::Aborted => {
                self.abort(meta.clone()).unwrap();
                let e = SerializationGraphTestingError::SerializableError;
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
            State::Committed => {
                // TODO: redunancy here.
                debug!("Node {} committed", id);
                self.clean_up_graph(id);
                debug!("Cleaned up committed node {}", id);
                self.get_exculsive_lock(id).unwrap().reset();
                debug!("Reset committed node {}", id);
            }
            State::Free => panic!("trying to commit a free sloted"),

            State::Active => panic!("node should not be active"),
        }

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

    /// Get shared lock on the node.
    ///
    /// # Errors
    /// - RWLock failed
    fn get_shared_lock(
        &self,
        id: usize,
    ) -> Result<RwLockReadGuard<Node>, SerializationGraphTestingError> {
        let sh = self.nodes[id]
            .read()
            .map_err(|_| SerializationGraphTestingError::RwLockFailed)?;
        Ok(sh)
    }

    /// Get exculsive lock on the node.
    ///
    /// # Errors
    /// - RWLock failed
    fn get_exculsive_lock(
        &self,
        id: usize,
    ) -> Result<RwLockWriteGuard<Node>, SerializationGraphTestingError> {
        let ex = self.nodes[id]
            .write()
            .map_err(|_| SerializationGraphTestingError::RwLockFailed)?;
        Ok(ex)
    }

    /// Insert an edge into the serialization graph `(from)--->(to)`.
    ///
    /// If the edge is a self edge or already exists the function returns early.
    ///
    /// # Errors
    /// - Parent node is aborted
    ///
    /// # Panics
    /// - Mutex lock failed
    /// - Node unexpectedly free
    pub fn add_edge(
        &self,
        from: usize,
        to: usize,
        rw_edge: bool,
    ) -> Result<(), SerializationGraphTestingError> {
        // Check for self edges.
        if from == to {
            return Ok(());
        }
        // Acquire read locks on nodes.
        let from_node = self.get_shared_lock(from).unwrap();
        let to_node = self.get_shared_lock(to).unwrap();
        // Get from node state.
        let from_node_state = from_node.get_state().unwrap();
        match from_node_state {
            // Parent is aborted.
            State::Aborted => {
                // If WW or WR edge.
                if !rw_edge {
                    return Err(SerializationGraphTestingError::ParentAborted);
                }
            }
            // Insert edge.
            State::Active => {
                from_node.insert_edge(to, EdgeType::Outgoing).unwrap();
                to_node.insert_edge(from, EdgeType::Incoming).unwrap();
            }
            // Skip.
            State::Committed => {}
            // Not expected
            State::Free => {
                // TODO: causing some necessary aborts.
                panic!(
                    "Adding edge {} -> {}. Parent node {} state should not be free.",
                    from, to, from
                );
            }
        }
        Ok(())
    }

    pub fn commit_check(&self, id: usize) -> bool {
        debug!("Commit check for node {}", id);
        let node = self.get_exculsive_lock(id).unwrap();
        if !node.has_incoming().unwrap() {
            debug!("Node {} has no incoming edges", id);
            node.set_state(State::Committed).unwrap();
            return true;
        } else {
            debug!("Node {} has incoming edges", id);
            return false;
        }
    }

    /// Perform a reduced depth first search.
    ///
    /// # Panics
    /// - RwLock failed
    /// - Mutex failed
    pub fn reduced_depth_first_search(&self, start: usize) -> bool {
        // Tracking nodes to visit.
        let mut stack = Vec::new();
        // Tracking visited nodes.
        let mut visited = HashSet::new();
        {
            // Get read lock on start node
            let start_node = self.get_shared_lock(start).unwrap();
            // Push children to stack.
            stack.append(&mut start_node.get_outgoing().unwrap());
            // Mark as visited.
            // visited.insert(start);
        }
        // Pop until no more nodes to visit.
        while let Some(current) = stack.pop() {
            if current == start {
                return true;
            }
            // Skip if already visited
            if visited.contains(&current) {
                continue;
            }
            // Mark as visited.
            visited.insert(current);
            // Get read lock on current_node.
            let current_node = self.get_shared_lock(current).unwrap();
            // If current is active
            if let State::Active = current_node.get_state().unwrap() {
                // For each outgoing node.
                for child in current_node.get_outgoing().unwrap() {
                    // If has outgoing edge to start node then there is a cycle.
                    if child == start {
                        return true;
                    } else {
                        // Get read lock on child_node.
                        let c = self.get_shared_lock(child).unwrap();
                        // Mark as visited.
                        visited.insert(child);
                        // Add outgoing to stack.
                        stack.append(&mut c.get_outgoing().unwrap());
                    }
                }
            }
        }
        false
    }

    /// Clean up graph.
    ///
    /// If node with `id` aborted then abort outgoing nodes before removing edges.
    /// Else node committed, remove outgoing edges.
    ///
    /// # Panics
    ///
    /// - RwLock failed
    /// - Mutex lock failed
    fn clean_up_graph(&self, id: usize) {
        // Take shared lock.
        let this_node = self.get_shared_lock(id).unwrap();
        // Get state.
        let state = this_node.get_state().unwrap();
        // Get outgoing edges
        let outgoing_nodes = this_node.get_outgoing().unwrap();
        for out in outgoing_nodes {
            let outgoing_node = self.get_shared_lock(out).unwrap();
            // Abort children if this node was aborted
            if let State::Aborted = state {
                if outgoing_node.get_state().unwrap() == State::Active {
                    outgoing_node.set_state(State::Aborted).unwrap();
                }
            }
            // Remove edges.
            outgoing_node.delete_edge(id, EdgeType::Incoming).unwrap();
            this_node.delete_edge(out, EdgeType::Outgoing).unwrap();
        }
    }

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
    use crate::server::storage::datatype;
    use crate::workloads::tatp;
    use crate::workloads::tatp::keys::TatpPrimaryKey;
    use crate::workloads::Internal;
    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
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
    fn dfs_1_test() {
        logging(false);

        let sg = SerializationGraphTesting::new(5, Arc::clone(&WORKLOAD));
        sg.get_exculsive_lock(0)
            .unwrap()
            .set_state(State::Active)
            .unwrap();
        sg.get_exculsive_lock(1)
            .unwrap()
            .set_state(State::Active)
            .unwrap();
        sg.get_exculsive_lock(2)
            .unwrap()
            .set_state(State::Active)
            .unwrap();

        assert_eq!(sg.add_edge(0, 1, true).unwrap(), ());
        assert_eq!(sg.add_edge(1, 2, true).unwrap(), ());
        assert_eq!(sg.add_edge(2, 0, true).unwrap(), ());

        assert_eq!(sg.reduced_depth_first_search(0), true);
        assert_eq!(sg.reduced_depth_first_search(1), true);
        assert_eq!(sg.reduced_depth_first_search(2), true);
    }

    #[test]
    fn dfs_2_test() {
        logging(false);

        let sg = SerializationGraphTesting::new(5, Arc::clone(&WORKLOAD));

        sg.get_exculsive_lock(0)
            .unwrap()
            .set_state(State::Active)
            .unwrap();
        sg.get_exculsive_lock(1)
            .unwrap()
            .set_state(State::Active)
            .unwrap();
        sg.get_exculsive_lock(2)
            .unwrap()
            .set_state(State::Active)
            .unwrap();

        assert_eq!(sg.add_edge(0, 1, true).unwrap(), ());
        assert_eq!(sg.add_edge(0, 2, true).unwrap(), ());
        assert_eq!(sg.add_edge(1, 2, true).unwrap(), ());
        assert_eq!(sg.add_edge(2, 1, true).unwrap(), ());

        assert_eq!(sg.reduced_depth_first_search(0), false);
        assert_eq!(sg.reduced_depth_first_search(1), true);
        assert_eq!(sg.reduced_depth_first_search(2), true);
    }

    #[test]
    fn crud_test() {
        let sg = SerializationGraphTesting::new(5, Arc::clone(&WORKLOAD));

        let builder = thread::Builder::new().name("0".into());

        let handler = builder
            .spawn(move || {
                assert_eq!(thread::current().name(), Some("0"));
                let meta = sg.register().unwrap();
                assert_eq!(meta, TransactionInfo::new(Some("0".to_string()), None));

                let table = "access_info";
                let columns: Vec<&str> = vec!["data_1", "data_2", "data_3", "data_4"];

                let key = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 1));

                assert_eq!(
                    datatype::to_result(
                        &columns,
                        &sg.read(table, key, &columns, meta.clone()).unwrap()
                    )
                    .unwrap(),
                    "{data_1=\"57\", data_2=\"200\", data_3=\"IEU\", data_4=\"WIDHY\"}"
                );

                let values: Vec<&str> = vec!["12", "678", "POD", "TDHDH"];
                let meta_2 = TransactionInfo::new(Some("1".to_string()), None);
                sg.update(table, key, &columns, &values, meta_2).unwrap();

                // Check graph has edge between 0->1
                assert_eq!(
                    sg.get_shared_lock(0).unwrap().get_outgoing().unwrap(),
                    vec![1]
                );
                assert_eq!(
                    sg.get_shared_lock(1).unwrap().get_incoming().unwrap(),
                    vec![0]
                );

                assert_eq!(sg.commit(meta).unwrap(), ());
            })
            .unwrap();

        handler.join().unwrap();
    }
}
