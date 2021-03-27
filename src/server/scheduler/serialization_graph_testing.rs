use crate::common::error::NonFatalError;
use crate::server::scheduler::serialization_graph_testing::error::SerializationGraphTestingError;
use crate::server::scheduler::serialization_graph_testing::node::{
    EdgeType, Node, OperationType, State,
};
use crate::server::scheduler::{Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{Access, Row};
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
    fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        // Get thread name.
        let tname = thread::current().name().unwrap().to_string();
        let node_id = tname.parse::<usize>().unwrap();
        debug!(
            "Thread {}: Registered transaction with node {}",
            tname, node_id
        );
        // Get exculsive lock.
        let node = self.get_exculsive_lock(node_id);
        // Set state to active.
        node.set_state(State::Active);
        // Set transaction id.
        Ok(TransactionInfo::new(Some(node_id.to_string()), None))
    }

    /// Create row in table. The row is immediately inserted into the table and marked as dirty.
    ///
    /// # Aborts
    ///
    /// An abort is triggered if either (i) table or index does not exist, (ii) unable to intialise row.
    /// - Incorrect column or value
    fn create(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let handle = thread::current();
        debug!(
            "Thread {}: Executing create operation",
            handle.name().unwrap()
        );

        // Get table and index.
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Create new row.
        let mut row = Row::new(Arc::clone(&table), "sgt");

        row.set_primary_key(key.clone());
        for (i, column) in columns.iter().enumerate() {
            if let Err(_) = row.init_value(column, &values[i].to_string()) {
                // ABORT  - UnableToInitialiseRow
                self.abort(meta.clone()).unwrap();
                // Return abort reason.
                return Err(NonFatalError::UnableToInitialiseRow(
                    table.to_string(),
                    column.to_string(),
                    values[i].to_string(),
                ));
            }
        }
        // Set values - needed to make the row "dirty"
        if let Err(e) = row.set_values(columns, values, "sgt", &meta.get_id().unwrap()) {
            // ABORT - UnableToConvertToDataType
            self.abort(meta.clone()).unwrap();
            return Err(e);
        }

        // Get position of this transaction in the graph.
        let id = meta.get_id().unwrap().parse::<usize>().unwrap();

        // Record insert - used to rollback if transaction is aborted.
        self.get_shared_lock(id)
            .add_key(&index.get_name(), key.clone(), OperationType::Insert);

        // Attempt to insert row.
        if let Err(e) = index.insert(key, row) {
            // ABORT - RowAlreadyExists
            self.abort(meta.clone()).unwrap();
            return Err(e);
        }
        Ok(())
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
    ) -> Result<Vec<Data>, NonFatalError> {
        let handle = thread::current();
        debug!(
            "Thread {}: Executing read operation",
            handle.name().unwrap()
        );
        // Get table and index.
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Get read guard on the row in the hashmap.
        let rg = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                // ABORT - RowNotFound.
                self.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };

        // Acquire mutex on the row.
        let mut mg = rg.lock().unwrap();
        // Deref to row
        let row = &mut *mg;
        // Get the calues
        let res = row.get_values(columns, "sgt", &meta.get_id().unwrap());
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
                            // ABORT - ParentAborted.
                            self.abort(meta.clone()).unwrap();
                            return Err(e.into());
                        }
                    }
                }
                let this_node = self.get_shared_lock(this_node);
                this_node.add_key(&index.get_name(), key, OperationType::Read);
                // Get values
                let vals = res.get_values().unwrap();
                Ok(vals)
            }
            Err(e) => {
                // ABORT - RowDeleted
                self.abort(meta.clone()).unwrap();
                return Err(e);
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
        read: bool,
        params: Vec<Data>,
        // (columns, current_values, parameters) -> (columns,new_values)
        f: &dyn Fn(Vec<String>, Option<Vec<Data>>, Vec<Data>) -> (Vec<String>, Vec<String>),
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let handle = thread::current();
        debug!(
            "Thread {}: Executing update operation",
            handle.name().unwrap()
        );

        // Get table and index
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Get row
        let read_guard = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                // ABORT - RowNotFound.
                self.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };

        // Get mutex on row.
        let mut mg = read_guard.lock().unwrap();
        // Deref to row.
        let row = &mut *mg;

        // 1. Get current values of columns.
        let current_values;
        if read {
            let res = row.get_values(columns, "sgt", &meta.get_id().unwrap())?;
            current_values = res.get_values();
        } else {
            current_values = None;
        }

        // 2. Compute new values.
        let todo = vec!["todo".to_string()];
        let (columns, new_values) = f(todo, current_values, params);

        // 3. Converson
        let v: Vec<&str> = new_values.iter().map(|s| s as &str).collect();

        // Set values.

        let todo = vec!["todo"];

        let res = row.set_values(&todo, &v, "sgt", &meta.get_id().unwrap());
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
                                // ABORT - parent aborted.
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into());
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let from_node: usize = tid.parse().unwrap();
                            // Insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                // ABORT - parent aborted.
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into());
                            }
                        }
                    }
                }
                let this_node = self.get_shared_lock(this_node);
                this_node.add_key(&index.get_name(), key, OperationType::Update);
                Ok(())
            }
            Err(e) => {
                // ABORT - RowDeleted or RowDirty
                self.abort(meta).unwrap();
                return Err(e);
            }
        }
    }

    /// Delete from row.
    fn delete(
        &self,
        table: &str,
        key: PrimaryKey,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let handle = thread::current();
        debug!(
            "Thread {}: Executing delete operation",
            handle.name().unwrap()
        );
        // Get table and indexes.
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Get read guard on row in hashmap.
        let read_guard = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                // ABORT - RowNotFound.
                self.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };
        // Get mutex on row.
        let mut mg = read_guard.lock().unwrap();
        // Deref to row.
        let row = &mut *mg;
        let res = row.delete("sgt");
        match res {
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
                                return Err(e.into());
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            // Insert edges
                            let from_node: usize = tid.parse().unwrap();
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into());
                            }
                        }
                    }
                }
                // Record delete
                let this_node = self.get_shared_lock(this_node);

                this_node.add_key(&index.get_name(), key, OperationType::Delete);

                Ok(())
            }
            Err(e) => {
                // ABORT - RowDeleted or RowDirty

                self.abort(meta).unwrap();
                Err(e)
            }
        }
    }

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        let handle = thread::current();
        debug!(
            "Thread {}: Starting abort procedure",
            handle.name().unwrap()
        );
        let this_node_id = meta.get_id().unwrap().parse::<usize>().unwrap();

        // Set state to abort.
        {
            self.get_exculsive_lock(this_node_id)
                .set_state(State::Aborted);

            // Drop write lock on node.
        }
        // Revert updates.
        let sl = self.get_shared_lock(this_node_id);
        let inserts = sl.get_keys(OperationType::Insert);
        let reads = sl.get_keys(OperationType::Read);
        let updates = sl.get_keys(OperationType::Update);
        let deletes = sl.get_keys(OperationType::Delete);
        drop(sl);

        for (index, key) in &inserts {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.remove(key.clone()).unwrap();
        }

        for (index, key) in &reads {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index
                .revert_read(key.clone(), &meta.get_id().unwrap())
                .unwrap();
        }

        for (index, key) in &updates {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index
                .revert(key.clone(), "sgt", &meta.get_id().unwrap())
                .unwrap();
        }

        for (index, key) in &deletes {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index
                .revert(key.clone(), "sgt", &meta.get_id().unwrap())
                .unwrap();
        }

        // Abort outgoing nodes.
        self.clean_up_graph(this_node_id);

        // Reset node information.
        self.get_exculsive_lock(this_node_id).reset();
        debug!(
            "Thread {}: Abort procedure finished",
            handle.name().unwrap()
        );
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        let handle = thread::current();
        debug!(
            "Thread {}: Starting commit procedure",
            handle.name().unwrap()
        );
        // Get node id.
        let id = meta.get_id().unwrap().parse::<usize>().unwrap();
        // While node is active keep cycle checking.
        loop {
            // Acquire shared lock on the node.
            let sl = self.get_shared_lock(id);
            // Get node state
            let state = sl.get_state();
            // Drop lock.
            drop(sl);
            if let State::Active = state {
                let commit_check = self.commit_check(id);
                if !commit_check {
                    let cycle_check = self.reduced_depth_first_search(id);
                    if cycle_check {
                        let sl = self.get_shared_lock(id);
                        sl.set_state(State::Aborted);
                        drop(sl);
                    }
                }
            } else {
                break;
            }
        }

        // get node state.
        let sl = self.get_shared_lock(id);
        let state = sl.get_state();
        drop(sl);

        match state {
            State::Aborted => {
                self.abort(meta.clone()).unwrap();
                let e = SerializationGraphTestingError::NonSerializable;
                return Err(e.into());
            }
            State::Committed => {
                self.clean_up_graph(id);
                let sl = self.get_shared_lock(id);
                let inserts = sl.get_keys(OperationType::Insert);
                let updates = sl.get_keys(OperationType::Update);
                let deletes = sl.get_keys(OperationType::Delete);

                for (index, key) in inserts {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index.commit(key, "sgt", &id.to_string()).unwrap();
                }

                for (index, key) in updates {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index.commit(key, "sgt", &id.to_string()).unwrap();
                }

                for (index, key) in deletes {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index.remove(key).unwrap();
                }

                drop(sl);
                {
                    self.get_exculsive_lock(id).reset();
                }
            }
            State::Active => panic!("node should not be active"),
        }

        debug!(
            "Thread {}: Commit procedure finished",
            handle.name().unwrap()
        );
        Ok(())
    }

    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}

impl SerializationGraphTesting {
    /// Initialise serialization graph with `size` nodes.
    ///
    /// The number of nodes is equal to the number of cores.
    pub fn new(size: u32, data: Arc<Workload>) -> Self {
        info!("Initialise serialization graph with {} nodes", size);
        let mut nodes = vec![];
        for i in 0..size {
            let node = RwLock::new(Node::new(i as usize));
            nodes.push(node);
        }
        SerializationGraphTesting { nodes, data }
    }

    /// Get shared lock on the node.
    ///
    /// # Panics
    ///
    /// Acquiring `RwLock` fails.
    fn get_shared_lock(&self, id: usize) -> RwLockReadGuard<Node> {
        let rg = self.nodes[id].read().unwrap();
        rg
    }

    /// Get exculsive lock on the node.
    ///
    /// # Panics
    ///
    /// Acquiring `RwLock` fails.
    fn get_exculsive_lock(&self, id: usize) -> RwLockWriteGuard<Node> {
        let wg = self.nodes[id].write().unwrap();
        wg
    }

    /// Insert an edge into the serialization graph `(from) --> (to)`.
    ///
    /// If the edges is (i) a self edge, (ii) already exists, or (iii) from node is
    /// committed no edge is added.
    ///
    /// # Errors
    ///
    /// The operation fails if the parent node (from) is already aborted.
    ///
    /// # Panics
    ///
    /// Unable to acquire shared locks.
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
        // Acquire shared locks on nodes.
        let from_node = self.get_shared_lock(from);
        let to_node = self.get_shared_lock(to);
        // Get from node state.
        let from_node_state = from_node.get_state();
        match from_node_state {
            // Parent is aborted.
            State::Aborted => {
                // If WW or WR edge.
                if !rw_edge {
                    return Err(SerializationGraphTestingError::ParentAborted);
                } else {
                    return Ok(());
                }
            }
            // Insert edge.
            State::Active => {
                // insert_edge handles the case the edge already exists.
                from_node.insert_edge(to, EdgeType::Outgoing);
                to_node.insert_edge(from, EdgeType::Incoming);
                return Ok(());
            }
            // Skip.
            State::Committed => {
                return Ok(());
            }
        }
    }

    /// Check if node has any incoming edges. If so, set state to
    /// committed and return true.
    ///
    /// # Panics
    ///
    /// Unable to acquire exculsive lock.
    pub fn commit_check(&self, id: usize) -> bool {
        let node = self.get_exculsive_lock(id);
        if !node.has_incoming() {
            node.set_state(State::Committed);
            return true;
        }
        false
    }

    /// Perform a reduced depth first search.
    ///
    /// # Panics
    ///
    /// Unable to acquire locks on node.
    pub fn reduced_depth_first_search(&self, start: usize) -> bool {
        // Tracking nodes to visit.
        let mut stack = Vec::new();
        // Tracking visited nodes.
        let mut visited = HashSet::new();
        {
            // Get shared lock on start node
            let sl = self.get_shared_lock(start);
            // Push children to stack.
            stack.append(&mut sl.get_outgoing());
            // Drop shared lock.
            drop(sl)
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
            // Get shared lock on current_node.
            let current_node = self.get_shared_lock(current);
            // If current is active
            if let State::Active = current_node.get_state() {
                // For each outgoing node.
                for child in current_node.get_outgoing() {
                    // If has outgoing edge to start node then there is a cycle.
                    if child == start {
                        return true;
                    } else {
                        // Get read lock on child_node.
                        let c = self.get_shared_lock(child);
                        // Mark as visited.
                        visited.insert(child);
                        // Add outgoing to stack.
                        stack.append(&mut c.get_outgoing());
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
    /// Unable to acquire locks on node.
    fn clean_up_graph(&self, id: usize) {
        // Take shared lock.
        let sl = self.get_shared_lock(id);
        // Get state.
        let state = sl.get_state();
        // Get outgoing edges
        let outgoing_nodes = sl.get_outgoing();
        for out in outgoing_nodes {
            let outgoing_node = self.get_shared_lock(out);
            // Abort children if this node was aborted
            if let State::Aborted = state {
                if outgoing_node.get_state() == State::Active {
                    outgoing_node.set_state(State::Aborted);
                }
            }
            // Remove edges.
            outgoing_node.delete_edge(id, EdgeType::Incoming);
            sl.delete_edge(out, EdgeType::Outgoing);
            drop(out);
        }
        drop(sl);
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
        sg.get_exculsive_lock(0).set_state(State::Active);
        sg.get_exculsive_lock(1).set_state(State::Active);
        sg.get_exculsive_lock(2).set_state(State::Active);

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

        sg.get_exculsive_lock(0).set_state(State::Active);
        sg.get_exculsive_lock(1).set_state(State::Active);
        sg.get_exculsive_lock(2).set_state(State::Active);

        assert_eq!(sg.add_edge(0, 1, true).unwrap(), ());
        assert_eq!(sg.add_edge(0, 2, true).unwrap(), ());
        assert_eq!(sg.add_edge(1, 2, true).unwrap(), ());
        assert_eq!(sg.add_edge(2, 1, true).unwrap(), ());

        assert_eq!(sg.reduced_depth_first_search(0), false);
        assert_eq!(sg.reduced_depth_first_search(1), true);
        assert_eq!(sg.reduced_depth_first_search(2), true);
    }

    // #[test]
    // fn crud_test() {
    //     let sg = SerializationGraphTesting::new(5, Arc::clone(&WORKLOAD));

    //     let builder = thread::Builder::new().name("0".into());

    //     let handler = builder
    //         .spawn(move || {
    //             assert_eq!(thread::current().name(), Some("0"));
    //             let meta = sg.register().unwrap();
    //             assert_eq!(meta, TransactionInfo::new(Some("0".to_string()), None));

    //             let table = "access_info";
    //             let columns: Vec<&str> = vec!["data_1", "data_2", "data_3", "data_4"];

    //             let key = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 1));

    //             assert_eq!(
    //                 datatype::to_result(
    //                     &columns,
    //                     &sg.read(table, key.clone(), &columns, meta.clone()).unwrap()
    //                 )
    //                 .unwrap(),
    //                 "{data_1=\"57\", data_2=\"200\", data_3=\"IEU\", data_4=\"WIDHY\"}"
    //             );

    //             let values: Vec<&str> = vec!["12", "678", "POD", "TDHDH"];
    //             let meta_2 = TransactionInfo::new(Some("1".to_string()), None);
    //             sg.update(table, key, &columns, &values, meta_2).unwrap();

    //             // Check graph has edge between 0->1
    //             assert_eq!(sg.get_shared_lock(0).get_outgoing(), vec![1]);
    //             assert_eq!(sg.get_shared_lock(1).get_incoming(), vec![0]);

    //             assert_eq!(sg.commit(meta).unwrap(), ());
    //         })
    //         .unwrap();

    //     handler.join().unwrap();
    // }
}
