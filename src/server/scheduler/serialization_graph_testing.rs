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
///
/// Graph can have multiple owners and shared across thread boundaries.
/// Each `Node` is wrapped in a `RwLock`.
#[derive(Debug)]
pub struct SerializationGraphTesting {
    nodes: Vec<RwLock<Node>>,

    /// Handle to storage layer.
    data: Arc<Workload>,
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
        if from == to {
            return Ok(()); // don't add self edges
        }

        let from_node = self.get_shared_lock(from); // get shared locks
        let to_node = self.get_shared_lock(to);

        match from_node.get_state() {
            State::Aborted => {
                // check for cascading abort
                if !rw_edge {
                    return Err(SerializationGraphTestingError::ParentAborted);
                }
            }
            // insert edge
            State::Active => {
                from_node.insert_edge(to, EdgeType::Outgoing);
                to_node.insert_edge(from, EdgeType::Incoming);
            }
            // skip adding edge
            State::Committed => {}
        }

        drop(from_node); // drop locks
        drop(to_node);
        Ok(())
    }

    /// Check if node has any incoming edges. If so, set state to
    /// committed and return true.
    ///
    /// # Panics
    ///
    /// Unable to acquire exculsive lock.
    pub fn commit_check(&self, id: usize) -> bool {
        let node = self.get_exculsive_lock(id); // get exculsive lock
        if !node.has_incoming() && node.get_state() == State::Active {
            node.set_state(State::Committed);
            return true;
        }
        false
    }

    /// Perform a reduced depth first search from `start` node.
    ///
    /// # Panics
    ///
    /// Unable to acquire locks on node.
    pub fn reduced_depth_first_search(&self, start: usize) -> bool {
        let mut stack = Vec::new(); // nodes to visit
        let mut visited = HashSet::new(); // nodes visited

        let sl = self.get_shared_lock(start); // get shared lock on start node
        stack.append(&mut sl.get_outgoing()); // push outgoing to stack
        drop(sl); // drop shared lock

        // pop until no more nodes to visit
        while let Some(current) = stack.pop() {
            if current == start {
                return true; // cycle found
            }

            if visited.contains(&current) {
                continue; // already visited
            }

            visited.insert(current); // mark as visited
            let current_node = self.get_shared_lock(current); // get shared lock on current_node.

            if let State::Active = current_node.get_state() {
                for child in current_node.get_outgoing() {
                    if child == start {
                        return true; // outgoing edge to start node -- cycle found
                    } else {
                        let c = self.get_shared_lock(child); // get read lock on child_node
                        visited.insert(child); // mark as visited
                        stack.append(&mut c.get_outgoing()); // add outgoing to stack
                        drop(c);
                    }
                }
            }
        }
        false // no cycle found
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
        let th = thread::current();
        let thread_id = th.name().unwrap(); // get thread id
        debug!("Thread {}: Starting clean up procedure", thread_id);

        let this_node = self.get_shared_lock(id); // get shared lock on this_node
        let state = this_node.get_state(); // get state of this_node
        let outgoing_nodes = this_node.get_outgoing(); // get outgoing edges

        for out in outgoing_nodes {
            let outgoing_node = self.get_shared_lock(out); // get shared lock on outgoing node

            // if node aborted; then abort children
            if let State::Aborted = state {
                if outgoing_node.get_state() == State::Active {
                    outgoing_node.set_state(State::Aborted); // cascading abort
                }
            }
            outgoing_node.delete_edge(id, EdgeType::Incoming); // remove incoming edge from out
            this_node.delete_edge(out, EdgeType::Outgoing); // remove outgoing edge from this_node
            drop(outgoing_node); // drop shared lock on outgoing node
        }
        drop(this_node); // drop shared lock on this_node
    }
}

impl Scheduler for SerializationGraphTesting {
    /// Register a transaction with the serialization graph.
    ///
    /// Transaction gets the ID of the thread it is executed on.
    fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        let th = thread::current(); // get handle to thread
        let thread_id = th.name().unwrap(); // get thread id
        let node_id = thread_id.parse::<usize>().unwrap(); // get node id
        debug!(
            "Thread {}: Registered transaction with node {}",
            thread_id, node_id
        );
        let node = self.get_exculsive_lock(node_id); // get exculsive lock
        node.set_state(State::Active); // set state to active

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
        let th = thread::current(); // get handle to thread
        let thread_id = th.name().unwrap(); // get thread id
        debug!("Thread {}: Executing create operation", thread_id);

        let table = self.get_table(table, meta.clone())?; // get handle to table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get handle to index
        let mut row = Row::new(Arc::clone(&table), "sgt"); // create new row
        row.set_primary_key(key.clone()); // set pk

        // initialise each field
        for (i, column) in columns.iter().enumerate() {
            if let Err(_) = row.init_value(column, &values[i].to_string()) {
                self.abort(meta.clone()).unwrap(); // abort -- unable to initialise row
                return Err(NonFatalError::UnableToInitialiseRow(
                    table.to_string(),
                    column.to_string(),
                    values[i].to_string(),
                ));
            }
        }

        // set values in fields -- makes rows "dirty"
        if let Err(e) = row.set_values(columns, values, "sgt", &meta.get_id().unwrap()) {
            self.abort(meta.clone()).unwrap(); // abort -- unable to convert to datatype
            return Err(e);
        }

        match index.insert(key.clone(), row) {
            Ok(_) => {
                let id = meta.get_id().unwrap().parse::<usize>().unwrap(); // get position in graph
                let node = self.get_shared_lock(id); // get shared lock
                node.add_key(&index.get_name(), key.clone(), OperationType::Insert); // operation succeeded -- register
                drop(node); // drop shared lock
            }

            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row already exists
                return Err(e);
            }
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
        let th = thread::current(); // get handle to thread
        let thread_id = th.name().unwrap(); // get thread id
        debug!("Thread {}: Executing read operation", thread_id);
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        // get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row does not exist in index
                return Err(e);
            }
        };
        let mut mg = rh.lock().unwrap(); // acquire mutex on the row
        let row = &mut *mg; // deref to row
        match row.get_values(columns, "sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // get this_node position in sgt
                let node = self.get_shared_lock(this_node); // take shared lock on this_node
                node.add_key(&index.get_name(), key.clone(), OperationType::Read); // operation succeeded -- register
                drop(node); // drop shared lock on node
                let access_history = res.get_access_history().unwrap(); // get access history

                // detect conflicts and insert edges.
                for access in access_history {
                    // WR conflict
                    if let Access::Write(tid) = access {
                        let from_node: usize = tid.parse().unwrap(); // get from_node position in sgt

                        // add edge
                        if let Err(e) = self.add_edge(from_node, this_node, false) {
                            drop(mg); // drop mutex on row
                            drop(rh); // drop read handle to row
                            self.abort(meta.clone()).unwrap(); // abort -- from_node aborted
                            return Err(e.into());
                        }
                    }
                }

                let vals = res.get_values().unwrap(); // get values

                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row

                Ok(vals)
            }
            Err(e) => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta.clone()).unwrap(); // abort -- row deleted
                return Err(e);
            }
        }
    }

    /// Execute an update operation.
    ///
    /// Adds an edge in the graph for each WW and RW conflict.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: Vec<String>,
        read: bool,
        params: Vec<Data>,
        f: &dyn Fn(
            Vec<String>,
            Option<Vec<Data>>,
            Vec<Data>,
        ) -> Result<(Vec<String>, Vec<String>), NonFatalError>,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let th = thread::current();
        let thread_id = th.name().unwrap(); // get thread id
        debug!("Thread {}: Executing update operation", thread_id);

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        // get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row not found
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap(); // get mutex on row
        let row = &mut *mg; // deref to row

        // read current values (optional)
        let c: Vec<&str> = columns.iter().map(|s| s as &str).collect(); // convert to expected type
        let current;
        if read {
            let res = match row.get_values(&c, "sgt", &meta.get_id().unwrap()) {
                Ok(res) => {
                    let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // get position in graph
                    let node = self.get_shared_lock(this_node); // take shared lock on this_node
                    node.add_key(&index.get_name(), key.clone(), OperationType::Read); // operation succeeded -- register
                    drop(node);
                    res
                }
                Err(e) => {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- row not found
                    return Err(e);
                }
            };
            current = res.get_values();
        } else {
            current = None;
        }

        // compute new values.
        let (_, new_values) = match f(columns.clone(), current, params) {
            Ok(res) => res,
            Err(e) => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta.clone()).unwrap(); // abort -- row not found
                return Err(e);
            }
        };

        let nv: Vec<&str> = new_values.iter().map(|s| s as &str).collect(); // convert to expected type
        match row.set_values(&c, &nv, "sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // get position in graph
                let node = self.get_shared_lock(this_node); // get shared lock on this node
                node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded register
                drop(node);

                let access_history = res.get_access_history().unwrap(); // get access history

                // insert edges
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let from_node: usize = tid.parse().unwrap(); // get from_node position in graph

                            // insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, false) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap(); // abort -- from_node aborted
                                return Err(e.into());
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let from_node: usize = tid.parse().unwrap();

                            // insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap(); // abort -- from_node aborted
                                return Err(e.into());
                            }
                        }
                    }
                }

                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                Ok(())
            }
            Err(e) => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta).unwrap(); // abort -- row deleted or dirty
                return Err(e);
            }
        }
    }

    /// Append `value` to `column`.
    ///
    /// Adds an edge in the graph for each WW and RW conflict.
    fn append(
        &self,
        table: &str,
        key: PrimaryKey,
        column: &str,
        value: &str,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let th = thread::current();
        let thread_id = th.name().unwrap(); // get thread id
        debug!("Thread {}: Executing append operation", thread_id);

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh, // get handle to row
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- RowNotFound.
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap(); // get mutex on row
        let row = &mut *mg; // deref to row

        match row.append_value(column, value, "sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // get position of this transaction in the graph
                let node = self.get_shared_lock(this_node);
                node.add_key(&index.get_name(), key, OperationType::Update); // operation succeeded -- register
                drop(node);

                let access_history = res.get_access_history().unwrap(); // get access history

                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let from_node: usize = tid.parse().unwrap(); // get position of conflicting transaction in the graph.

                            // insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, false) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap(); // abort -- parent aborted
                                return Err(e.into());
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let from_node: usize = tid.parse().unwrap();
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap(); // abort -- parent aborted
                                return Err(e.into());
                            }
                        }
                    }
                }
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                Ok(())
            }
            Err(e) => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta).unwrap(); // abort -- row deleted or row dirty
                return Err(e);
            }
        }
    }

    /// Read (get) and update (set).
    ///
    /// Adds an edge in the graph for each WW and RW conflict.
    fn read_and_update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        let handle = thread::current();
        debug!(
            "Thread {}: Executing read and update operation",
            handle.name().unwrap()
        );

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row not found.
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap(); // get mutex on row
        let row = &mut *mg; // deref to row

        // get and set values
        match row.get_and_set_values(columns, values, "sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // Get position of this transaction in the graph.
                let node = self.get_shared_lock(this_node); // get shared lock
                node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                drop(node);

                let access_history = res.get_access_history().unwrap(); // get access history

                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let from_node: usize = tid.parse().unwrap(); // Get position of conflicting transaction in the graph.

                            // insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, false) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap(); // abort -- parent aborted.
                                return Err(e.into());
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let from_node: usize = tid.parse().unwrap();

                            // insert edges
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap(); // abort -- parent aborted.
                                return Err(e.into());
                            }
                        }
                    }
                }
                let vals = res.get_values().unwrap(); // get values
                Ok(vals)
            }
            Err(e) => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta).unwrap(); // abort - row deleted or row dirty
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

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        // get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort - row not found
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap(); // get mutex on row
        let row = &mut *mg; // deref to row

        match row.delete("sgt") {
            Ok(res) => {
                let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // get position of this transaction in the graph
                let node = self.get_shared_lock(this_node); // get shared lock
                node.add_key(&index.get_name(), key, OperationType::Delete); // operation succeeded -- register
                drop(node);

                let access_history = res.get_access_history().unwrap(); // get the access history

                // detect conflicts
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let from_node: usize = tid.parse().unwrap(); // get from_node id
                            if let Err(e) = self.add_edge(from_node, this_node, false) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into());
                            }
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let from_node: usize = tid.parse().unwrap(); // get from_node id
                            if let Err(e) = self.add_edge(from_node, this_node, true) {
                                drop(mg); // drop mutex on row
                                drop(rh); // drop read handle to row
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into());
                            }
                        }
                    }
                }

                Ok(())
            }
            Err(e) => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta).unwrap(); // abort -- row deleted or row dirty
                Err(e)
            }
        }
    }

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        let th = thread::current();
        let thread_id = th.name().unwrap(); // get thread id
        debug!("Thread {}: Starting abort procedure", thread_id);

        let this_node_id = meta.get_id().unwrap().parse::<usize>().unwrap();

        {
            self.get_exculsive_lock(this_node_id)
                .set_state(State::Aborted); // set state to aborted
        }

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
            let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

            // get read handle to row
            if let Ok(rh) = index.get_lock_on_row(key.clone()) {
                let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                let row = &mut *mg; // deref to row
                row.revert_read(&meta.get_id().unwrap());
                drop(mg);
            };
        }

        for (index, key) in &updates {
            let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

            // get read handle to row
            if let Ok(rh) = index.get_lock_on_row(key.clone()) {
                let mut mg = rh.lock().unwrap(); // acquire mutex on the row

                let row = &mut *mg; // deref to row
                row.revert("sgt", &meta.get_id().unwrap());
                drop(mg);
            };
        }

        for (index, key) in &deletes {
            let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

            // get read handle to row
            if let Ok(rh) = index.get_lock_on_row(key.clone()) {
                let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                let row = &mut *mg; // deref to row
                row.revert("sgt", &meta.get_id().unwrap());
                drop(mg);
            };
        }

        self.clean_up_graph(this_node_id); // abort outgoing nodes
        self.get_exculsive_lock(this_node_id).reset(); // reset node information

        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        let th = thread::current();
        let thread_id = th.name().unwrap(); // get thread id
        let id = meta.get_id().unwrap().parse::<usize>().unwrap(); // get this_node id
        debug!("Thread {}: Starting commit procedure", thread_id);

        // While node is active keep cycle checking.
        loop {
            let sl = self.get_shared_lock(id); // get shared lock on this_node
            let state = sl.get_state(); // get node state
            drop(sl); // drop shared lock

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

        let sl = self.get_shared_lock(id); // take shared lock on this_node
        let state = sl.get_state(); // get this_node state
        drop(sl);

        match state {
            State::Aborted => {
                self.abort(meta.clone()).unwrap();
                let e = SerializationGraphTestingError::NonSerializable;
                return Err(e.into());
            }
            State::Committed => {
                self.clean_up_graph(id); // remove outgoing edges
                let sl = self.get_shared_lock(id); // get shared lock
                let inserts = sl.get_keys(OperationType::Insert);
                let updates = sl.get_keys(OperationType::Update);
                let deletes = sl.get_keys(OperationType::Delete);
                drop(sl); // drop shared lock

                for (index, key) in inserts {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.commit("sgt", &id.to_string()); // commit inserts
                    drop(mg);
                }

                for (index, key) in updates {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.commit("sgt", &id.to_string()); // commit updates
                    drop(mg);
                }

                for (index, key) in deletes {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.delete("sgt").unwrap(); // commit delete
                    drop(mg);
                }

                {
                    self.get_exculsive_lock(id).reset();
                }
            }
            State::Active => panic!("node should not be active"),
        }

        Ok(())
    }

    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::workloads::tatp;
    //    use crate::workloads::tatp::keys::TatpPrimaryKey;
    use crate::workloads::Internal;

    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    //  use std::convert::TryFrom;
    //use std::{thread, time};
    use test_env_log::test;

    lazy_static! {
        static ref WORKLOAD: Arc<Workload> = {
            // Initialise configuration.
           let mut c = Config::default();
            // Load from test file.
            c.merge(config::File::with_name("./tests/Test-sgt.toml")).unwrap();
           let config = Arc::new(c);

            // Workload with fixed seed.
            let schema = "./schema/tatp_schema.txt".to_string();
            let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
            let seed = config.get_int("seed").unwrap() as u64;
            let mut rng = StdRng::seed_from_u64(seed);
            tatp::loader::populate_tables(&internals, &mut rng).unwrap();
            Arc::new(Workload::Tatp(internals))
        };
    }

    #[test]
    fn dfs_1_test() {
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

    // fn test_sgt_g1a() {
    //     let sg = Arc::new(SerializationGraphTesting::new(5, Arc::clone(&WORKLOAD)));
    //     let sg1 = Arc::clone(&sg);
    //     let sg2 = Arc::clone(&sg);

    //     let builder = thread::Builder::new().name("0".into());

    //     // write a record then sleep
    //     let jh1 = builder
    //         .spawn(move || {
    //             let meta = sg1.register().unwrap(); // register transaction
    //             let table = "access_info"; // table name
    //             let key = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 1)); // primary key
    //             let columns: Vec<String> = vec!["data_1".to_string()]; // columns to write
    //             let params = vec![Data::Int(12)]; // values to write

    //             // (columns, current_values, parameters) -> (columns, new_values)
    //             let update = |_columns: Vec<String>,
    //                           _current: Option<Vec<Data>>,
    //                           params: Vec<Data>|
    //              -> Result<(Vec<String>, Vec<String>), NonFatalError> {
    //                 let value = match i64::try_from(params[0].clone()) {
    //                     Ok(value) => value,
    //                     Err(e) => {
    //                         sg1.abort(meta.clone()).unwrap();
    //                         return Err(e);
    //                     }
    //                 };

    //                 let new_values = vec![value.to_string()];
    //                 let columns = vec!["data_1".to_string()];
    //                 Ok((columns, new_values))
    //             };

    //             // table, pk, columns, read cols, params, update closure
    //             sg1.update(
    //                 table,
    //                 key.clone(),
    //                 columns,
    //                 false,
    //                 params,
    //                 &update,
    //                 meta.clone(),
    //             )
    //             .unwrap();

    //             let ten_millis = time::Duration::from_millis(5000);

    //             thread::sleep(ten_millis);

    //             // // Check graph has edge between 0->1
    //             // assert_eq!(sg.get_shared_lock(0).get_outgoing(), vec![1]);
    //             // assert_eq!(sg.get_shared_lock(1).get_incoming(), vec![0]);

    //             assert_eq!(sg1.abort(meta).unwrap(), ());
    //         })
    //         .unwrap();

    //     let ten_millis = time::Duration::from_millis(1000);

    //     thread::sleep(ten_millis);
    //     let builder2 = thread::Builder::new().name("1".into());

    //     // write a record then sleep
    //     let jh2 = builder2.spawn(move || {
    //         let meta = sg2.register().unwrap(); // register transaction
    //         let table = "access_info"; // table name
    //         let key = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 1)); // primary key
    //         let columns: Vec<&str> = vec!["data_1"]; // columns to write
    //         let _values = sg2.read(table, key, &columns, meta.clone()).unwrap();
    //         assert_eq!(
    //             sg2.commit(meta.clone()),
    //             Err(NonFatalError::SerializationGraphTesting(
    //                 SerializationGraphTestingError::NonSerializable
    //             ))
    //         );
    //     });

    //     jh1.join().unwrap();
    //     jh2.unwrap().join().unwrap();
    // }
}
