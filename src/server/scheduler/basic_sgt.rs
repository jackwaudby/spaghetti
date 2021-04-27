//! Basic-SGT; cycle check performed after each edge insertion

use crate::common::error::NonFatalError;
use crate::server::scheduler::basic_sgt::error::BasicSerializationGraphTestingError as ProtocolError;
use crate::server::scheduler::basic_sgt::node::{EdgeType, Node, OperationType, State};
use crate::server::scheduler::{Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{Access, Row, State as RowState};
use crate::workloads::{PrimaryKey, Workload};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use tracing::{debug, info};

pub mod node;

pub mod error;

/// Basic Serialization Graph Testing
#[derive(Debug)]
pub struct BasicSerializationGraphTesting {
    /// Graph.
    nodes: Vec<RwLock<Node>>,

    /// Handle to storage layer.
    data: Arc<Workload>,
}

impl BasicSerializationGraphTesting {
    /// Initialise serialization graph with `size` nodes.
    pub fn new(size: u32, data: Arc<Workload>) -> Self {
        info!("Initialise basic serialization graph with {} nodes", size);
        let mut nodes = vec![];
        for i in 0..size {
            let node = RwLock::new(Node::new(i as usize));
            nodes.push(node);
        }
        BasicSerializationGraphTesting { nodes, data }
    }

    /// Get shared lock on the node.
    fn get_shared_lock(&self, id: usize) -> RwLockReadGuard<Node> {
        let rg = self.nodes[id].read();
        rg
    }

    /// Get exculsive lock on the node.
    fn get_exculsive_lock(&self, id: usize) -> RwLockWriteGuard<Node> {
        let wg = self.nodes[id].write();
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
    pub fn add_edge(&self, from: usize, to: usize, rw_edge: bool) -> Result<(), ProtocolError> {
        if from == to {
            return Ok(()); // don't add self edges
        }

        let from_node = self.get_shared_lock(from); // get shared locks
        let to_node = self.get_shared_lock(to);

        match from_node.get_state() {
            State::Aborted => {
                if !rw_edge {
                    return Err(ProtocolError::CascadingAbort); // w-w/w-r; cascading abort
                }
            }
            State::Active => {
                from_node.insert_edge(to, EdgeType::Outgoing); // insert edge
                to_node.insert_edge(from, EdgeType::Incoming);
            }
            State::Committed => {}
        }

        drop(from_node); // drop shared locks
        drop(to_node);
        Ok(())
    }

    pub fn detect_write_conflicts(
        &self,
        this_node: usize,
        access_history: Vec<Access>,
    ) -> Result<(), NonFatalError> {
        for access in access_history {
            match access {
                // WW conflict
                Access::Write(tid) => {
                    let from_node: usize = tid.parse().unwrap();
                    if let Err(e) = self.add_edge(from_node, this_node, false) {
                        return Err(e.into()); // abort -- cascading abort
                    }
                }
                // RW conflict
                Access::Read(tid) => {
                    let from_node: usize = tid.parse().unwrap();
                    if let Err(e) = self.add_edge(from_node, this_node, true) {
                        return Err(e.into()); // abort -- cascading abort
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns `true` if node was committed.
    pub fn commit_check(&self, id: usize) -> bool {
        let node = self.get_exculsive_lock(id); // get exculsive lock
        if !node.has_incoming() && node.get_state() == State::Active {
            node.set_state(State::Committed);
            return true;
        }
        false
    }

    /// Perform a reduced depth first search from `start` node.
    pub fn reduced_depth_first_search(&self, start: usize) -> Result<(), ProtocolError> {
        let mut stack = Vec::new(); // nodes to visit
        let mut visited = HashSet::new(); // nodes visited

        let start_node = self.get_shared_lock(start); // get shared lock on start node
        stack.append(&mut start_node.get_outgoing()); // push outgoing to stack
        drop(start_node); // drop shared lock

        // pop until no more nodes to visit
        while let Some(current) = stack.pop() {
            if current == start {
                return Err(ProtocolError::CycleFound); // cycle found
            }

            if visited.contains(&current) {
                continue; // already visited
            }

            visited.insert(current); // mark as visited
            let current_node = self.get_shared_lock(current); // get shared lock on current_node.

            if let State::Active = current_node.get_state() {
                for child in current_node.get_outgoing() {
                    if child == start {
                        return Err(ProtocolError::CycleFound); // outgoing edge to start node -- cycle found
                    } else {
                        let child_node = self.get_shared_lock(child); // get read lock on child_node
                        visited.insert(child); // mark as visited
                        stack.append(&mut child_node.get_outgoing()); // add outgoing to stack
                        drop(child_node);
                    }
                }
            }
            drop(current_node);
        }
        Ok(()) // no cycle found
    }

    /// Clean up graph.
    ///
    /// If node with `id` aborted then abort outgoing nodes before removing edges.
    /// Else; node committed, remove outgoing edges.
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

fn parse_id(joint: String) -> (usize, u64) {
    let split = joint.split("-");
    let vec: Vec<usize> = split.map(|x| x.parse::<usize>().unwrap()).collect();
    (vec[0], vec[1] as u64)
}

impl Scheduler for BasicSerializationGraphTesting {
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
        let (node_id, txn_id) = node.get_transaction_id();

        let transaction_id = format!("{}-{}", node_id, txn_id); // create transaction id

        Ok(TransactionInfo::new(Some(transaction_id), None))
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
    fn read(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        // -- setup
        let th = thread::current(); // get handle to thread
        let thread_id = th.name().unwrap(); // get thread id
        debug!("Thread {}: Executing read operation", thread_id);

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        let (this_node, txn_id) = parse_id(meta.get_id().unwrap());

        // --- get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row does not exist
                return Err(e);
            }
        };

        // --- get exculsive access on the row
        let mut mg = rh.lock().unwrap(); // acquire mutex on the row
        let row = &mut *mg; // deref to row

        // --- add edges
        let ah = row.get_access_history(); // get access history --- every previously scheduled operation
        for access in ah {
            if let Access::Write(tid) = access {
                let from_node: usize = tid.parse().unwrap(); // get from_node position in sgt
                if let Err(e) = self.add_edge(from_node, this_node, false) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(e.into());
                }
            }
        }

        // --- cycle check
        if let Err(e) = self.reduced_depth_first_search(this_node) {
            drop(mg); // drop mutex on row
            drop(rh); // drop read handle to row
            self.abort(meta.clone()).unwrap(); // abort -- cycle found
            return Err(e.into());
        }

        // --- execute read
        match row.get_values(columns, "basic-sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                row.append_access(Access::Read(meta.get_id().unwrap().clone())); // add to access history
                let node = self.get_shared_lock(this_node); // take shared lock on this_node
                node.add_key(&index.get_name(), key.clone(), OperationType::Read); // register operation
                drop(node); // drop shared lock on node
                let vals = res.get_values().unwrap(); // get values
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                return Ok(vals);
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

        let (this_node, txn_id) = parse_id(meta.get_id().unwrap());
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        // --- get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row not found
                return Err(e);
            }
        };

        // --- exculsive access on the row
        let mut mg = rh.lock().unwrap(); // get mutex on row
        let row = &mut *mg; // deref to row

        match row.get_state() {
            RowState::Clean => {
                // --- detect conflicts
                let ah = row.get_access_history(); // get access history
                if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cascading abort
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cycle found
                }

                // --- read current values (optional)
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

                // --- compute new values.
                let (_, new_values) = match f(columns.clone(), current, params) {
                    Ok(res) => res,
                    Err(e) => {
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        self.abort(meta.clone()).unwrap(); // abort -- row not found
                        return Err(e);
                    }
                };

                // --- execute update
                let nv: Vec<&str> = new_values.iter().map(|s| s as &str).collect(); // convert to expected type
                match row.set_values(&c, &nv, "sgt", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let node = self.get_shared_lock(this_node);
                        node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                        drop(node); // drop shared lock on node
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        return Ok(());
                    }
                    Err(e) => {
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        self.abort(meta).unwrap(); // abort -- append error
                        return Err(e);
                    }
                }
            }
            // w-w conflict; delay operation
            RowState::Modified => {
                // --- detect conflicts
                let mut ah = row.get_access_history(); // get access history
                let mut delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                for (node_id, txn_id) in delayed {
                    let transaction_id = format!("{}-{}", node_id, txn_id); // create transaction id
                    ah.push(Access::Write(transaction_id));
                }
                if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(e.into());
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cycle found
                    return Err(e.into());
                }

                // --- delay and drop locks
                let (wait_th, wait_t) = row.add_delayed((this_node, txn_id)); // add to delayed queue; returns wait on
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row

                while !self.get_shared_lock(wait_th).has_terminated(wait_t) {}
                // -- remove from delayed queye
                row.remove_delayed((this_node, txn_id));

                // --- detect conflicts
                let ah = row.get_access_history(); // get access history --- every previously scheduled operation
                if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(e.into());
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cycle found
                    return Err(e.into());
                }

                // --- read current values (optional)
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

                // --- compute new values.
                let (_, new_values) = match f(columns.clone(), current, params) {
                    Ok(res) => res,
                    Err(e) => {
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        self.abort(meta.clone()).unwrap(); // abort -- row not found
                        return Err(e);
                    }
                };

                // --- execute update
                let nv: Vec<&str> = new_values.iter().map(|s| s as &str).collect(); // convert to expected type
                match row.set_values(&c, &nv, "sgt", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let node = self.get_shared_lock(this_node);
                        node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                        drop(node); // drop shared lock on node
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        return Ok(());
                    }
                    Err(e) => {
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        self.abort(meta).unwrap(); // abort -- append error
                        return Err(e);
                    }
                }
            }
            RowState::Deleted => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                return Err(NonFatalError::RowDeleted(
                    "todo".to_string(),
                    "todo".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Append `value` to `column`.
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

        // --- setup
        let (this_node, txn_id) = parse_id(meta.get_id().unwrap());
        let table = self.get_table(table, meta.clone())?; // get table handle
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index handle

        // --- get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row not found
                return Err(e);
            }
        };

        // --- exculsive access on the row
        let mut mg = rh.lock().unwrap(); // get mutex on row
        let row = &mut *mg; // deref to row

        match row.get_state() {
            RowState::Clean => {
                // --- detect conflicts
                let ah = row.get_access_history(); // get access history
                if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cascading abort
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cycle found
                }

                // --- execute append
                match row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let node = self.get_shared_lock(this_node);
                        node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                        drop(node); // drop shared lock on node
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        return Ok(());
                    }
                    Err(e) => {
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        self.abort(meta).unwrap(); // abort -- append error
                        return Err(e);
                    }
                }
            }
            // w-w conflict; delay operation
            RowState::Modified => {
                // --- detect conflicts
                let mut ah = row.get_access_history(); // get access history
                let mut delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                for (node_id, txn_id) in delayed {
                    let transaction_id = format!("{}-{}", node_id, txn_id); // create transaction id
                    ah.push(Access::Write(transaction_id));
                }
                if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(e.into());
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cycle found
                    return Err(e.into());
                }

                // --- delay and drop locks
                let (wait_th, wait_t) = row.add_delayed((this_node, txn_id)); // add to delayed queue; returns wait on
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row

                while !self.get_shared_lock(wait_th).has_terminated(wait_t) {}
                // -- remove from delayed queye
                row.remove_delayed((this_node, txn_id));

                // --- detect conflicts
                let ah = row.get_access_history(); // get access history --- every previously scheduled operation
                if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(e.into());
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg); // drop mutex on row
                    drop(rh); // drop read handle to row
                    self.abort(meta.clone()).unwrap(); // abort -- cycle found
                    return Err(e.into());
                }

                // --- execute append
                match row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let node = self.get_shared_lock(this_node);
                        node.add_key(&index.get_name(), key, OperationType::Update); // operation succeeded -- register
                        drop(node);
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        return Ok(());
                    }
                    Err(e) => {
                        drop(mg); // drop mutex on row
                        drop(rh); // drop read handle to row
                        self.abort(meta).unwrap(); // abort
                        return Err(e);
                    }
                }
            }
            RowState::Deleted => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                return Err(NonFatalError::RowDeleted(
                    "todo".to_string(),
                    "todo".to_string(),
                ));
            }
        }
        Ok(())
    }

    // /// Read (get) and update (set).
    // ///
    // /// Adds an edge in the graph for each WW and RW conflict.
    // fn read_and_update(
    //     &self,
    //     table: &str,
    //     key: PrimaryKey,
    //     columns: &Vec<&str>,
    //     values: &Vec<&str>,
    //     meta: TransactionInfo,
    // ) -> Result<Vec<Data>, NonFatalError> {
    //     let handle = thread::current();
    //     debug!(
    //         "Thread {}: Executing read and update operation",
    //         handle.name().unwrap()
    //     );

    //     let table = self.get_table(table, meta.clone())?; // get table
    //     let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

    //     let rh = match index.get_lock_on_row(key.clone()) {
    //         Ok(rh) => rh,
    //         Err(e) => {
    //             self.abort(meta.clone()).unwrap(); // abort -- row not found.
    //             return Err(e);
    //         }
    //     };

    //     let mut mg = rh.lock().unwrap(); // get mutex on row
    //     let row = &mut *mg; // deref to row

    //     // get and set values
    //     match row.get_and_set_values(columns, values, "sgt", &meta.get_id().unwrap()) {
    //         Ok(res) => {
    //             let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // Get position of this transaction in the graph.
    //             let node = self.get_shared_lock(this_node); // get shared lock
    //             node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
    //             drop(node);

    //             let access_history = res.get_access_history(); // get access history

    //             for access in access_history {
    //                 match access {
    //                     // WW conflict
    //                     Access::Write(tid) => {
    //                         let from_node: usize = tid.parse().unwrap(); // Get position of conflicting transaction in the graph.

    //                         // insert edges
    //                         if let Err(e) = self.add_edge(from_node, this_node, false) {
    //                             drop(mg); // drop mutex on row
    //                             drop(rh); // drop read handle to row
    //                             self.abort(meta.clone()).unwrap(); // abort -- parent aborted.
    //                             return Err(e.into());
    //                         }
    //                     }
    //                     // RW conflict
    //                     Access::Read(tid) => {
    //                         let from_node: usize = tid.parse().unwrap();

    //                         // insert edges
    //                         if let Err(e) = self.add_edge(from_node, this_node, true) {
    //                             drop(mg); // drop mutex on row
    //                             drop(rh); // drop read handle to row
    //                             self.abort(meta.clone()).unwrap(); // abort -- parent aborted.
    //                             return Err(e.into());
    //                         }
    //                     }
    //                 }
    //             }
    //             let vals = res.get_values().unwrap(); // get values
    //             Ok(vals)
    //         }
    //         Err(e) => {
    //             drop(mg); // drop mutex on row
    //             drop(rh); // drop read handle to row
    //             self.abort(meta).unwrap(); // abort - row deleted or row dirty
    //             return Err(e);
    //         }
    //     }
    // }

    // /// Delete from row.
    // fn delete(
    //     &self,
    //     table: &str,
    //     key: PrimaryKey,
    //     meta: TransactionInfo,
    // ) -> Result<(), NonFatalError> {
    //     let handle = thread::current();
    //     debug!(
    //         "Thread {}: Executing delete operation",
    //         handle.name().unwrap()
    //     );

    //     let table = self.get_table(table, meta.clone())?; // get table
    //     let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

    //     // get read handle to row in index
    //     let rh = match index.get_lock_on_row(key.clone()) {
    //         Ok(rh) => rh,
    //         Err(e) => {
    //             self.abort(meta.clone()).unwrap(); // abort - row not found
    //             return Err(e);
    //         }
    //     };

    //     let mut mg = rh.lock().unwrap(); // get mutex on row
    //     let row = &mut *mg; // deref to row

    //     match row.delete("sgt") {
    //         Ok(res) => {
    //             let this_node = meta.get_id().unwrap().parse::<usize>().unwrap(); // get position of this transaction in the graph
    //             let node = self.get_shared_lock(this_node); // get shared lock
    //             node.add_key(&index.get_name(), key, OperationType::Delete); // operation succeeded -- register
    //             drop(node);

    //             let access_history = res.get_access_history(); // get the access history

    //             // detect conflicts
    //             for access in access_history {
    //                 match access {
    //                     // WW conflict
    //                     Access::Write(tid) => {
    //                         let from_node: usize = tid.parse().unwrap(); // get from_node id
    //                         if let Err(e) = self.add_edge(from_node, this_node, false) {
    //                             drop(mg); // drop mutex on row
    //                             drop(rh); // drop read handle to row
    //                             self.abort(meta.clone()).unwrap();
    //                             return Err(e.into());
    //                         }
    //                     }
    //                     // RW conflict
    //                     Access::Read(tid) => {
    //                         let from_node: usize = tid.parse().unwrap(); // get from_node id
    //                         if let Err(e) = self.add_edge(from_node, this_node, true) {
    //                             drop(mg); // drop mutex on row
    //                             drop(rh); // drop read handle to row
    //                             self.abort(meta.clone()).unwrap();
    //                             return Err(e.into());
    //                         }
    //                     }
    //                 }
    //             }

    //             Ok(())
    //         }
    //         Err(e) => {
    //             drop(mg); // drop mutex on row
    //             drop(rh); // drop read handle to row
    //             self.abort(meta).unwrap(); // abort -- row deleted or row dirty
    //             Err(e)
    //         }
    //     }
    // }

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
                    if let Err(ProtocolError::CycleFound) = cycle_check {
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
                let e = ProtocolError::ParentAborted;
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
                    index.get_map().remove(&key); // Remove the row from the map.

                    // let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    // let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    // let row = &mut *mg; // deref to row
                    // row.delete("sgt").unwrap(); // commit delete
                    // drop(mg);
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
