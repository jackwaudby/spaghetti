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
    /// If the edges is (i) a self edge, (ii) already exists, or (iii) from node is committed no edge is added.
    ///
    /// # Errors
    ///
    /// The operation fails if the parent node (from) is already aborted; a cascading abort.
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

    /// Given an access history, detect conflicts with a write operation, and insert edges into the graph
    /// for transaction residing in `this_node`.
    pub fn detect_write_conflicts(
        &self,
        this_node: usize,
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        for access in access_history {
            match access {
                // WW conflict
                Access::Write(tid) => {
                    let (from_node, _) = parse_id(tid);
                    self.add_edge(from_node, this_node, false)?;
                }
                // RW conflict
                Access::Read(tid) => {
                    let (from_node, _) = parse_id(tid);
                    self.add_edge(from_node, this_node, true)?;
                }
            }
        }
        Ok(())
    }

    /// Given an access history, detect conflicts with a read operation, and insert edges into the graph
    /// for transaction residing in `this_node`.
    pub fn detect_read_conflicts(
        &self,
        this_node: usize,
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        for access in access_history {
            match access {
                // WR conflict
                Access::Write(tid) => {
                    let (from_node, _) = parse_id(tid);
                    self.add_edge(from_node, this_node, false)?;
                }
                Access::Read(_) => {}
            }
        }
        Ok(())
    }

    /// Detect conflicts, insert edges, and do cycle check.
    pub fn insert_and_check(
        &self,
        this_node: usize,
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        self.detect_write_conflicts(this_node, access_history)?;
        self.reduced_depth_first_search(this_node)?;
        Ok(())
    }

    /// Attempt to commit a transaction.
    pub fn commit_check(&self, id: usize) -> Result<(), ProtocolError> {
        let node = self.get_exculsive_lock(id);
        let state = node.get_state();

        let res = match state {
            State::Active => {
                let incoming = node.has_incoming();
                if !incoming {
                    node.set_state(State::Committed); // if active and no incoming edges
                    Ok(())
                } else {
                    Err(ProtocolError::HasIncomingEdges)
                }
            }
            State::Aborted => Err(ProtocolError::CascadingAbort),
            State::Committed => unreachable!(),
        };
        drop(node);
        res
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

/// Split a transaction id into its thread id and thread-local transaction id.
pub fn parse_id(joint: String) -> (usize, u64) {
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
        let node_id: usize = thread_id.parse().unwrap(); // get node id
        let node = self.get_exculsive_lock(node_id); // get exculsive lock
        node.set_state(State::Active); // set state to active
        let (node_id, txn_id) = node.get_transaction_id();
        let transaction_id = format!("{}-{}", node_id, txn_id); // create transaction id
                                                                //        debug!("Registered transaction {}", transaction_id);

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
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;
        let (this_node, _) = parse_id(meta.get_id().unwrap());

        // --- get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row does not exist
                return Err(e);
            }
        };

        // --- get exculsive access on the row
        let mut mg = rh.lock().unwrap();
        let row = &mut *mg;

        // --- add edges
        let ah = row.get_access_history();
        if let Err(e) = self.detect_read_conflicts(this_node, ah) {
            drop(mg);
            drop(rh);
            self.abort(meta.clone()).unwrap();
            return Err(e.into()); // abort -- cascading abort
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
                let node = self.get_shared_lock(this_node);
                node.add_key(&index.get_name(), key.clone(), OperationType::Read); // register operation
                drop(node);
                let vals = res.get_values().unwrap(); // get values

                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row

                return Ok(vals);
            }
            Err(e) => {
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row
                self.abort(meta.clone()).unwrap(); // abort -- row marked for delete
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
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;
        let (this_node, txn_id) = parse_id(meta.get_id().unwrap());

        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row not found
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap();
        let row = &mut *mg;

        if !row.is_delayed() {
            match row.get_state() {
                RowState::Clean => {
                    debug!(
                        "Transaction: {}, row state: {}, others delayed: N",
                        meta.get_id().unwrap(),
                        row.get_state()
                    );

                    let ah = row.get_access_history();
                    if let Err(e) = self.insert_and_check(this_node, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // abort -- cascading abort or cycle found
                    }

                    let c: Vec<&str> = columns.iter().map(|s| s as &str).collect(); // convert to expected type
                    let current;
                    if read {
                        let res = match row.get_values(&c, "basic-sgt", &meta.get_id().unwrap()) {
                            Ok(res) => {
                                let node = self.get_shared_lock(this_node);
                                node.add_key(&index.get_name(), key.clone(), OperationType::Read); // register operation
                                drop(node);
                                res
                            }
                            Err(e) => {
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap(); // abort -- row marked for delete
                                return Err(e);
                            }
                        };
                        current = res.get_values();
                    } else {
                        current = None;
                    }

                    let (_, new_values) = match f(columns.clone(), current, params) {
                        Ok(res) => res,
                        Err(e) => {
                            drop(mg);
                            drop(rh);
                            self.abort(meta.clone()).unwrap(); // abort -- due to integrity constraint
                            return Err(e);
                        }
                    };

                    let nv: Vec<&str> = new_values.iter().map(|s| s as &str).collect();
                    row.set_values(&c, &nv, "sgt", &meta.get_id().unwrap())
                        .unwrap();

                    let node = self.get_shared_lock(this_node);
                    node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                    drop(node);
                    drop(mg);
                    drop(rh);

                    return Ok(());
                }

                RowState::Modified => {
                    debug!(
                        "Transaction: {}, Row: {}, State: {}, Others delayed: N",
                        meta.get_id().unwrap(),
                        key.clone(),
                        row.get_state()
                    );

                    let ah = row.get_access_history();
                    if let Err(e) = self.insert_and_check(this_node, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // abort -- cascading abort
                    }

                    row.append_delayed((this_node, txn_id)); // add to delayed queue

                    debug!("Transaction: {} delayed ", meta.get_id().unwrap(),);
                    drop(mg);
                    drop(rh);

                    loop {
                        let rh = match index.get_lock_on_row(key.clone()) {
                            Ok(rg) => rg,
                            Err(e) => {
                                self.abort(meta.clone()).unwrap(); // row not found
                                return Err(e);
                            }
                        };

                        let mut mg = rh.lock().unwrap();
                        let row = &mut *mg;

                        if row.resume((this_node, txn_id)) {
                            debug!("Transaction: {} resumed ", meta.get_id().unwrap(),);
                            row.remove_delayed((this_node, txn_id)); // remove from delayed queue

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort or cycle
                            }

                            let c: Vec<&str> = columns.iter().map(|s| s as &str).collect(); // convert to expected type
                            let current;
                            if read {
                                let res = match row.get_values(
                                    &c,
                                    "basic-sgt",
                                    &meta.get_id().unwrap(),
                                ) {
                                    Ok(res) => {
                                        let node = self.get_shared_lock(this_node);
                                        node.add_key(
                                            &index.get_name(),
                                            key.clone(),
                                            OperationType::Read,
                                        ); // register operation
                                        drop(node);
                                        res
                                    }
                                    Err(e) => {
                                        drop(mg);
                                        drop(rh);
                                        self.abort(meta.clone()).unwrap(); // abort -- row marked for delete
                                        return Err(e);
                                    }
                                };
                                current = res.get_values();
                            } else {
                                current = None;
                            }

                            let (_, new_values) = match f(columns.clone(), current, params) {
                                Ok(res) => res,
                                Err(e) => {
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta.clone()).unwrap(); // abort -- due to integrity constraint
                                    return Err(e);
                                }
                            };

                            let nv: Vec<&str> = new_values.iter().map(|s| s as &str).collect();
                            row.set_values(&c, &nv, "sgt", &meta.get_id().unwrap())
                                .unwrap();

                            let node = self.get_shared_lock(this_node);
                            node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                            drop(node);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                row.remove_delayed((this_node, txn_id));
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort or cycle
                            }

                            drop(mg);
                            drop(rh);
                        }
                    }
                }
                RowState::Deleted => {
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(NonFatalError::RowDeleted(
                        format!("{:?}", key.clone()),
                        table.to_string(),
                    ));
                }
            }
        } else {
            match row.get_state() {
                RowState::Clean | RowState::Modified => {
                    debug!(
                        "Transaction: {}, Row: {}, State: {}, Others delayed: Y",
                        meta.get_id().unwrap(),
                        key.clone(),
                        row.get_state()
                    );
                    let mut ah = row.get_access_history(); // get access history
                    let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                    for (node_id, txn_id) in delayed {
                        let transaction_id = format!("{}-{}", node_id, txn_id); // create transaction id
                        ah.push(Access::Write(transaction_id));
                    }

                    if let Err(e) = self.insert_and_check(this_node, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // abort -- cascading abort
                    }

                    row.append_delayed((this_node, txn_id)); // add to delayed queue; returns wait on

                    drop(mg);
                    drop(rh);

                    debug!("Transaction: {} delay", meta.get_id().unwrap(),);

                    loop {
                        let rh = match index.get_lock_on_row(key.clone()) {
                            Ok(rg) => rg,
                            Err(e) => {
                                self.abort(meta.clone()).unwrap(); // abort -- row not found
                                return Err(e);
                            }
                        };

                        let mut mg = rh.lock().unwrap();
                        let row = &mut *mg;

                        if row.resume((this_node, txn_id)) {
                            debug!(
                                "Transaction {} resumed (row: {})",
                                meta.get_id().unwrap(),
                                key.clone()
                            );
                            row.remove_delayed((this_node, txn_id));

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort
                            }

                            assert_eq!(
                                row.get_state(),
                                RowState::Clean,
                                "Transaction: {}, Row: {}",
                                meta.get_id().unwrap(),
                                row
                            );

                            let c: Vec<&str> = columns.iter().map(|s| s as &str).collect(); // convert to expected type
                            let current;
                            if read {
                                let res = match row.get_values(
                                    &c,
                                    "basic-sgt",
                                    &meta.get_id().unwrap(),
                                ) {
                                    Ok(res) => {
                                        let node = self.get_shared_lock(this_node);
                                        node.add_key(
                                            &index.get_name(),
                                            key.clone(),
                                            OperationType::Read,
                                        ); // register operation
                                        drop(node);
                                        res
                                    }
                                    Err(e) => {
                                        drop(mg);
                                        drop(rh);
                                        self.abort(meta.clone()).unwrap(); // abort -- row marked for delete
                                        return Err(e);
                                    }
                                };
                                current = res.get_values();
                            } else {
                                current = None;
                            }

                            let (_, new_values) = match f(columns.clone(), current, params) {
                                Ok(res) => res,
                                Err(e) => {
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta.clone()).unwrap(); // abort -- due to integrity constraint
                                    return Err(e);
                                }
                            };

                            let nv: Vec<&str> = new_values.iter().map(|s| s as &str).collect();
                            row.set_values(&c, &nv, "sgt", &meta.get_id().unwrap())
                                .unwrap();
                            let node = self.get_shared_lock(this_node);
                            node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                            drop(node);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                row.remove_delayed((this_node, txn_id));
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort or cycle
                            }

                            drop(mg);
                            drop(rh);
                        }
                    }
                }
                RowState::Deleted => {
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(NonFatalError::RowDeleted(
                        format!("{:?}", key.clone()),
                        table.to_string(),
                    ));
                }
            }
        }
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
        let (this_node, txn_id) = parse_id(meta.get_id().unwrap());
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rg) => rg,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // row not found
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap();
        let row = &mut *mg;

        if !row.is_delayed() {
            match row.get_state() {
                RowState::Clean => {
                    let ah = row.get_access_history();
                    if let Err(e) = self.insert_and_check(this_node, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // cascading abort or cycle found
                    }

                    row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap())
                        .unwrap(); // execute append

                    let node = self.get_shared_lock(this_node);
                    node.add_key(&index.get_name(), key.clone(), OperationType::Update); // register operation
                    drop(node);
                    drop(mg);
                    drop(rh);

                    return Ok(());
                }

                RowState::Modified => {
                    let ah = row.get_access_history(); // insert and check
                    if let Err(e) = self.insert_and_check(this_node, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // cascading abort or cycle found
                    }

                    row.append_delayed((this_node, txn_id)); // add to delayed queue

                    drop(mg);
                    drop(rh);

                    loop {
                        let rh = match index.get_lock_on_row(key.clone()) {
                            Ok(rg) => rg,
                            Err(e) => {
                                self.abort(meta.clone()).unwrap(); // row not found
                                return Err(e);
                            }
                        };

                        let mut mg = rh.lock().unwrap();
                        let row = &mut *mg;

                        if row.resume((this_node, txn_id)) {
                            row.remove_delayed((this_node, txn_id));

                            let ah = row.get_access_history(); // insert and check
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                drop(mg);
                                drop(rh);

                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // cascading abort or cycle found
                            }

                            row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap())
                                .unwrap(); // execute append ( never fails )

                            let node = self.get_shared_lock(this_node);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(node);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                row.remove_delayed((this_node, txn_id));
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort or cycle
                            }

                            drop(mg);
                            drop(rh);
                        }
                    }
                }
                RowState::Deleted => {
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(NonFatalError::RowDeleted(
                        format!("{:?}", key.clone()),
                        table.to_string(),
                    ));
                }
            }
        } else {
            match row.get_state() {
                RowState::Clean | RowState::Modified => {
                    let mut ah = row.get_access_history(); // get access history
                    let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                    for (node_id, txn_id) in delayed {
                        let transaction_id = format!("{}-{}", node_id, txn_id); // create transaction id
                        ah.push(Access::Write(transaction_id));
                    }

                    if let Err(e) = self.insert_and_check(this_node, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into());
                    }

                    row.append_delayed((this_node, txn_id)); // add to delayed queue; returns wait on

                    drop(mg);
                    drop(rh);

                    loop {
                        let rh = match index.get_lock_on_row(key.clone()) {
                            Ok(rg) => rg,
                            Err(e) => {
                                self.abort(meta.clone()).unwrap(); // abort -- row not found
                                return Err(e);
                            }
                        };

                        let mut mg = rh.lock().unwrap();
                        let row = &mut *mg;

                        if row.resume((this_node, txn_id)) {
                            row.remove_delayed((this_node, txn_id));

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort
                            }

                            row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap())
                                .unwrap();

                            let node = self.get_shared_lock(this_node);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(node);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(this_node, ah) {
                                row.remove_delayed((this_node, txn_id));
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort or cycle
                            }

                            drop(mg);
                            drop(rh);
                        }
                    }
                }
                RowState::Deleted => {
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                    return Err(NonFatalError::RowDeleted(
                        format!("{:?}", key.clone()),
                        table.to_string(),
                    ));
                }
            }
        }
    }

    /// Read (get) and update (set).
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

        // --- setup
        let (this_node, txn_id) = parse_id(meta.get_id().unwrap());
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        // --- get read handle to row in index
        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row not found.
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
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cascading abort
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cycle found
                }

                // --- execute get and set
                match row.get_and_set_values(columns, values, "basic-sgt", &meta.get_id().unwrap())
                {
                    Ok(res) => {
                        let node = self.get_shared_lock(this_node); // get shared lock
                        node.add_key(&index.get_name(), key.clone(), OperationType::Update); // register operation
                        drop(node);
                        drop(mg);
                        drop(rh);
                        let vals = res.get_values().unwrap(); // get values
                        Ok(vals)
                    }
                    Err(e) => {
                        drop(mg);
                        drop(rh);
                        self.abort(meta).unwrap(); // abort - row deleted or row dirty
                        return Err(e);
                    }
                }
            }
            RowState::Modified => {
                // --- detect conflicts
                let mut ah = row.get_access_history(); // get access history
                let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
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
                row.append_delayed((this_node, txn_id)); // add to delayed queue; returns wait on
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row

                debug!("Delay: {}", meta.get_id().unwrap());

                loop {
                    // --- get read handle to row in index
                    let rh = match index.get_lock_on_row(key.clone()) {
                        Ok(rg) => rg,
                        Err(e) => {
                            self.abort(meta.clone()).unwrap(); // abort -- row not found
                            return Err(e);
                        }
                    };

                    // --- exculsive access on the row
                    let mut mg = rh.lock().unwrap();
                    let row = &mut *mg;

                    // if txn waiting on has finished
                    if row.resume((this_node, txn_id)) {
                        debug!("Restart: {}", meta.get_id().unwrap());

                        // -- remove this_node from delayed queye
                        row.remove_delayed((this_node, txn_id));

                        // --- detect conflicts; if missed some reads
                        let ah = row.get_access_history();
                        if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                            drop(mg);
                            drop(rh);
                            self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                            return Err(e.into());
                        }

                        // --- cycle check
                        if let Err(e) = self.reduced_depth_first_search(this_node) {
                            drop(mg);
                            drop(rh);
                            self.abort(meta.clone()).unwrap(); // abort -- cycle found
                            return Err(e.into());
                        }
                        // --- execute get and set
                        match row.get_and_set_values(
                            columns,
                            values,
                            "basic-sgt",
                            &meta.get_id().unwrap(),
                        ) {
                            Ok(res) => {
                                let node = self.get_shared_lock(this_node); // get shared lock
                                node.add_key(&index.get_name(), key.clone(), OperationType::Update); // register operation
                                drop(node);
                                drop(mg);
                                drop(rh);
                                let vals = res.get_values().unwrap(); // get values
                                return Ok(vals);
                            }
                            Err(e) => {
                                drop(mg);
                                drop(rh);
                                self.abort(meta).unwrap(); // abort -- row deleted or row dirty
                                return Err(e.into());
                            }
                        }
                    }
                }
            }
            RowState::Deleted => {
                drop(mg);
                drop(rh);
                self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                return Err(NonFatalError::RowDeleted(
                    format!("{:?}", key.clone()),
                    table.to_string(),
                ));
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
        // --- setup
        let (this_node, txn_id) = parse_id(meta.get_id().unwrap());
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

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

        match row.get_state() {
            RowState::Clean => {
                // --- detect conflicts
                let ah = row.get_access_history(); // get access history
                if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cascading abort
                }

                // --- cycle check
                if let Err(e) = self.reduced_depth_first_search(this_node) {
                    drop(mg);
                    drop(rh);
                    self.abort(meta.clone()).unwrap();
                    return Err(e.into()); // abort -- cycle found
                }

                // --- execute delete
                match row.delete("basic-sgt") {
                    Ok(_) => {
                        let node = self.get_shared_lock(this_node); // get shared lock
                        node.add_key(&index.get_name(), key, OperationType::Delete); // operation succeeded -- register
                        drop(node);
                        drop(mg);
                        drop(rh);
                        return Ok(());
                    }
                    Err(e) => {
                        drop(mg);
                        drop(rh);
                        self.abort(meta).unwrap(); // abort -- row deleted or row dirty
                        Err(e)
                    }
                }
            }
            RowState::Modified => {
                // --- detect conflicts
                let mut ah = row.get_access_history(); // get access history
                let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
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
                row.append_delayed((this_node, txn_id)); // add to delayed queue; returns wait on
                drop(mg); // drop mutex on row
                drop(rh); // drop read handle to row

                loop {
                    let rh = match index.get_lock_on_row(key.clone()) {
                        Ok(rg) => rg,
                        Err(e) => {
                            self.abort(meta.clone()).unwrap(); // abort -- row not found
                            return Err(e);
                        }
                    };

                    let mut mg = rh.lock().unwrap();
                    let row = &mut *mg;

                    // if txn waiting on has finished
                    if row.resume((this_node, txn_id)) {
                        // -- remove this_node from delayed queye
                        row.remove_delayed((this_node, txn_id));

                        // --- detect conflicts; if missed some reads
                        let ah = row.get_access_history();
                        if let Err(e) = self.detect_write_conflicts(this_node, ah) {
                            drop(mg);
                            drop(rh);
                            self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                            return Err(e.into());
                        }

                        // --- cycle check
                        if let Err(e) = self.reduced_depth_first_search(this_node) {
                            drop(mg);
                            drop(rh);
                            self.abort(meta.clone()).unwrap(); // abort -- cycle found
                            return Err(e.into());
                        }
                        // --- execute delete
                        match row.delete("basic-sgt") {
                            Ok(_) => {
                                let node = self.get_shared_lock(this_node); // get shared lock
                                node.add_key(&index.get_name(), key, OperationType::Delete); // operation succeeded -- register
                                drop(node);
                                drop(mg);
                                drop(rh);
                                return Ok(());
                            }
                            Err(e) => {
                                drop(mg);
                                drop(rh);
                                self.abort(meta).unwrap(); // abort -- row deleted or row dirty
                                return Err(e.into());
                            }
                        }
                    }
                }
            }
            RowState::Deleted => {
                drop(mg);
                drop(rh);
                self.abort(meta.clone()).unwrap(); // abort -- cascading abort
                return Err(NonFatalError::RowDeleted(
                    format!("{:?}", key.clone()),
                    table.to_string(),
                ));
            }
        }
    }

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        //  debug!("Starting abort procedure for {}", meta.get_id().unwrap());
        let (this_node_id, _) = parse_id(meta.get_id().unwrap());

        {
            let wlock = self.get_exculsive_lock(this_node_id);
            wlock.set_state(State::Aborted); // set state to aborted
            drop(wlock);
        }

        let rlock = self.get_shared_lock(this_node_id);
        let inserts = rlock.get_keys(OperationType::Insert);
        let reads = rlock.get_keys(OperationType::Read);
        let updates = rlock.get_keys(OperationType::Update);
        let deletes = rlock.get_keys(OperationType::Delete);
        drop(rlock);

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
                //    debug!("Row after read revert: {}", row);

                drop(mg);
                drop(rh);
            };
        }
        //    info!("Reverting updates for {}", meta.get_id().unwrap());

        for (index, key) in &updates {
            let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

            // get read handle to row
            if let Ok(rh) = index.get_lock_on_row(key.clone()) {
                let mut mg = rh.lock().unwrap(); // acquire mutex on the row

                let row = &mut *mg; // deref to row

                row.revert("sgt", &meta.get_id().unwrap());
                //     debug!("Row after update revert: {}", row);

                drop(mg);
                drop(rh);
            };
        }
        // info!("Updates reverted for {}", meta.get_id().unwrap());

        for (index, key) in &deletes {
            let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

            // get read handle to row
            if let Ok(rh) = index.get_lock_on_row(key.clone()) {
                let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                let row = &mut *mg; // deref to row
                row.revert("sgt", &meta.get_id().unwrap());
                drop(mg);
                drop(rh);
            };
        }
        // info!("Clean up for {}", meta.get_id().unwrap());

        self.clean_up_graph(this_node_id); // abort outgoing nodes
                                           //        info!("Cleaned up for {}", meta.get_id().unwrap());

        {
            self.get_exculsive_lock(this_node_id).reset(); // reset node information
        }

        //        debug!("Graph after aborting {}: {}", meta.get_id().unwrap(), &self);
        debug!("Transaction {} terminated (abort)", meta.get_id().unwrap());
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        //      debug!("Start commit for: {}", meta.get_id().unwrap());

        let (id, _) = parse_id(meta.get_id().unwrap());

        while let Err(ProtocolError::HasIncomingEdges) = self.commit_check(id) {
            if let Err(ProtocolError::CycleFound) = self.reduced_depth_first_search(id) {
                let rlock = self.get_shared_lock(id);
                rlock.set_state(State::Aborted);
                drop(rlock);
                self.abort(meta.clone()).unwrap();
            }
        }

        let rlock = self.get_shared_lock(id); // take shared lock on this_node
        let state = rlock.get_state(); // get this_node state
        drop(rlock);

        match state {
            State::Aborted => {
                self.abort(meta.clone()).unwrap();
                return Err(ProtocolError::CascadingAbort.into());
            }
            State::Committed => {
                self.clean_up_graph(id); // remove outgoing edges
                let sl = self.get_shared_lock(id); // get shared lock
                let inserts = sl.get_keys(OperationType::Insert);
                let reads = sl.get_keys(OperationType::Read);
                let updates = sl.get_keys(OperationType::Update);
                let deletes = sl.get_keys(OperationType::Delete);
                drop(sl); // drop shared lock

                for (index, key) in inserts {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.commit("basic-sgt", &meta.get_id().unwrap()); // commit inserts
                    drop(mg);
                    drop(rh);
                }

                for (index, key) in &reads {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

                    // get read handle to row
                    if let Ok(rh) = index.get_lock_on_row(key.clone()) {
                        let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                        let row = &mut *mg; // deref to row
                        row.revert_read(&meta.get_id().unwrap());
                        //      debug!("Row after read commit: {}", row);

                        drop(mg);
                        drop(rh);
                    };
                }

                for (index, key) in updates {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.commit("basic-sgt", &meta.get_id().unwrap()); // commit inserts
                                                                      //        debug!("Row after update commit: {}", row);
                    drop(mg);
                    drop(rh);
                }

                for (index, key) in deletes {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    index.get_map().remove(&key); // Remove the row from the map.
                }

                {
                    self.get_exculsive_lock(id).reset();
                }
                // debug!(
                //     "Graph after committing {}: {}",
                //     meta.get_id().unwrap(),
                //     &self
                // );
            }
            State::Active => panic!("node should not be active"),
        }
        debug!("Transaction {} terminated (commit)", meta.get_id().unwrap());

        Ok(())
    }

    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}
