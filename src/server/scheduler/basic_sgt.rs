use crate::common::error::NonFatalError;
use crate::server::scheduler::basic_sgt::error::BasicSerializationGraphTestingError as ProtocolError;
use crate::server::scheduler::basic_sgt::node::{EdgeType, NodeSet, OperationType, State};
use crate::server::scheduler::{Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{Access, Row, State as RowState};

use crate::workloads::{PrimaryKey, Workload};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
//use no_deadlocks::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashSet;
use std::sync::Arc; //, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{fmt, thread};
use tracing::{debug, info};

pub mod node;

pub mod error;

/// Basic Serialization Graph Testing
#[derive(Debug)]
pub struct BasicSerializationGraphTesting {
    /// Graph.
    nodes: Vec<RwLock<NodeSet>>,

    /// Handle to storage layer.
    data: Arc<Workload>,
}

impl BasicSerializationGraphTesting {
    /// Initialise serialization graph with `size` nodes.
    pub fn new(size: u32, data: Arc<Workload>) -> Self {
        info!("Initialise basic serialization graph with {} nodes", size);
        let mut nodes = vec![];
        for i in 0..size {
            let node = RwLock::new(NodeSet::new(i as usize));
            nodes.push(node);
        }
        BasicSerializationGraphTesting { nodes, data }
    }

    /// Get shared lock on the node.
    fn get_shared_lock(&self, id: usize) -> RwLockReadGuard<NodeSet> {
        let rg = self.nodes[id].read();
        rg
    }

    /// Get exculsive lock on the node.
    fn get_exculsive_lock(&self, id: usize) -> RwLockWriteGuard<NodeSet> {
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
    pub fn add_edge(
        &self,
        from: (usize, u64),
        to: (usize, u64),
        rw_edge: bool,
    ) -> Result<(), ProtocolError> {
        let (from_thread, from_txn_id) = from;
        let (to_thread, to_txn_id) = to;

        if from == to {
            return Ok(()); // don't add self edges
        }
        debug!("ADD - {:?} request for shared lock on {:?}", to, from);
        let from_node = self.get_shared_lock(from_thread); // get shared locks
        debug!("ADD - {:?} got shared lock on {:?}", to, from);

        debug!("ADD - {:?} request for shared lock on {:?}", to, to);
        let to_node = self.get_shared_lock(to_thread);
        debug!("ADD - {:?} got shared lock on {:?}", to, to);

        match from_node.get_transaction(from_txn_id).get_state() {
            State::Aborted => {
                if !rw_edge {
                    drop(from_node); // drop shared locks
                    debug!("ADD - {:?} drop shared lock on {:?}", to, from);
                    drop(to_node);
                    debug!("ADD - {:?} drop shared lock on {:?}", to, to);
                    return Err(ProtocolError::CascadingAbort); // w-w/w-r; cascading abort
                }
            }
            State::Active => {
                from_node
                    .get_transaction(from_txn_id)
                    .insert_edge(to, EdgeType::Outgoing); // insert edge
                to_node
                    .get_transaction(to_txn_id)
                    .insert_edge(from, EdgeType::Incoming);
            }
            State::Committed => {}
        }

        drop(from_node); // drop shared locks
        debug!("ADD - {:?} drop shared lock on {:?}", to, from);
        drop(to_node);
        debug!("ADD - {:?} drop shared lock on {:?}", to, to);

        Ok(())
    }

    /// Given an access history, detect conflicts with a write operation, and insert edges into the graph
    /// for transaction residing in `this_node`.
    pub fn detect_write_conflicts(
        &self,
        this_node: (usize, u64),
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        for access in access_history {
            match access {
                // WW conflict
                Access::Write(tid) => {
                    let from_node = parse_id(tid);
                    self.add_edge(from_node, this_node, false)?;
                }
                // RW conflict
                Access::Read(tid) => {
                    let from_node = parse_id(tid);
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
        this_node: (usize, u64),
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        for access in access_history {
            match access {
                // WR conflict
                Access::Write(tid) => {
                    let from_node = parse_id(tid);
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
        this_node: (usize, u64),
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        self.detect_write_conflicts(this_node, access_history)?;
        self.reduced_depth_first_search(this_node)?;
        Ok(())
    }

    /// Attempt to commit a transaction.
    pub fn commit_check(&self, id: (usize, u64)) -> Result<(), ProtocolError> {
        let (thread_id, txn_id) = id;
        debug!("Commit - {:?} Request for exculsive lock on {:?}", id, id);
        let wlock = self.get_exculsive_lock(thread_id);
        debug!("Commit - {:?} Got exculsive lock on {:?}", id, id);
        let node = wlock.get_transaction(txn_id);
        let state = node.get_state();

        match state {
            State::Active => {
                let incoming = node.has_incoming();
                if !incoming {
                    node.set_state(State::Committed); // if active and no incoming edges
                    debug!("commit check on {:?}: committed", id);
                    drop(wlock);
                    debug!("Commit - {:?} Drop exculsive lock on {:?}", id, id);

                    return Ok(());
                } else {
                    debug!("commit check on {:?}: has incoming edges", id);
                    drop(wlock);
                    debug!("Commit - {:?} Drop exculsive lock on {:?}", id, id);
                    return Err(ProtocolError::HasIncomingEdges);
                }
            }
            State::Aborted => {
                drop(wlock);
                debug!("Commit - {:?} Drop exculsive lock on {:?}", id, id);

                return Err(ProtocolError::CascadingAbort);
            }
            State::Committed => unreachable!(),
        }
    }

    /// Perform a reduced depth first search from `start` node.
    pub fn reduced_depth_first_search(&self, start: (usize, u64)) -> Result<(), ProtocolError> {
        let (thread_id, txn_id) = start;
        //      debug!("DFS - {} start", start);

        let mut stack = Vec::new(); // nodes to visit
        let mut visited = HashSet::new(); // nodes visited

        //        debug!("DFS - {} request for shared lock on {}", start, start);
        let rlock = self.get_shared_lock(thread_id); // get shared lock on start node
                                                     //        debug!("DFS - {} got shared lock on {}", start, start);
        let start_node = rlock.get_transaction(txn_id);
        stack.append(&mut start_node.get_outgoing()); // push outgoing to stack
        drop(rlock); // drop shared lock
                     //        debug!("DFS - {} drop for shared lock on {}", start, start);

        // pop until no more nodes to visit
        while let Some(current) = stack.pop() {
            if current == start {
                //                debug!("DFS - {} finish", start);

                return Err(ProtocolError::CycleFound); // cycle found
            }

            if visited.contains(&current) {
                continue; // already visited
            }

            visited.insert(current); // mark as visited
                                     //            debug!("DFS - {} request shared lock on {}", start, current);
            let (thread_id, txn_id) = current;
            let rlock = self.get_shared_lock(thread_id); // get shared lock on current_node.
            let current_node = rlock.get_transaction(txn_id);
            //          debug!("DFS - {} got shared lock on {}", start, current);
            let cs = current_node.get_state();
            let cc = current_node.get_outgoing();
            drop(rlock);
            if let State::Active = cs {
                for child in cc {
                    if child == start {
                        //         debug!("DFS - {} drop shared lock on {}", start, current);
                        //                        debug!("DFS - {} finish", start);
                        return Err(ProtocolError::CycleFound); // outgoing edge to start node -- cycle found
                    } else {
                        //       debug!("DFS - {} request shared lock on {}", start, child);
                        let (thread_id, txn_id) = child;

                        let rlock = self.get_shared_lock(thread_id); // get read lock on child_node
                        let child_node = rlock.get_transaction(txn_id);
                        //         debug!("DFS - {} got shared lock on {}", start, child);

                        visited.insert(child); // mark as visited
                        stack.append(&mut child_node.get_outgoing()); // add outgoing to stack
                        drop(rlock);
                        //         debug!("DFS - {} drop shared lock on {}", start, child);
                    }
                }
            }

            //      debug!("DFS - {} drop shared lock on {}", start, current);
        }
        //    debug!("DFS - {} finish", start);

        Ok(()) // no cycle found
    }

    /// Clean up graph.
    ///
    /// If node with `id` aborted then abort outgoing nodes before removing edges.
    /// Else; node committed, remove outgoing edges.
    fn clean_up_graph(&self, id: (usize, u64)) {
        //        debug!("CLEAN - {} request shared lock on {}", id, id);
        let (thread_id, txn_id) = id;
        let rlock1 = self.get_shared_lock(thread_id); // get shared lock on this_node
        let this_node = rlock1.get_transaction(txn_id);
        //        debug!("CLEAN - {} got shared lock on {}", id, id);

        let state = this_node.get_state(); // get state of this_node
        let outgoing_nodes = this_node.get_outgoing(); // get outgoing edges

        for out in outgoing_nodes {
            //            debug!("CLEAN - {} request shared lock on {}", id, out);
            let (thread_id, txn_id) = out;

            let rlock2 = self.get_shared_lock(thread_id); // get shared lock on outgoing node
            let outgoing_node = rlock2.get_transaction(txn_id);
            //          debug!("CLEAN - {} got shared lock on {}", id, out);

            // if node aborted; then abort children
            if let State::Aborted = state {
                if outgoing_node.get_state() == State::Active {
                    outgoing_node.set_state(State::Aborted); // cascading abort
                }
            }

            outgoing_node.delete_edge(id, EdgeType::Incoming); // remove incoming edge from out
            this_node.delete_edge(out, EdgeType::Outgoing); // remove outgoing edge from this_node

            drop(rlock2); // drop shared lock on outgoing node
                          //            debug!("CLEAN - {} drop shared lock on {}", id, out);
        }
        drop(rlock1); // drop shared lock on this_node
                      //      debug!("CLEAN - {} drop shared lock on {}", id, id);
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

        //        debug!("REG - {} request exculsive lock on {}", node_id, node_id);
        let mut wlock = self.get_exculsive_lock(node_id); // get exculsive lock
                                                          //        debug!("REG - {} got exculsive lock on {}", node_id, node_id);
        let (node_id, txn_id) = wlock.create_node();

        let transaction_id = format!("{}-{}", node_id, txn_id); // create transaction id
        debug!("Registered transaction {}", transaction_id);
        drop(wlock);
        //      debug!("REG - {} drop exculsive lock on {}", node_id, node_id);

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
        let mut row = Row::new(Arc::clone(&table), "basic-sgt"); // create new row
        row.set_primary_key(key.clone()); // set pk
        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;

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
        if let Err(e) = row.set_values(columns, values, "basic-sgt", &meta.get_id().unwrap()) {
            self.abort(meta.clone()).unwrap(); // abort -- unable to convert to datatype
            return Err(e);
        }

        match index.insert(key.clone(), row) {
            Ok(_) => {
                let rlock = self.get_shared_lock(thread_id); // get shared lock
                let node = rlock.get_transaction(txn_id);
                node.add_key(&index.get_name(), key.clone(), OperationType::Insert); // operation succeeded -- register
                drop(rlock); // drop shared lock
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
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;
        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;

        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort -- row does not exist
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap();
        let row = &mut *mg;
        debug!(
            "Transaction {}, read key: {}, row: {}",
            meta.get_id().unwrap(),
            key.clone(),
            row
        );

        let ah = row.get_access_history();
        if let Err(e) = self.detect_read_conflicts(id, ah) {
            drop(mg);
            drop(rh);
            self.abort(meta.clone()).unwrap();

            return Err(e.into()); // abort -- cascading abort
        }

        if let Err(e) = self.reduced_depth_first_search(id) {
            drop(mg);
            drop(rh);

            self.abort(meta.clone()).unwrap(); // abort -- cycle found
            return Err(e.into());
        }

        match row.get_values(columns, "basic-sgt", &meta.get_id().unwrap()) {
            Ok(res) => {
                let rlock = self.get_shared_lock(thread_id);
                let node = rlock.get_transaction(txn_id);
                node.add_key(&index.get_name(), key.clone(), OperationType::Read);
                drop(rlock);
                let vals = res.get_values().unwrap();
                drop(mg);
                drop(rh);
                debug!(
                    "Transaction {} read key: {}",
                    meta.get_id().unwrap(),
                    key.clone(),
                );
                return Ok(vals);
            }
            Err(e) => {
                drop(mg);
                drop(rh);
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
        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;

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
                        "Transaction: {}, Key: {}, State: {}, others delayed: N",
                        meta.get_id().unwrap(),
                        key.clone(),
                        row.get_state()
                    );

                    let ah = row.get_access_history();
                    if let Err(e) = self.insert_and_check(id, ah) {
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
                                let rlock = self.get_shared_lock(thread_id);
                                let node = rlock.get_transaction(txn_id);
                                node.add_key(&index.get_name(), key.clone(), OperationType::Read); // register operation
                                drop(rlock);
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
                    row.set_values(&c, &nv, "basic-sgt", &meta.get_id().unwrap())
                        .unwrap();

                    let rlock = self.get_shared_lock(thread_id);
                    let node = rlock.get_transaction(txn_id);
                    node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                    drop(rlock);
                    drop(mg);
                    drop(rh);

                    return Ok(());
                }

                RowState::Modified => {
                    debug!(
                        "Transaction: {}, Key: {}, State: {}, others delayed: N",
                        meta.get_id().unwrap(),
                        key.clone(),
                        row.get_state()
                    );

                    let ah = row.get_access_history();
                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // abort -- cascading abort
                    }

                    row.append_delayed(id); // add to delayed queue

                    debug!(
                        "Transaction: {} delayed on key {} --- Row: {}",
                        meta.get_id().unwrap(),
                        key.clone(),
                        row
                    );
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

                        if row.resume(id) {
                            debug!(
                                "Transaction: {} resumed on key {}",
                                meta.get_id().unwrap(),
                                key.clone()
                            );
                            row.remove_delayed(id); // remove from delayed queue

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
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
                                        let rlock = self.get_shared_lock(thread_id);
                                        let node = rlock.get_transaction(txn_id);
                                        node.add_key(
                                            &index.get_name(),
                                            key.clone(),
                                            OperationType::Read,
                                        ); // register operation
                                        drop(rlock);
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
                            row.set_values(&c, &nv, "basic-sgt", &meta.get_id().unwrap())
                                .unwrap();

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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
                        "Transaction: {}, Key: {}, State: {}, others delayed: Y",
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

                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // abort -- cascading abort
                    }

                    row.append_delayed(id); // add to delayed queue; returns wait on
                    debug!(
                        "Transaction: {} delayed on key {} --- Row: {}",
                        meta.get_id().unwrap(),
                        key.clone(),
                        row
                    );
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

                        if row.resume(id) {
                            debug!(
                                "Transaction: {} resumed on key {}",
                                meta.get_id().unwrap(),
                                key.clone()
                            );

                            row.remove_delayed(id);

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
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
                                        let rlock = self.get_shared_lock(thread_id);
                                        let node = rlock.get_transaction(txn_id);
                                        node.add_key(
                                            &index.get_name(),
                                            key.clone(),
                                            OperationType::Read,
                                        ); // register operation
                                        drop(rlock);
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
                            row.set_values(&c, &nv, "basic-sgt", &meta.get_id().unwrap())
                                .unwrap();

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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
        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;
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
            let ah = row.get_access_history();
            if let Err(e) = self.insert_and_check(id, ah) {
                drop(mg);
                drop(rh);
                self.abort(meta.clone()).unwrap();
                return Err(e.into()); // cascading abort or cycle found
            }

            // if no delayed transactions
            match row.get_state() {
                RowState::Clean => {
                    row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap())
                        .unwrap(); // execute append

                    let rlock = self.get_shared_lock(thread_id);
                    let node = rlock.get_transaction(txn_id);
                    node.add_key(&index.get_name(), key.clone(), OperationType::Update); // register operation
                    drop(rlock);
                    drop(mg);
                    drop(rh);

                    return Ok(());
                }

                RowState::Modified => {
                    row.append_delayed(id); // add to delayed queue

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

                        if row.resume(id) {
                            row.remove_delayed(id);

                            let ah = row.get_access_history(); // insert and check
                            if let Err(e) = self.insert_and_check(id, ah) {
                                drop(mg);
                                drop(rh);

                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // cascading abort or cycle found
                            }

                            row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap())
                                .unwrap(); // execute append ( never fails )

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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

                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into());
                    }

                    row.append_delayed(id); // add to delayed queue; returns wait on

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

                        if row.resume(id) {
                            row.remove_delayed(id);

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort
                            }

                            row.append_value(column, value, "basic-sgt", &meta.get_id().unwrap())
                                .unwrap();

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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
        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;
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

        if !row.is_delayed() {
            match row.get_state() {
                RowState::Clean => {
                    let ah = row.get_access_history();
                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // cascading abort or cycle found
                    }

                    let res = row
                        .get_and_set_values(columns, values, "basic-sgt", &meta.get_id().unwrap())
                        .unwrap();

                    let vals = res.get_values().unwrap(); // get values

                    let rlock = self.get_shared_lock(thread_id);
                    let node = rlock.get_transaction(txn_id);
                    node.add_key(&index.get_name(), key.clone(), OperationType::Update); // register operation
                    drop(rlock);
                    drop(mg);
                    drop(rh);

                    return Ok(vals);
                }
                RowState::Modified => {
                    let ah = row.get_access_history(); // insert and check
                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // cascading abort or cycle found
                    }

                    row.append_delayed(id); // add to delayed queue

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

                        if row.resume(id) {
                            row.remove_delayed(id);

                            let ah = row.get_access_history(); // insert and check
                            if let Err(e) = self.insert_and_check(id, ah) {
                                drop(mg);
                                drop(rh);

                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // cascading abort or cycle found
                            }

                            let res = row
                                .get_and_set_values(
                                    columns,
                                    values,
                                    "basic-sgt",
                                    &meta.get_id().unwrap(),
                                )
                                .unwrap();
                            let vals = res.get_values().unwrap(); // get values

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(vals);
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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

                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into());
                    }

                    row.append_delayed(id); // add to delayed queue; returns wait on

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

                        if row.resume(id) {
                            row.remove_delayed(id);

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort
                            }

                            let res = row
                                .get_and_set_values(
                                    columns,
                                    values,
                                    "basic-sgt",
                                    &meta.get_id().unwrap(),
                                )
                                .unwrap();
                            let vals = res.get_values().unwrap(); // get values

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(vals);
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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

    /// Delete from row.
    fn delete(
        &self,
        table: &str,
        key: PrimaryKey,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        let rh = match index.get_lock_on_row(key.clone()) {
            Ok(rh) => rh,
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort - row not found
                return Err(e);
            }
        };

        let mut mg = rh.lock().unwrap(); // get mutex on row
        let row = &mut *mg; // deref to row

        if !row.is_delayed() {
            match row.get_state() {
                RowState::Clean => {
                    let ah = row.get_access_history();
                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // cascading abort or cycle found
                    }
                    row.delete("basic-sgt").unwrap();

                    let rlock = self.get_shared_lock(thread_id);
                    let node = rlock.get_transaction(txn_id);
                    node.add_key(&index.get_name(), key.clone(), OperationType::Update); // register operation
                    drop(rlock);
                    drop(mg);
                    drop(rh);

                    return Ok(());
                }

                RowState::Modified => {
                    let ah = row.get_access_history(); // insert and check
                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into()); // cascading abort or cycle found
                    }

                    row.append_delayed(id); // add to delayed queue

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

                        if row.resume(id) {
                            row.remove_delayed(id);

                            let ah = row.get_access_history(); // insert and check
                            if let Err(e) = self.insert_and_check(id, ah) {
                                drop(mg);
                                drop(rh);

                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // cascading abort or cycle found
                            }
                            row.delete("basic-sgt").unwrap();

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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

                    if let Err(e) = self.insert_and_check(id, ah) {
                        drop(mg);
                        drop(rh);
                        self.abort(meta.clone()).unwrap();
                        return Err(e.into());
                    }

                    row.append_delayed(id); // add to delayed queue; returns wait on

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

                        if row.resume(id) {
                            row.remove_delayed(id);

                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                drop(mg);
                                drop(rh);
                                self.abort(meta.clone()).unwrap();
                                return Err(e.into()); // abort -- cascading abort
                            }

                            row.delete("basic-sgt").unwrap();

                            let rlock = self.get_shared_lock(thread_id);
                            let node = rlock.get_transaction(txn_id);
                            node.add_key(&index.get_name(), key.clone(), OperationType::Update); // operation succeeded -- register
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            return Ok(());
                        } else {
                            let ah = row.get_access_history();
                            if let Err(e) = self.insert_and_check(id, ah) {
                                row.remove_delayed(id);
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

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        debug!("Starting abort for {}", meta.get_id().unwrap());
        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;

        // debug!(
        //     "ABORT - {} request exculsive lock on {}",
        //     this_node_id, this_node_id
        // );
        let wlock = self.get_exculsive_lock(thread_id);
        // debug!(
        //     "ABORT - {} get exculsive lock on {}",
        //     this_node_id, this_node_id
        // );
        let node = wlock.get_transaction(txn_id);
        node.set_state(State::Aborted); // set state to aborted
        drop(wlock);
        // debug!(
        //     "ABORT - {} drop exculsive lock on {}",
        //     this_node_id, this_node_id
        // );

        // debug!("Abort set complete for {}", meta.get_id().unwrap());

        // debug!(
        //     "ABORT - {} request shared lock on {}",
        //     this_node_id, this_node_id
        // );

        let rlock = self.get_shared_lock(thread_id);
        let node = rlock.get_transaction(txn_id);
        // debug!(
        //     "ABORT - {} got shared lock on {}",
        //     this_node_id, this_node_id
        // );

        let inserts = node.get_keys(OperationType::Insert);
        let reads = node.get_keys(OperationType::Read);
        let updates = node.get_keys(OperationType::Update);
        let deletes = node.get_keys(OperationType::Delete);
        drop(rlock);
        // debug!(
        //     "ABORT - {} drop shared lock on {}",
        //     this_node_id, this_node_id
        // );

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

        self.clean_up_graph(id); // abort outgoing nodes
                                 //        info!("Cleaned up for {}", meta.get_id().unwrap());

        // debug!(
        //     "ABORT - {} request exculsive lock on {}",
        //     this_node_id, this_node_id
        // );
        // let wlock = self.get_exculsive_lock(this_node_id);
        // debug!(
        //     "ABORT - {} got exculsive lock on {}",
        //     this_node_id, this_node_id
        // );
        // wlock.reset(); // reset node information
        //                //wlock.set_state(State::Active);
        // drop(wlock);
        // debug!(
        //     "ABORT - {} drop exculsive lock on {}",
        //     this_node_id, this_node_id
        // );

        //        debug!("Graph after aborting {}: {}", meta.get_id().unwrap(), &self);
        debug!("Transaction {} terminated (abort)", meta.get_id().unwrap());
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        debug!("Start commit for: {}", meta.get_id().unwrap());

        let id = parse_id(meta.get_id().unwrap());
        let (thread_id, txn_id) = id;

        while let Err(ProtocolError::HasIncomingEdges) = self.commit_check(id) {
            if let Err(ProtocolError::CycleFound) = self.reduced_depth_first_search(id) {
                // debug!("Transaction {} found cycle", meta.get_id().unwrap());

                // debug!("CO - {} request shared lock on {}", id, id);
                let rlock = self.get_shared_lock(thread_id);
                //                debug!("CO - {} got shared lock on {}", id, id);
                let node = rlock.get_transaction(txn_id);
                node.set_state(State::Aborted);
                drop(rlock);
            //                debug!("CO - {} drop shared lock on {}", id, id);
            } else {
                //                debug!("Transaction {} has incoming", meta.get_id().unwrap());
            }
        }

        // debug!(
        //     "Commit cycle check complete for: {}",
        //     meta.get_id().unwrap()
        // );
        //        debug!("CO - {} request shared lock on {}", id, id);

        let rlock = self.get_shared_lock(thread_id); // take shared lock on this_node
                                                     //      debug!("CO - {} got shared lock on {}", id, id);
        let node = rlock.get_transaction(txn_id);

        let state = node.get_state(); // get this_node state
        drop(rlock);
        //    debug!("CO - {} drop shared lock on {}", id, id);

        match state {
            State::Aborted => {
                self.abort(meta.clone()).unwrap();
                return Err(ProtocolError::CascadingAbort.into());
            }
            State::Committed => {
                self.clean_up_graph(id); // remove outgoing edges
                                         //                debug!("CO - {} request shared lock on {}", id, id);

                let rlock = self.get_shared_lock(thread_id); // get shared lock
                                                             //              debug!("CO - {} got shared lock on {}", id, id);
                let node = rlock.get_transaction(txn_id);
                let inserts = node.get_keys(OperationType::Insert);
                let reads = node.get_keys(OperationType::Read);
                let updates = node.get_keys(OperationType::Update);
                let deletes = node.get_keys(OperationType::Delete);
                drop(rlock); // drop shared lock

                //            debug!("CO - {} drop shared lock on {}", id, id);

                for (index, key) in inserts {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.commit("basic-sgt", &meta.get_id().unwrap()); // commit inserts
                    drop(mg);
                    drop(rh);
                }

                //                debug!("Committing reads: {}", meta.get_id().unwrap());

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
                //                debug!("Read commited: {}", meta.get_id().unwrap());

                //              debug!("Committing updates: {}", meta.get_id().unwrap());

                for (index, key) in updates {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    let rh = index.get_lock_on_row(key.clone()).unwrap(); // get read handle to row
                    let mut mg = rh.lock().unwrap(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.commit("basic-sgt", &meta.get_id().unwrap()); // commit inserts

                    drop(mg);
                    drop(rh);
                }
                //                debug!("Updates commited: {}", meta.get_id().unwrap());

                for (index, key) in deletes {
                    let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                    index.get_map().remove(&key); // Remove the row from the map.
                }

                //              debug!("CO - {} request exculsive lock on {}", id, id);

                // let wlock = self.get_exculsive_lock(id);
                // debug!("CO - {} got exculsive lock on {}", id, id);
                // wlock.reset();
                // //wlock.set_state(State::Active);
                // debug!("CO - {} drop exculsive lock on {}", id, id);

                // debug!(
                //     "Graph after committing {}: {}",
                //     meta.get_id().unwrap(),
                //     &self
                // );
            }
            State::Active => panic!("node should not be active"),
        }
        //        debug!("Transaction {} terminated (commit)", meta.get_id().unwrap());

        Ok(())
    }

    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}

impl fmt::Display for BasicSerializationGraphTesting {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\n").unwrap();

        for node in &self.nodes {
            write!(f, "{}\n", node.read()).unwrap();
        }
        Ok(())
    }
}
