use crate::common::error::NonFatalError;
use crate::server::scheduler::basic_sgt::error::BasicSerializationGraphTestingError as ProtocolError;
use crate::server::scheduler::basic_sgt::node::{EdgeType, NodeSet, OperationType, State};
use crate::server::scheduler::{Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::row::{Access, Row, State as RowState};

use crate::workloads::{PrimaryKey, Workload};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashSet;
use std::sync::Arc;
use std::{fmt, thread};
use tracing::info;

pub mod node;

pub mod error;

/// Basic serialization graph testing
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
        info!("Initialise basic serialization graph with {} node(s)", size);
        let mut nodes = vec![];
        for i in 0..size {
            let node = RwLock::new(NodeSet::new(i as usize));
            nodes.push(node);
        }
        BasicSerializationGraphTesting { nodes, data }
    }

    /// Get shared lock on the node.
    fn get_shared_lock(&self, id: usize) -> RwLockReadGuard<NodeSet> {
        self.nodes[id].read()
    }

    /// Get exculsive lock on the node.
    fn get_exculsive_lock(&self, id: usize) -> RwLockWriteGuard<NodeSet> {
        self.nodes[id].write()
    }

    /// Insert an edge into the serialization graph `(from) --> (to)`.
    ///
    /// If the edges is (i) a self edge, (ii) already exists, or (iii) from node is committed no edge is added.
    pub fn add_edge(
        &self,
        rlock: &RwLockReadGuard<NodeSet>,
        from: (usize, u64),
        to: (usize, u64),
        rw_edge: bool,
    ) -> Result<(), ProtocolError> {
        if from == to {
            return Ok(()); // don't add self edges
        }

        let (from_thread, from_txn_id) = from;

        let from_node = self.get_shared_lock(from_thread);

        match from_node.get_transaction(from_txn_id).get_state() {
            State::Aborted => {
                if !rw_edge {
                    drop(from_node);
                    return Err(ProtocolError::CascadingAbort); // w-w/w-r; cascading abort
                }
            }
            State::Active => {
                let (to_thread, to_txn_id) = to;

                from_node
                    .get_transaction(from_txn_id)
                    .insert_edge(to, EdgeType::Outgoing);
                rlock
                    .get_transaction(to_txn_id)
                    .insert_edge(from, EdgeType::Incoming);
            }
            State::Committed => {}
        }
        drop(from_node);

        Ok(())
    }

    /// Check if a transaction with `id` has aborted.
    pub fn abort_check(
        &self,
        rlock: &RwLockReadGuard<NodeSet>,
        txn_id: u64,
    ) -> Result<(), ProtocolError> {
        let state = rlock.get_transaction(txn_id).get_state();
        if let State::Aborted = state {
            Err(ProtocolError::CascadingAbort)
        } else {
            Ok(())
        }
    }

    /// Given an access history, detect conflicts with a write operation, and insert edges into the graph
    /// for transaction residing in `this_node`.
    pub fn detect_write_conflicts(
        &self,
        rlock: &RwLockReadGuard<NodeSet>,
        this_node: (usize, u64),
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        for access in access_history {
            match access {
                Access::Write(from_node) => {
                    if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } =
                        from_node
                    {
                        self.add_edge(rlock, (thread_id, txn_id), this_node, false)?;
                        // w-w conflict
                    }
                }
                Access::Read(from_node) => {
                    if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } =
                        from_node
                    {
                        self.add_edge(rlock, (thread_id, txn_id), this_node, true)?;
                        // r-w conflict
                    }
                }
            }
        }
        Ok(())
    }

    /// Given an access history, detect conflicts with a read operation, and insert edges into the graph.
    pub fn detect_read_conflicts(
        &self,
        rlock: &RwLockReadGuard<NodeSet>,
        this_node: (usize, u64),
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        for access in access_history {
            match access {
                Access::Write(from_node) => {
                    if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } =
                        from_node
                    {
                        self.add_edge(rlock, (thread_id, txn_id), this_node, false)?;
                    }
                }
                Access::Read(_) => {}
            }
        }
        Ok(())
    }

    /// Detect conflicts, insert edges, and do cycle check.
    pub fn insert_and_check(
        &self,
        rlock: &RwLockReadGuard<NodeSet>,
        this_node: (usize, u64),
        access_history: Vec<Access>,
    ) -> Result<(), ProtocolError> {
        self.detect_write_conflicts(rlock, this_node, access_history)?;
        self.reduced_depth_first_search(rlock, this_node)?;
        Ok(())
    }

    /// Attempt to commit a transaction.
    pub fn commit_check(&self, id: (usize, u64)) -> Result<(), ProtocolError> {
        let (thread_id, txn_id) = id;

        let wlock = self.get_exculsive_lock(thread_id);

        let node = wlock.get_transaction(txn_id);
        let state = node.get_state();

        match state {
            State::Active => {
                let incoming = node.has_incoming();
                if !incoming {
                    node.set_state(State::Committed); // if active and no incoming edges
                    drop(wlock);
                    Ok(())
                } else {
                    drop(wlock);
                    Err(ProtocolError::HasIncomingEdges)
                }
            }
            State::Aborted => {
                drop(wlock);
                Err(ProtocolError::CascadingAbort)
            }
            State::Committed => unreachable!(),
        }
    }

    /// Perform a reduced depth first search from `start` node.
    pub fn reduced_depth_first_search(
        &self,
        rlock: &RwLockReadGuard<NodeSet>,
        start: (usize, u64),
    ) -> Result<(), ProtocolError> {
        let (thread_id, txn_id) = start;

        let mut stack = Vec::new(); // nodes to visit
        let mut visited = HashSet::new(); // nodes visited

        let start_node = rlock.get_transaction(txn_id);
        stack.append(&mut start_node.get_outgoing()); // push outgoing to stack

        // pop until no more nodes to visit
        while let Some(current) = stack.pop() {
            if current == start {
                return Err(ProtocolError::CycleFound); // cycle found
            }

            if visited.contains(&current) {
                continue; // already visited
            }

            visited.insert(current); // mark as visited

            let (thread_id2, txn_id2) = current;

            let rlock2 = self.get_shared_lock(thread_id2); // get shared lock on current_node.
            let current_node = rlock2.get_transaction(txn_id2);
            let cs = current_node.get_state();
            if let State::Active = cs {
                stack.append(&mut current_node.get_outgoing()); // add outgoing to stack
            }
            drop(rlock2);
        }

        Ok(()) // no cycle found
    }

    /// Clean up graph.
    ///
    /// If node with `id` aborted then abort outgoing nodes before removing edges.
    /// Else; node committed, remove outgoing edges.
    fn clean_up_graph(&self, id: (usize, u64)) {
        let (thread_id, txn_id) = id;
        let rlock1 = self.get_shared_lock(thread_id); // get shared lock on this_node
        let this_node = rlock1.get_transaction(txn_id);

        let state = this_node.get_state(); // get state of this_node
        let outgoing_nodes = this_node.get_outgoing(); // get outgoing edges

        for out in outgoing_nodes {
            let (thread_id, txn_id) = out;

            let rlock2 = self.get_shared_lock(thread_id); // get shared lock on outgoing node
            let outgoing_node = rlock2.get_transaction(txn_id);

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

    fn get_ind(&self, name: &str) -> Result<Arc<Index>, NonFatalError> {
        self.get_data().get_internals().get_index(name)
    }
}

impl Scheduler for BasicSerializationGraphTesting {
    /// Register a transaction with the serialization graph.
    ///
    /// Transaction gets the ID of the thread it is executed on.
    fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        let th = thread::current();
        let thread_id: usize = th.name().unwrap().parse().unwrap(); // get node id

        let mut wlock = self.get_exculsive_lock(thread_id); // get exculsive lock

        let (_, txn_id) = wlock.create_node();

        drop(wlock);

        Ok(TransactionInfo::BasicSerializationGraph { thread_id, txn_id })
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
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let table = self.get_table(table, &meta)?; // get handle to table
            let index = self.get_index(Arc::clone(&table), &meta)?; // get handle to index
            let mut row = Row::new(key.clone(), Arc::clone(&table), true, true); // create new row

            // initialise each field
            for (i, column) in columns.iter().enumerate() {
                if row
                    .init_value(column, Data::from(values[i].clone()))
                    .is_err()
                {
                    self.abort(&meta).unwrap(); // abort -- unable to initialise row
                    return Err(NonFatalError::UnableToInitialiseRow(
                        table.to_string(),
                        column.to_string(),
                        values[i].to_string(),
                    ));
                }
            }

            // set values in fields -- makes rows "dirty"
            if let Err(e) = row.set_values(columns, values, meta) {
                self.abort(&meta).unwrap(); // abort -- unable to convert to datatype
                return Err(e);
            }

            match index.insert(key, row) {
                Ok(_) => {
                    let rlock = self.get_shared_lock(*thread_id); // get shared lock
                    let node = rlock.get_transaction(*txn_id);
                    node.add_key(&index.get_name(), key, OperationType::Insert); // operation succeeded -- register
                    drop(rlock); // drop shared lock
                }

                Err(e) => {
                    self.abort(meta).unwrap(); // abort -- row already exists
                    return Err(e);
                }
            }
            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Execute a read operation.

    fn read(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            let index = match self.get_ind(index.unwrap()) {
                Ok(index) => index,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap();
                    return Err(e.into());
                }
            };

            let rh = match index.get_lock_on_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort -- row does not exist
                    return Err(e);
                }
            };

            let mut mg = rh.lock();
            let row = &mut *mg;

            let ah = row.get_access_history();
            if let Err(e) = self.detect_read_conflicts(&rlock, (*thread_id, *txn_id), ah) {
                drop(rlock);
                drop(mg);
                drop(rh);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            if let Err(e) = self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id)) {
                drop(rlock);
                drop(mg);
                drop(rh);
                self.abort(&meta).unwrap(); // abort -- cycle found
                return Err(e.into());
            }

            match row.get_values(columns, meta) {
                Ok(res) => {
                    let node = rlock.get_transaction(*txn_id);
                    node.add_key2(Arc::clone(&index), key, OperationType::Read);
                    drop(rlock);
                    let vals = res.get_values();
                    drop(mg);
                    drop(rh);
                    Ok(vals)
                }
                Err(e) => {
                    drop(rlock);
                    drop(mg);
                    drop(rh);
                    self.abort(meta).unwrap(); // abort -- row marked for delete
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Execute an update operation.
    ///
    /// Adds an edge in the graph for each WW and RW conflict.
    fn update(
        &self,
        table: &str,
        key: &PrimaryKey,
        columns: &[&str],
        read: bool,
        params: Option<&[Data]>,
        f: &dyn Fn(
            &[&str],           // columns
            Option<Vec<Data>>, // current values
            Option<&[Data]>,   // parameters
        ) -> Result<(Vec<String>, Vec<Data>), NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            let id = (thread_id, txn_id);

            let index = self.get_index(self.get_table(table, &meta)?, &meta)?;

            let rh = match index.get_lock_on_row(&key) {
                Ok(rg) => rg,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort -- row not found
                    return Err(e);
                }
            };

            let mut mg = rh.lock();
            let row = &mut *mg;

            if !row.is_delayed() {
                match row.get_state() {
                    RowState::Clean => {
                        let ah = row.get_access_history();
                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into()); // abort -- cascading abort or cycle found
                        }

                        let current_values;
                        if read {
                            let res = row.get_values(columns, meta).unwrap(); // should not fail
                            let node = rlock.get_transaction(*txn_id);
                            node.add_key(&index.get_name(), &key, OperationType::Read); // register operation
                            current_values = Some(res.get_values());
                        } else {
                            current_values = None;
                        }

                        let (new_columns, new_values) = match f(columns, current_values, params) {
                            Ok(res) => res,
                            Err(e) => {
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                self.abort(meta).unwrap(); // abort -- due to integrity constraint
                                return Err(e);
                            }
                        };

                        let cols: Vec<&str> = new_columns.iter().map(|s| &**s).collect();
                        row.set_values(&cols, &new_values, meta).unwrap();
                        let node = rlock.get_transaction(*txn_id);
                        node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                        drop(rlock);
                        drop(mg);
                        drop(rh);

                        Ok(())
                    }

                    RowState::Modified => {
                        let ah = row.get_access_history();
                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into()); // abort -- cascading abort
                        }

                        row.append_delayed(meta); // add to delayed queue

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta); // remove from delayed queue

                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                let current_values;
                                if read {
                                    let res = row.get_values(columns, meta).unwrap(); // should not fail
                                    let node = rlock.get_transaction(*txn_id);
                                    node.add_key(&index.get_name(), &key, OperationType::Read); // register operation
                                    current_values = Some(res.get_values());
                                } else {
                                    current_values = None;
                                }

                                let (new_columns, new_values) =
                                    match f(columns, current_values, params) {
                                        Ok(res) => res,
                                        Err(e) => {
                                            drop(rlock);
                                            drop(mg);
                                            drop(rh);
                                            self.abort(&meta).unwrap(); // abort -- due to integrity constraint
                                            return Err(e);
                                        }
                                    };
                                let cols: Vec<&str> = new_columns.iter().map(|s| &**s).collect();
                                row.set_values(&cols, &new_values, meta).unwrap();
                                let node = rlock.get_transaction(*txn_id);
                                node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(());
                            } else {
                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            format!("{:?}", key),
                            table.to_string(),
                        ))
                    }
                }
            } else {
                match row.get_state() {
                    RowState::Clean | RowState::Modified => {
                        let mut ah = row.get_access_history(); // get access history
                        let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                        for tid in delayed {
                            ah.push(Access::Write(tid));
                        }

                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into()); // abort -- cascading abort
                        }

                        row.append_delayed(meta); // add to delayed queue; returns wait on

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // abort -- row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta);

                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(&meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort
                                }

                                let current_values;
                                if read {
                                    let res = row.get_values(columns, meta).unwrap(); // should not fail
                                    let node = rlock.get_transaction(*txn_id);
                                    node.add_key(&index.get_name(), &key, OperationType::Read); // register operation
                                    current_values = Some(res.get_values());
                                } else {
                                    current_values = None;
                                }

                                let (new_columns, new_values) =
                                    match f(columns, current_values, params) {
                                        Ok(res) => res,
                                        Err(e) => {
                                            drop(rlock);
                                            drop(mg);
                                            drop(rh);
                                            self.abort(&meta).unwrap(); // abort -- due to integrity constraint
                                            return Err(e);
                                        }
                                    };
                                let cols: Vec<&str> = new_columns.iter().map(|s| &**s).collect();

                                row.set_values(&cols, &new_values, meta).unwrap();

                                let node = rlock.get_transaction(*txn_id);
                                node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(());
                            } else {
                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            format!("{:?}", key),
                            table.to_string(),
                        ))
                    }
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Append value to column.
    fn append(
        &self,
        table: &str,
        key: &PrimaryKey,
        column: &str,
        value: Data,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            let id = (thread_id, txn_id);

            let table = self.get_table(table, &meta)?;
            let index = self.get_index(Arc::clone(&table), &meta)?;

            let rh = match index.get_lock_on_row(&key) {
                Ok(rg) => rg,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // row not found
                    return Err(e);
                }
            };

            let mut mg = rh.lock();
            let row = &mut *mg;

            if !row.is_delayed() {
                let ah = row.get_access_history();
                if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                    drop(rlock);
                    drop(mg);
                    drop(rh);
                    self.abort(meta).unwrap();
                    return Err(e.into()); // cascading abort or cycle found
                }

                // if no delayed transactions
                match row.get_state() {
                    RowState::Clean => {
                        row.append_value(column, value, meta).unwrap(); // execute append
                        rlock.get_transaction(*txn_id).add_key(
                            &index.get_name(),
                            key,
                            OperationType::Update,
                        ); // register operation
                        drop(rlock);
                        drop(mg);
                        drop(rh);

                        Ok(())
                    }

                    RowState::Modified => {
                        row.append_delayed(meta); // add to delayed queue

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta);

                                let ah = row.get_access_history(); // insert and check
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // cascading abort or cycle found
                                }

                                row.append_value(column, value, meta).unwrap(); // execute append ( never fails )

                                let node = rlock.get_transaction(*txn_id).add_key(
                                    &index.get_name(),
                                    key,
                                    OperationType::Update,
                                ); // operation succeeded -- register
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(());
                            } else {
                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(&meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(&meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            format!("{:?}", key),
                            table.to_string(),
                        ))
                    }
                }
            } else {
                match row.get_state() {
                    RowState::Clean | RowState::Modified => {
                        let mut ah = row.get_access_history(); // get access history
                        let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                        for tid in delayed {
                            ah.push(Access::Write(tid));
                        }

                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into());
                        }

                        row.append_delayed(meta); // add to delayed queue; returns wait on

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // abort -- row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta);

                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort
                                }

                                row.append_value(column, value, meta).unwrap();
                                rlock.get_transaction(*txn_id).add_key(
                                    &index.get_name(),
                                    key,
                                    OperationType::Update,
                                ); // operation succeeded -- register
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(());
                            } else {
                                let ah = row.get_access_history();

                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            format!("{:?}", key),
                            table.to_string(),
                        ))
                    }
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Read (get) and update (set).
    fn read_and_update(
        &self,
        table: &str,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            let id = (thread_id, txn_id);

            let index = self.get_index(self.get_table(table, &meta)?, &meta)?; // get index

            let rh = match index.get_lock_on_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort -- row not found.
                    return Err(e);
                }
            };

            let mut mg = rh.lock(); // get mutex on row
            let row = &mut *mg; // deref to row

            if !row.is_delayed() {
                match row.get_state() {
                    RowState::Clean => {
                        let ah = row.get_access_history();
                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into()); // cascading abort or cycle found
                        }

                        let res = row.get_and_set_values(columns, values, meta).unwrap();
                        let vals = res.get_values(); // get values
                        let node = rlock.get_transaction(*txn_id);
                        node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                        drop(rlock);
                        drop(mg);
                        drop(rh);

                        Ok(vals)
                    }
                    RowState::Modified => {
                        let ah = row.get_access_history(); // insert and check
                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into()); // cascading abort or cycle found
                        }

                        row.append_delayed(meta); // add to delayed queue

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta);

                                let ah = row.get_access_history(); // insert and check
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);

                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // cascading abort or cycle found
                                }

                                let res = row.get_and_set_values(columns, values, meta).unwrap();
                                let vals = res.get_values(); // get values

                                rlock.get_transaction(*txn_id).add_key(
                                    &index.get_name(),
                                    key,
                                    OperationType::Update,
                                ); // operation succeeded -- register
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(vals);
                            } else {
                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(&meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            format!("{:?}", key),
                            table.to_string(),
                        ))
                    }
                }
            } else {
                match row.get_state() {
                    RowState::Clean | RowState::Modified => {
                        let mut ah = row.get_access_history(); // get access history
                        let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                        for tid in delayed {
                            ah.push(Access::Write(tid));
                        }

                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(&meta).unwrap();
                            return Err(e.into());
                        }

                        row.append_delayed(meta); // add to delayed queue; returns wait on

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // abort -- row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta);

                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort
                                }

                                let res = row.get_and_set_values(columns, values, meta).unwrap();
                                let vals = res.get_values(); // get values

                                rlock.get_transaction(*txn_id).add_key(
                                    &index.get_name(),
                                    key,
                                    OperationType::Update,
                                ); // operation succeeded -- register
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(vals);
                            } else {
                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            format!("{:?}", key),
                            table.to_string(),
                        ))
                    }
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Delete from row.
    fn delete(
        &self,
        table: &str,
        key: &PrimaryKey,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            let id = (thread_id, txn_id);

            let (thread_id, txn_id) = id;

            let index = self.get_index(self.get_table(table, &meta)?, &meta)?;

            let rh = match index.get_lock_on_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort - row not found
                    return Err(e);
                }
            };

            let mut mg = rh.lock(); // get mutex on row
            let row = &mut *mg; // deref to row

            if !row.is_delayed() {
                match row.get_state() {
                    RowState::Clean => {
                        let ah = row.get_access_history();
                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into()); // cascading abort or cycle found
                        }
                        row.delete(meta).unwrap();

                        let rlock = self.get_shared_lock(*thread_id);
                        let node = rlock.get_transaction(*txn_id);
                        node.add_key(&index.get_name(), key, OperationType::Update); // register operation
                        drop(rlock);
                        drop(mg);
                        drop(rh);

                        Ok(())
                    }

                    RowState::Modified => {
                        let ah = row.get_access_history(); // insert and check
                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(&meta).unwrap();
                            return Err(e.into()); // cascading abort or cycle found
                        }

                        row.append_delayed(meta); // add to delayed queue

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta);

                                let ah = row.get_access_history(); // insert and check
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);

                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // cascading abort or cycle found
                                }
                                row.delete(meta).unwrap();

                                rlock.get_transaction(*txn_id).add_key(
                                    &index.get_name(),
                                    &key,
                                    OperationType::Update,
                                ); // operation succeeded -- register
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(());
                            } else {
                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            key.to_string(),
                            table.to_string(),
                        ))
                    }
                }
            } else {
                match row.get_state() {
                    RowState::Clean | RowState::Modified => {
                        let mut ah = row.get_access_history(); // get access history
                        let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
                        for transaction_id in delayed {
                            ah.push(Access::Write(transaction_id));
                        }

                        if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                            drop(rlock);
                            drop(mg);
                            drop(rh);
                            self.abort(meta).unwrap();
                            return Err(e.into());
                        }

                        row.append_delayed(meta); // add to delayed queue; returns wait on

                        drop(mg);
                        drop(rh);

                        loop {
                            let rh = match index.get_lock_on_row(&key) {
                                Ok(rg) => rg,
                                Err(e) => {
                                    drop(rlock);
                                    self.abort(meta).unwrap(); // abort -- row not found
                                    return Err(e);
                                }
                            };

                            let mut mg = rh.lock();
                            let row = &mut *mg;

                            if row.resume(meta) {
                                row.remove_delayed(meta);

                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort
                                }

                                row.delete(meta).unwrap();

                                rlock.get_transaction(*txn_id).add_key(
                                    &index.get_name(),
                                    &key,
                                    OperationType::Update,
                                ); // operation succeeded -- register
                                drop(rlock);
                                drop(mg);
                                drop(rh);
                                return Ok(());
                            } else {
                                let ah = row.get_access_history();
                                if let Err(e) =
                                    self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
                                {
                                    row.remove_delayed(meta);
                                    drop(rlock);
                                    drop(mg);
                                    drop(rh);
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort or cycle
                                }

                                drop(mg);
                                drop(rh);
                            }
                        }
                    }
                    RowState::Deleted => {
                        drop(rlock);
                        drop(mg);
                        drop(rh);
                        self.abort(&meta).unwrap(); // abort -- cascading abort
                        Err(NonFatalError::RowDeleted(
                            format!("{:?}", key),
                            table.to_string(),
                        ))
                    }
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: &TransactionInfo) -> crate::Result<()> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let id = (thread_id, txn_id);

            let wlock = self.get_exculsive_lock(*thread_id);

            let node = wlock.get_transaction(*txn_id);
            node.set_state(State::Aborted); // set state to aborted
            drop(wlock);

            let rlock = self.get_shared_lock(*thread_id);
            let node = rlock.get_transaction(*txn_id);

            let inserts = node.get_keys(OperationType::Insert);
            let reads = node.get_keys2(OperationType::Read);
            let updates = node.get_keys(OperationType::Update);
            let deletes = node.get_keys(OperationType::Delete);
            drop(rlock);

            for (index, key) in &inserts {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.remove(&key).unwrap();
            }

            for (index, key) in &reads {
                // get read handle to row
                if let Ok(rh) = index.get_lock_on_row(&key) {
                    let mut mg = rh.lock(); // acquire mutex on the row
                    let row = &mut *mg; // deref to row
                    row.revert_read(meta);

                    drop(mg);
                    drop(rh);
                };
            }

            for (index, key) in &updates {
                let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

                if let Ok(rh) = index.get_lock_on_row(&key) {
                    let mut mg = rh.lock(); // acquire mutex on the row

                    let row = &mut *mg; // deref to row

                    row.revert(meta);

                    drop(mg);
                    drop(rh);
                };
            }

            for (index, key) in &deletes {
                let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index

                if let Ok(rh) = index.get_lock_on_row(&key) {
                    let mut mg = rh.lock();
                    let row = &mut *mg;
                    row.revert(meta);
                    drop(mg);
                    drop(rh);
                };
            }

            self.clean_up_graph((*thread_id, *txn_id)); // abort outgoing nodes

            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit a transaction.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            while let Err(ProtocolError::HasIncomingEdges) =
                self.commit_check((*thread_id, *txn_id))
            {
                let rlock = self.get_shared_lock(*thread_id);

                if let Err(ProtocolError::CycleFound) =
                    self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id))
                {
                    let node = rlock.get_transaction(*txn_id);
                    node.set_state(State::Aborted);
                }
                drop(rlock);
            }

            let rlock = self.get_shared_lock(*thread_id); // take shared lock on this_node

            let node = rlock.get_transaction(*txn_id);

            let state = node.get_state(); // get this_node state
            drop(rlock);

            match state {
                State::Aborted => {
                    self.abort(&meta).unwrap();
                    return Err(ProtocolError::CascadingAbort.into());
                }
                State::Committed => {
                    self.clean_up_graph((*thread_id, *txn_id)); // remove outgoing edges

                    let rlock = self.get_shared_lock(*thread_id); // get shared lock

                    let node = rlock.get_transaction(*txn_id);
                    let inserts = node.get_keys(OperationType::Insert);
                    let reads = node.get_keys2(OperationType::Read);
                    let updates = node.get_keys(OperationType::Update);
                    let deletes = node.get_keys(OperationType::Delete);
                    drop(rlock); // drop shared lock

                    for (index, key) in inserts {
                        let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                        let rh = index.get_lock_on_row(&key).unwrap(); // get read handle to row
                        let mut mg = rh.lock(); // acquire mutex on the row
                        let row = &mut *mg; // deref to row
                        row.commit(meta); // commit inserts
                        drop(mg);
                        drop(rh);
                    }

                    for (index, key) in &reads {
                        // get read handle to row
                        if let Ok(rh) = index.get_lock_on_row(&key) {
                            let mut mg = rh.lock(); // acquire mutex on the row
                            let row = &mut *mg; // deref to row
                            row.revert_read(meta);

                            drop(mg);
                            drop(rh);
                        };
                    }

                    for (index, key) in updates {
                        let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                        let rh = index.get_lock_on_row(&key).unwrap(); // get read handle to row
                        let mut mg = rh.lock(); // acquire mutex on the row
                        let row = &mut *mg;
                        row.commit(meta);

                        drop(mg);
                        drop(rh);
                    }

                    for (index, key) in deletes {
                        let index = self.data.get_internals().get_index(&index).unwrap(); // get handle to index
                        index.get_map().remove(&key); // Remove the row from the map.
                    }
                }

                State::Active => panic!("node should not be active"),
            }
        }
        Ok(())
    }

    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}

impl fmt::Display for BasicSerializationGraphTesting {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, " ").unwrap();

        for node in &self.nodes {
            writeln!(f, "{}", node.read()).unwrap();
        }
        Ok(())
    }
}
