use crate::common::error::NonFatalError;
use crate::scheduler::basic_sgt::error::BasicSerializationGraphTestingError as ProtocolError;
use crate::scheduler::basic_sgt::node::{EdgeType, NodeSet, OperationType, State};
use crate::scheduler::{Scheduler, TransactionInfo};
use crate::storage::datatype::Data;
use crate::storage::index::Index;
use crate::storage::row::{Access, State as RowState};
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
        let (_to_thread, to_txn_id) = to;

        if rlock
            .get_transaction(to_txn_id)
            .incoming_edge_from_exists(from)
        {
            return Ok(());
        }

        let from_node = self.get_shared_lock(from_thread);

        match from_node.get_transaction(from_txn_id).get_state() {
            State::Aborted => {
                if !rw_edge {
                    drop(from_node);
                    return Err(ProtocolError::CascadingAbort); // w-w/w-r; cascading abort
                }
            }
            State::Active => {
                let (_to_thread, to_txn_id) = to;

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
                    }
                }
                Access::Read(from_node) => {
                    if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } =
                        from_node
                    {
                        self.add_edge(rlock, (thread_id, txn_id), this_node, true)?;
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

    /// Perform a reduced depth first search from `start` node.
    pub fn reduced_depth_first_search(
        &self,
        rlock: &RwLockReadGuard<NodeSet>,
        start: (usize, u64),
    ) -> Result<(), ProtocolError> {
        let (_thread_id, txn_id) = start;
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
    fn clean_up_graph(&self, rlock: &RwLockReadGuard<NodeSet>, id: (usize, u64)) {
        let (_thread_id, txn_id) = id;

        let this_node = rlock.get_transaction(txn_id);
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

            drop(rlock2);
        }
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

    /// Execute a read operation.
    fn read(
        &self,
        _table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        // tracing::info!("read by {} on {}", meta, key);
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            let index = self.get_ind(index.unwrap()).unwrap(); // never fails

            let rh = match index.get_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort -- row does not exist
                    return Err(e);
                }
            };

            let mut guard = rh.lock();

            let ah = guard.get_access_history();
            if let Err(e) = self.detect_read_conflicts(&rlock, (*thread_id, *txn_id), ah) {
                drop(rlock);
                drop(guard);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            if let Err(e) = self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id)) {
                drop(rlock);
                drop(guard);
                self.abort(&meta).unwrap(); // abort -- cycle found
                return Err(e.into());
            }

            let mut res = guard.get_values(columns, meta).unwrap();
            let node = rlock.get_transaction(*txn_id);
            node.add_key2(Arc::clone(&index), key, OperationType::Read);
            drop(rlock);
            let vals = res.get_values();
            drop(guard);
            Ok(vals)
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Execute an update operation.
    fn update(
        &self,
        _table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: &dyn Fn(Option<Vec<Data>>, Option<&[Data]>) -> Result<Vec<Data>, NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        // tracing::info!("update by {} on {}", meta, key);
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            // abort check
            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            // get handle to index
            let index = self.get_ind(index.unwrap()).unwrap(); // never fails

            // get handle to row
            let rh = match index.get_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort -- row not found
                    return Err(e);
                }
            };

            let mut guard = rh.lock(); // take lock on row

            if !guard.is_delayed() && guard.get_state() == RowState::Clean {
                let ah = guard.get_access_history();
                if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                    drop(guard);
                    drop(rlock);
                    self.abort(meta).unwrap();
                    return Err(e.into()); // abort-- cascading abort or cycle found
                }

                let node = rlock.get_transaction(*txn_id); // get handle to node

                let current_values;
                if let Some(columns) = read {
                    let mut res = guard.get_values(columns, meta).unwrap();
                    node.add_key2(Arc::clone(&index), key, OperationType::Read);
                    current_values = Some(res.get_values());
                } else {
                    current_values = None;
                }

                let new_values = match f(current_values, params) {
                    Ok(res) => res,
                    Err(e) => {
                        drop(guard);
                        drop(rlock);
                        self.abort(meta).unwrap(); // abort -- due to integrity constraint
                        return Err(e);
                    }
                };

                guard.set_values(columns, &new_values, meta).unwrap();
                node.add_key2(Arc::clone(&index), key, OperationType::Update);

                drop(guard);
                drop(rlock);

                Ok(())
            } else {
                let mut ah = guard.get_access_history(); // get access history
                let dependency = guard.append_delayed(meta); // add to delayed queue and get dependency

                drop(guard); // drop row lock
                ah.push(Access::Write(dependency.clone()));
                if let Err(e) = self.detect_write_conflicts(&rlock, (*thread_id, *txn_id), ah) {
                    drop(rlock);
                    self.abort(meta).unwrap();
                    return Err(e.into());
                }

                loop {
                    if let TransactionInfo::BasicSerializationGraph {
                        thread_id: dep_thread_id,
                        txn_id: dep_txn_id,
                    } = dependency
                    {
                        let dep = self.get_shared_lock(dep_thread_id); // get read lock on dependency
                        let dep_node = dep.get_transaction(dep_txn_id); // get transaction node
                        let dep_state = dep_node.get_state();
                        match dep_state {
                            // if active do cycle check
                            State::Active => {
                                drop(dep); // drop read lock
                                if let Err(e) =
                                    self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id))
                                {
                                    // if cycle found remove from dependency
                                    let rh = index.get_row(&key).unwrap();
                                    let mut guard = rh.lock();
                                    guard.remove_delayed(meta); // remove from delayed queue
                                    drop(guard);
                                    drop(rlock);
                                    self.abort(meta).unwrap();
                                    return Err(e.into());
                                }
                            }
                            // if dependency aborted then cascading abort
                            State::Aborted => {
                                drop(dep); // drop read lock
                                let rh = index.get_row(&key).unwrap();
                                let mut guard = rh.lock();
                                guard.remove_delayed(meta); // remove from delayed queue
                                drop(guard);
                                drop(rlock);
                                self.abort(meta).unwrap();
                                return Err(ProtocolError::CascadingAbort.into());
                            }
                            // dependency committed then it is my turn
                            State::Committed => {
                                drop(dep); // drop read lock
                                break;
                            }
                        }
                    }
                }

                // get handle to row
                let rh = index.get_row(&key).unwrap(); // row must exist
                let mut guard = rh.lock(); // take lock on row
                assert!(guard.resume(meta), "{} failed: {}", meta, guard);
                guard.remove_delayed(meta); // remove from delayed queue

                let ah = guard.get_access_history();
                if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                    drop(rlock);
                    drop(guard);
                    self.abort(meta).unwrap();
                    return Err(e.into()); // abort -- cascading abort or cycle
                }
                let node = rlock.get_transaction(*txn_id);
                let current_values;
                if let Some(columns) = read {
                    let mut res = guard.get_values(columns, meta).unwrap(); // should not fail

                    node.add_key2(Arc::clone(&index), key, OperationType::Read);
                    current_values = Some(res.get_values());
                } else {
                    current_values = None;
                }

                let new_values = match f(current_values, params) {
                    Ok(res) => res,
                    Err(e) => {
                        drop(guard);
                        drop(rlock);

                        self.abort(&meta).unwrap(); // abort -- due to integrity constraint
                        return Err(e);
                    }
                };

                guard.set_values(columns, &new_values, meta).unwrap();
                node.add_key2(Arc::clone(&index), key, OperationType::Update);
                drop(rlock);
                drop(guard);
                return Ok(());
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Append value to column.
    fn append(
        &self,
        _table: &str,
        index: Option<&str>,
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

            let index = match self.get_ind(index.unwrap()) {
                Ok(index) => index,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap();
                    return Err(e.into());
                }
            };

            let rh = match index.get_row(&key) {
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
                        rlock.get_transaction(*txn_id).add_key2(
                            Arc::clone(&index),
                            key,
                            OperationType::Update,
                        );
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
                            let rh = match index.get_row(&key) {
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
                                rlock.get_transaction(*txn_id).add_key2(
                                    Arc::clone(&index),
                                    key,
                                    OperationType::Update,
                                );

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
                            let rh = match index.get_row(&key) {
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
                                rlock.get_transaction(*txn_id).add_key2(
                                    Arc::clone(&index),
                                    key,
                                    OperationType::Update,
                                );

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
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Read (get) and update (set).
    fn read_and_update(
        &self,
        _table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        // tracing::info!("read + update by {} on {}", meta, key);
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // take shared lock

            // abort check
            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }

            // get handle to index
            let index = self.get_ind(index.unwrap()).unwrap();

            // get handle to row
            let rh = match index.get_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort -- row not found.
                    return Err(e);
                }
            };

            let mut guard = rh.lock(); // get mutex on row

            if !guard.is_delayed() && guard.get_state() == RowState::Clean {
                let ah = guard.get_access_history();
                if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                    drop(guard);
                    drop(rlock);
                    self.abort(meta).unwrap();
                    return Err(e.into()); // abort-- cascading abort or cycle found
                }

                let node = rlock.get_transaction(*txn_id); // get handle to node
                let mut res = guard.get_and_set_values(columns, values, meta).unwrap();
                let vals = res.get_values(); // get values
                node.add_key2(Arc::clone(&index), key, OperationType::Update);
                drop(guard);
                drop(rlock);

                Ok(vals)
            } else {
                let mut ah = guard.get_access_history(); // get access history
                let dependency = guard.append_delayed(meta); // add to delayed queue and get dependency

                drop(guard); // drop row lock
                ah.push(Access::Write(dependency.clone()));
                if let Err(e) = self.detect_write_conflicts(&rlock, (*thread_id, *txn_id), ah) {
                    drop(rlock);
                    self.abort(meta).unwrap();
                    return Err(e.into());
                }

                loop {
                    if let TransactionInfo::BasicSerializationGraph {
                        thread_id: dep_thread_id,
                        txn_id: dep_txn_id,
                    } = dependency
                    {
                        let dep = self.get_shared_lock(dep_thread_id); // get read lock on dependency
                        let dep_node = dep.get_transaction(dep_txn_id); // get transaction node

                        let dep_state = dep_node.get_state();
                        match dep_state {
                            // if active do cycle check
                            State::Active => {
                                drop(dep); // drop read lock
                                if let Err(e) =
                                    self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id))
                                {
                                    // if cycle found remove from dependency
                                    let rh = index.get_row(&key).unwrap();
                                    let mut guard = rh.lock();
                                    guard.remove_delayed(meta); // remove from delayed queue
                                    drop(guard);
                                    drop(rlock);
                                    self.abort(meta).unwrap();
                                    return Err(e.into());
                                }
                            }
                            // if dependency aborted then cascading abort
                            State::Aborted => {
                                drop(dep); // drop read lock
                                let rh = index.get_row(&key).unwrap();
                                let mut guard = rh.lock();
                                guard.remove_delayed(meta); // remove from delayed queue
                                drop(guard);
                                drop(rlock);
                                self.abort(meta).unwrap();
                                return Err(ProtocolError::CascadingAbort.into());
                            }
                            // dependency committed then it is my turn
                            State::Committed => {
                                drop(dep); // drop read lock
                                break;
                            }
                        }
                    }
                }

                // get handle to row
                let rh = index.get_row(&key).unwrap(); // row must exist
                let mut guard = rh.lock(); // take lock on row
                assert!(guard.resume(meta), "{} failed: {}", meta, guard);
                guard.remove_delayed(meta); // remove from delayed queue

                let ah = guard.get_access_history();
                if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
                    drop(rlock);
                    drop(guard);
                    self.abort(meta).unwrap();
                    return Err(e.into()); // abort -- cascading abort or cycle
                }
                let node = rlock.get_transaction(*txn_id);

                let mut res = guard.get_and_set_values(columns, values, meta).unwrap();
                let vals = res.get_values(); // get values
                node.add_key2(Arc::clone(&index), key, OperationType::Update);
                drop(rlock);
                drop(guard);
                return Ok(vals);
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Abort a transaction.
    fn abort(&self, meta: &TransactionInfo) -> crate::Result<()> {
        //    tracing::info!("aborting {}", meta);
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let rlock = self.get_shared_lock(*thread_id); // get read lock

            let node = rlock.get_transaction(*txn_id);
            node.set_state(State::Aborted);

            let reads = node.get_keys2(OperationType::Read);
            let updates = node.get_keys2(OperationType::Update);

            for (index, key) in &reads {
                if let Ok(rh) = index.get_row(&key) {
                    let mut mg = rh.lock();
                    let row = &mut *mg;
                    row.revert_read(meta);
                    drop(mg);
                    drop(rh);
                };
            }

            for (index, key) in &updates {
                if let Ok(rh) = index.get_row(&key) {
                    let mut mg = rh.lock();
                    let row = &mut *mg;
                    row.commit(meta);
                    drop(mg);
                    drop(rh);
                };
            }

            self.clean_up_graph(&rlock, (*thread_id, *txn_id));

            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit a transaction.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        // tracing::info!("committing {}", meta);
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            loop {
                let wlock = self.get_exculsive_lock(*thread_id); // get write lock
                let node = wlock.get_transaction(*txn_id); // get transaction
                let state = node.get_state(); // get state

                match state {
                    State::Active => {
                        let incoming = node.has_incoming();
                        if !incoming {
                            let reads = node.get_keys2(OperationType::Read);
                            let updates = node.get_keys2(OperationType::Update);
                            drop(wlock); // drop write lock

                            // important to drop write lock whilst making changes as other wise can deadlock

                            for (index, key) in &reads {
                                if let Ok(rh) = index.get_row(&key) {
                                    let mut guard = rh.lock();
                                    guard.revert_read(meta);
                                    drop(guard);
                                };
                            }

                            for (index, key) in updates {
                                if let Ok(rh) = index.get_row(&key) {
                                    let mut guard = rh.lock();
                                    guard.commit(meta);
                                    drop(guard);
                                };
                            }

                            // if active and no incoming edges
                            let wlock = self.get_exculsive_lock(*thread_id); // get write lock
                            let node = wlock.get_transaction(*txn_id); // get transaction
                            node.set_state(State::Committed);
                            drop(wlock); // drop write lock

                            let rlock = self.get_shared_lock(*thread_id); // TODO: potenrial deadlock
                            self.clean_up_graph(&rlock, (*thread_id, *txn_id)); // remove outgoing edges
                            drop(rlock);

                            return Ok(());
                        } else {
                            // if active but incoming edges

                            drop(wlock); // drop write lock
                            let rlock = self.get_shared_lock(*thread_id); // get read lock
                            if let Err(ProtocolError::CycleFound) =
                                self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id))
                            {
                                drop(rlock); // drop read lock
                                self.abort(meta).unwrap(); // start abort procedure
                                return Err(ProtocolError::CycleFound.into());
                            }
                            drop(rlock); // drop read lock
                        }
                    }
                    State::Aborted => {
                        drop(wlock);
                        self.abort(meta).unwrap(); // start abort procedure
                        return Err(ProtocolError::CascadingAbort.into());
                    }
                    State::Committed => unreachable!(),
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
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
