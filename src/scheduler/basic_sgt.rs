use crate::common::error::NonFatalError;
use crate::scheduler::basic_sgt::error::BasicSerializationGraphTestingError as ProtocolError;
use crate::scheduler::basic_sgt::node::{EdgeType, NodeSet, OperationType, State};
use crate::scheduler::{Scheduler, TransactionInfo};
use crate::storage::datatype::Data;
use crate::storage::index::Index;
use crate::storage::row::Access;
use crate::workloads::{PrimaryKey, Workload};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::{fmt, thread};
use tracing::{debug, info};

pub mod node;

pub mod error;

/// Basic serialization graph testing
#[derive(Debug)]
pub struct BasicSerializationGraphTesting {
    /// Graph.
    nodes: Vec<RwLock<NodeSet>>,

    /// Data.
    data: Arc<Workload>,
}

impl BasicSerializationGraphTesting {
    /// Initialise serialization graph with `size` nodes.
    pub fn new(size: u32, data: Workload) -> Self {
        info!("Initialise basic serialization graph with {} node(s)", size);
        let mut nodes = vec![];
        for i in 0..size {
            let node = RwLock::new(NodeSet::new(i as usize));
            nodes.push(node);
        }
        BasicSerializationGraphTesting {
            nodes,
            data: Arc::new(data),
        }
    }

    /// Get shared lock on the node.
    fn get_shared_lock(&self, id: usize) -> RwLockReadGuard<NodeSet> {
        self.nodes[id].read()
    }

    /// Get exculsive lock on the node.
    fn get_exculsive_lock(&self, id: usize) -> RwLockWriteGuard<NodeSet> {
        self.nodes[id].write()
    }

    fn get_index(&self, name: &str) -> Result<&Index, NonFatalError> {
        self.data.get_index(name)
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
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            //debug!("read on {} by txn {}: begin", key, meta);

            let rlock = self.get_shared_lock(*thread_id);

            if let Err(e) = self.abort_check(&rlock, *txn_id) {
                drop(rlock); // drop read lock
                debug!("read on {} by txn {}: abort -- abort check", key, meta);
                self.abort(meta).unwrap();
                return Err(e.into()); // abort -- cascading abort
            }
            //debug!("read on {} by txn {}: passed abort check", key, meta);

            let index_name = index.unwrap();
            let index = self.get_index(index_name).unwrap();

            let rh = match index.get_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock); // drop read lock
                    self.abort(meta).unwrap(); // abort -- row does not exist
                    return Err(e);
                }
            };

            let lsn = index.get_lsn(&key).unwrap();
            let rw_table = index.get_rw_table(&key).unwrap();
            let mut guard = rw_table.lock();
            let prv = guard.push_front(Access::Read(meta.clone()));
            drop(guard);
            //debug!("read on {} by txn {}: prv: {}", key, meta, prv);

            // 8. Loop until it is this operations turn, i.e., previous has finished (moved the lsn forward)
            loop {
                let i = lsn.get(); // current sequence number
                if i == prv {
                    break; // break when my previous operation has completed
                }
            }

            //debug!("read on {} by txn {}: prv == lsn", key, meta);

            let guard = rw_table.lock();
            let snapshot: VecDeque<(u64, Access)> = guard.snapshot();
            drop(guard);

            // For each access in the rw table;
            for (id, access) in snapshot {
                // If operation was before this operation;
                if id < prv {
                    // 10a. Insert WR edge
                    match access {
                        Access::Write(from) => {
                            if let TransactionInfo::BasicSerializationGraph {
                                thread_id: from_thread,
                                txn_id: from_txn,
                            } = from
                            {
                                if let Err(e) = self.add_edge(
                                    &rlock,
                                    (from_thread, from_txn),
                                    (*thread_id, *txn_id),
                                    false,
                                ) {
                                    drop(rlock); // drop read lock
                                    let mut guard = rw_table.lock();
                                    guard.erase((prv, Access::Read(meta.clone()))); // remove from rw table
                                    drop(guard);
                                    lsn.replace(prv + 1); // increment to next operation
                                    debug!("read on {} by txn {}: abort -- add edge", key, meta);

                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort
                                }
                            }
                        }
                        // No conlict
                        Access::Read(_) => {}
                    }
                    // 10b. Cycle check
                    if let Err(e) = self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id)) {
                        drop(rlock);
                        let mut guard = rw_table.lock();
                        guard.erase((prv, Access::Read(meta.clone())));
                        drop(guard);
                        lsn.replace(prv + 1); // increment to next operation
                        debug!("read on {} by txn {}: abort -- cycle found", key, meta);
                        self.abort(&meta).unwrap(); // abort -- cycle found
                        return Err(e.into());
                    }
                }
            }
            //   debug!("read on {} by txn {}: execute", key, meta);

            // 11. operation ok to execute
            let mut guard = rh.lock(); // lock row
            let mut res = guard.get_values(columns, meta).unwrap();
            let vals = res.get_values();

            // 12. Register operation; so access can be removed at commit/abort time
            let node = rlock.get_transaction(*txn_id);
            node.add_key(index_name.to_string(), key, OperationType::Read);
            drop(rlock);
            drop(guard);

            lsn.replace(prv + 1); // increment to next operation
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
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            //debug!("update on {} by txn {}: begin", key, meta);
            let index_name = index.unwrap();
            let index = self.get_index(index_name).unwrap(); // handle to index

            let mut prv;
            let mut lsn;
            loop {
                let rlock = self.get_shared_lock(*thread_id);
                //debug!("update on {} by txn {}: get read lock", key, meta);

                if let Err(e) = self.abort_check(&rlock, *txn_id) {
                    drop(rlock);
                    debug!("read on {} by txn {}: abort -- abort check", key, meta);

                    self.abort(meta).unwrap();
                    return Err(e.into()); // abort -- cascading abort
                }
                //debug!("update on {} by txn {}: passed abort check", key, meta);

                let rh = match index.get_row(&key) {
                    Ok(rh) => rh,
                    Err(e) => {
                        drop(rlock);
                        self.abort(meta).unwrap(); // abort -- row not found
                        return Err(e);
                    }
                };

                lsn = index.get_lsn(&key).unwrap();

                let rw_table = index.get_rw_table(&key).unwrap();
                let mut guard = rw_table.lock();
                prv = guard.push_front(Access::Write(meta.clone()));
                drop(guard);
                //debug!("update on {} by txn {}: prv: {}", key, meta, prv);

                loop {
                    let i = lsn.get(); // current operation number
                    if i == prv {
                        break; // if  current = previous then this transaction can execute
                    }
                }

                //debug!("update on {} by txn {}: lsn == prv", key, meta,);

                let guard = rw_table.lock();
                let snapshot: VecDeque<(u64, Access)> = guard.snapshot();
                drop(guard);
                //debug!("update on {} by txn {}: rwtable {:?}", key, meta, snapshot);

                let mut wait = false;
                // For each access in the rw table;
                for (id, access) in snapshot {
                    // If operation was before this transaction;
                    if id < prv {
                        match access {
                            Access::Write(from) => {
                                if let TransactionInfo::BasicSerializationGraph {
                                    thread_id: from_thread,
                                    txn_id: from_txn,
                                } = from
                                {
                                    // debug!(
                                    //     "update on {} by txn {}: outstanding write by {}",
                                    //     key, meta, from
                                    // );

                                    // 10a. Check state
                                    let rlock2 = self.get_shared_lock(from_thread); // take shared lock
                                    let state = rlock2.get_transaction(from_txn).get_state(); // get handle to node
                                    drop(rlock2);
                                    match state {
                                        State::Aborted => {
                                            debug!(
                                                "update on {} by txn {}: {} write aborted",
                                                key, meta, from
                                            );
                                            drop(rlock); // drop read lock on the node
                                            let mut guard = rw_table.lock();
                                            guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                                            drop(guard);
                                            lsn.replace(prv + 1); // increment to next operation

                                            self.abort(meta).unwrap();
                                            return Err(ProtocolError::CascadingAbort.into());
                                            // abort -- cascading abort
                                        }
                                        State::Committed => {
                                            debug!(
                                                "update on {} by txn {}: {} committed",
                                                key, meta, from
                                            );
                                            continue;
                                        }
                                        State::Active => {
                                            debug!(
                                                "update on {} by txn {}: {} active",
                                                key, meta, from
                                            );

                                            if let Err(e) = self.add_edge(
                                                &rlock,
                                                (from_thread, from_txn),
                                                (*thread_id, *txn_id),
                                                false,
                                            ) {
                                                drop(rlock); // drop read lock on the node
                                                let mut guard = rw_table.lock();
                                                guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                                                drop(guard);
                                                lsn.replace(prv + 1); // increment to next operation
                                                self.abort(meta).unwrap();
                                                debug!(
                                                    "update on {} by txn {}: abort -- WW add edge",
                                                    key, meta
                                                );
                                                return Err(e.into()); // abort -- cascading abort
                                            }
                                            // cycle check
                                            if let Err(e) = self.reduced_depth_first_search(
                                                &rlock,
                                                (*thread_id, *txn_id),
                                            ) {
                                                drop(rlock);
                                                let mut guard = rw_table.lock();
                                                guard.erase((prv, Access::Write(meta.clone())));
                                                drop(guard);
                                                lsn.replace(prv + 1);

                                                debug!(
                                                    "update on {} by txn {}: abort -- WW cycle check",
                                                    key, meta
                                                );
                                                self.abort(&meta).unwrap(); // abort -- cycle found
                                                return Err(e.into());
                                            }
                                            // wait
                                            wait = true;
                                        }
                                    }
                                }
                            }
                            Access::Read(from) => {}
                        }
                    }
                }

                if wait {
                    //  debug!("update on {} by txn {}: delay", key, meta);

                    drop(rlock);
                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Write(meta.clone())));
                    drop(guard);
                    lsn.replace(prv + 1);
                } else {
                    //  debug!("update on {} by txn {}: no delay", key, meta);
                    drop(rlock);
                    break; // no delay
                }
            }

            let rlock = self.get_shared_lock(*thread_id);

            // 11. Get snapshot of the access history
            // 6. get handle to access history
            let rw_table = index.get_rw_table(&key).unwrap();
            let guard = rw_table.lock();
            let snapshot: VecDeque<(u64, Access)> = guard.snapshot();
            drop(guard);

            // 12. RW edges; for each access in the rw table;
            for (id, access) in snapshot {
                // if operation was before this transaction;
                if id < prv {
                    // insert RW edge
                    match access {
                        Access::Read(from) => {
                            if let TransactionInfo::BasicSerializationGraph {
                                thread_id: from_thread,
                                txn_id: from_txn,
                            } = from
                            {
                                if let Err(e) = self.add_edge(
                                    &rlock,
                                    (from_thread, from_txn),
                                    (*thread_id, *txn_id),
                                    true,
                                ) {
                                    drop(rlock); // drop read lock on the node
                                    let mut guard = rw_table.lock();
                                    guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                                    drop(guard);
                                    lsn.replace(prv + 1); // increment to next operation
                                    debug!(
                                        "update on {} by txn {}: abort -- RW add edge",
                                        key, meta
                                    );
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort
                                }
                            }
                        }
                        Access::Write(_) => {}
                    }
                    // cycle check
                    if let Err(e) = self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id)) {
                        drop(rlock);

                        let mut guard = rw_table.lock();
                        guard.erase((prv, Access::Write(meta.clone())));
                        drop(guard);
                        lsn.replace(prv + 1);
                        debug!("update on {} by txn {}: abort -- RW cycle check", key, meta);
                        self.abort(&meta).unwrap(); // abort -- cycle found
                        return Err(e.into());
                    }
                }
            }

            // 12. do write
            //    debug!("update on {} by txn {}: execute", key, meta);

            let rh = match index.get_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    self.abort(meta).unwrap(); // abort -- row not found
                    return Err(e);
                }
            };
            let mut guard = rh.lock(); // lock row
            let current_values;
            if let Some(columns) = read {
                let mut res = guard.get_values(columns, meta).unwrap();
                current_values = Some(res.get_values());
            } else {
                current_values = None;
            }

            let new_values = match f(current_values, params) {
                Ok(res) => res,
                Err(e) => {
                    drop(guard);
                    drop(rlock);
                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Write(meta.clone())));
                    drop(guard);

                    lsn.replace(prv + 1);
                    self.abort(meta).unwrap(); // abort -- due to integrity constraint
                    return Err(e);
                }
            };

            guard.set_values(columns, &new_values, meta).unwrap();

            // 13. register
            let node = rlock.get_transaction(*txn_id); // get handle to node
            node.add_key(index_name.to_string(), key, OperationType::Update);
            drop(guard);
            drop(rlock);
            lsn.replace(prv + 1);
            Ok(())
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
        // if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
        //     let rlock = self.get_shared_lock(*thread_id); // take shared lock

        //     if let Err(e) = self.abort_check(&rlock, *txn_id) {
        //         drop(rlock);
        //         self.abort(meta).unwrap();
        //         return Err(e.into()); // abort -- cascading abort
        //     }

        //     let index = match self.get_index(index.unwrap()) {
        //         Ok(index) => index,
        //         Err(e) => {
        //             drop(rlock);
        //             self.abort(meta).unwrap();
        //             return Err(e.into());
        //         }
        //     };

        //     let rh = match index.get_row(&key) {
        //         Ok(rg) => rg,
        //         Err(e) => {
        //             drop(rlock);
        //             self.abort(meta).unwrap(); // row not found
        //             return Err(e);
        //         }
        //     };

        //     let mut mg = rh.lock();
        //     let row = &mut *mg;

        //     if !row.is_delayed() {
        //         let ah = row.get_access_history();
        //         if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
        //             drop(rlock);
        //             drop(mg);
        //             drop(rh);
        //             self.abort(meta).unwrap();
        //             return Err(e.into()); // cascading abort or cycle found
        //         }

        //         // if no delayed transactions
        //         match row.get_state() {
        //             RowState::Clean => {
        //                 row.append_value(column, value, meta).unwrap(); // execute append
        //                 rlock.get_transaction(*txn_id).add_key2(
        //                     Arc::clone(&index),
        //                     key,
        //                     OperationType::Update,
        //                 );
        //                 drop(rlock);
        //                 drop(mg);
        //                 drop(rh);

        //                 Ok(())
        //             }

        //             RowState::Modified => {
        //                 row.append_delayed(meta); // add to delayed queue

        //                 drop(mg);
        //                 drop(rh);

        //                 loop {
        //                     let rh = match index.get_row(&key) {
        //                         Ok(rg) => rg,
        //                         Err(e) => {
        //                             drop(rlock);
        //                             self.abort(meta).unwrap(); // row not found
        //                             return Err(e);
        //                         }
        //                     };

        //                     let mut mg = rh.lock();
        //                     let row = &mut *mg;

        //                     if row.resume(meta) {
        //                         row.remove_delayed(meta);

        //                         let ah = row.get_access_history(); // insert and check
        //                         if let Err(e) =
        //                             self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
        //                         {
        //                             drop(rlock);
        //                             drop(mg);
        //                             drop(rh);
        //                             self.abort(meta).unwrap();
        //                             return Err(e.into()); // cascading abort or cycle found
        //                         }

        //                         row.append_value(column, value, meta).unwrap(); // execute append ( never fails )
        //                         rlock.get_transaction(*txn_id).add_key2(
        //                             Arc::clone(&index),
        //                             key,
        //                             OperationType::Update,
        //                         );

        //                         drop(rlock);
        //                         drop(mg);
        //                         drop(rh);
        //                         return Ok(());
        //                     } else {
        //                         let ah = row.get_access_history();
        //                         if let Err(e) =
        //                             self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
        //                         {
        //                             row.remove_delayed(meta);
        //                             drop(rlock);
        //                             drop(mg);
        //                             drop(rh);
        //                             self.abort(&meta).unwrap();
        //                             return Err(e.into()); // abort -- cascading abort or cycle
        //                         }

        //                         drop(mg);
        //                         drop(rh);
        //                     }
        //                 }
        //             }
        //         }
        //     } else {
        //         match row.get_state() {
        //             RowState::Clean | RowState::Modified => {
        //                 let mut ah = row.get_access_history(); // get access history
        //                 let delayed = row.get_delayed(); // other delayed transactions; multiple w-w conflicts
        //                 for tid in delayed {
        //                     ah.push(Access::Write(tid));
        //                 }

        //                 if let Err(e) = self.insert_and_check(&rlock, (*thread_id, *txn_id), ah) {
        //                     drop(rlock);
        //                     drop(mg);
        //                     drop(rh);
        //                     self.abort(meta).unwrap();
        //                     return Err(e.into());
        //                 }

        //                 row.append_delayed(meta); // add to delayed queue; returns wait on

        //                 drop(mg);
        //                 drop(rh);

        //                 loop {
        //                     let rh = match index.get_row(&key) {
        //                         Ok(rg) => rg,
        //                         Err(e) => {
        //                             drop(rlock);
        //                             self.abort(meta).unwrap(); // abort -- row not found
        //                             return Err(e);
        //                         }
        //                     };

        //                     let mut mg = rh.lock();
        //                     let row = &mut *mg;

        //                     if row.resume(meta) {
        //                         row.remove_delayed(meta);

        //                         let ah = row.get_access_history();
        //                         if let Err(e) =
        //                             self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
        //                         {
        //                             drop(rlock);
        //                             drop(mg);
        //                             drop(rh);
        //                             self.abort(meta).unwrap();
        //                             return Err(e.into()); // abort -- cascading abort
        //                         }

        //                         row.append_value(column, value, meta).unwrap();
        //                         rlock.get_transaction(*txn_id).add_key2(
        //                             Arc::clone(&index),
        //                             key,
        //                             OperationType::Update,
        //                         );

        //                         drop(rlock);
        //                         drop(mg);
        //                         drop(rh);
        //                         return Ok(());
        //                     } else {
        //                         let ah = row.get_access_history();

        //                         if let Err(e) =
        //                             self.insert_and_check(&rlock, (*thread_id, *txn_id), ah)
        //                         {
        //                             row.remove_delayed(meta);
        //                             drop(rlock);
        //                             drop(mg);
        //                             drop(rh);
        //                             self.abort(meta).unwrap();
        //                             return Err(e.into()); // abort -- cascading abort or cycle
        //                         }

        //                         drop(mg);
        //                         drop(rh);
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // } else {
        //     panic!("unexpected transaction info");
        // }
        Err(NonFatalError::NonSerializable)
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
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            // 1. get read lock

            // 3. get handle to index
            let index_name = index.unwrap();
            let index = self.get_index(index_name).unwrap(); // never fails

            let mut prv;
            let mut lsn;
            loop {
                let rlock = self.get_shared_lock(*thread_id);
                // 2. abort check
                if let Err(e) = self.abort_check(&rlock, *txn_id) {
                    drop(rlock); // drop read lock
                    self.abort(meta).unwrap();
                    return Err(e.into()); // abort -- cascading abort
                }

                // 4. get handle to row
                let rh = match index.get_row(&key) {
                    Ok(rh) => rh,
                    Err(e) => {
                        drop(rlock);
                        self.abort(meta).unwrap(); // abort -- row not found
                        return Err(e);
                    }
                };

                // 5. get handle to log sequence number
                lsn = index.get_lsn(&key).unwrap();

                // 6. get handle to access history
                let rw_table = index.get_rw_table(&key).unwrap();

                // 7. get previous operation number
                let mut guard = rw_table.lock();
                prv = guard.push_front(Access::Write(meta.clone()));
                drop(guard);

                // 8. Loop until it is this operations turn, i.e., previous has finished (moved the lsn forward)
                loop {
                    let i = lsn.get(); // current operation number
                    if i == prv {
                        break; // if  current = previous then this transaction can execute
                    }
                }

                // 9. Get snapshot of the access history
                let guard = rw_table.lock();
                let snapshot: VecDeque<(u64, Access)> = guard.snapshot();
                drop(guard);

                // 10. Delay is any outstanding writes
                let mut wait = false;
                // For each access in the rw table;
                for (id, access) in snapshot {
                    // If operation was before this transaction;
                    if id < prv {
                        match access {
                            // If operation is a write
                            Access::Write(from) => {
                                if let TransactionInfo::BasicSerializationGraph {
                                    thread_id: from_thread,
                                    txn_id: from_txn,
                                } = from
                                {
                                    // 10a. Check state
                                    let rlock2 = self.get_shared_lock(from_thread); // take shared lock
                                    let state = rlock2.get_transaction(from_txn).get_state(); // get handle to node
                                    drop(rlock2);
                                    match state {
                                        State::Aborted => {
                                            drop(rlock); // drop read lock on the node
                                            let mut guard = rw_table.lock();
                                            guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                                            drop(guard);
                                            lsn.replace(prv + 1); // increment to next operation
                                            self.abort(meta).unwrap();
                                            return Err(ProtocolError::CascadingAbort.into());
                                            // abort -- cascading abort
                                        }
                                        State::Committed => continue,
                                        State::Active => {
                                            // insert edge
                                            if let Err(e) = self.add_edge(
                                                &rlock,
                                                (from_thread, from_txn),
                                                (*thread_id, *txn_id),
                                                false,
                                            ) {
                                                drop(rlock); // drop read lock on the node
                                                let mut guard = rw_table.lock();
                                                guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                                                drop(guard);
                                                lsn.replace(prv + 1); // increment to next operation
                                                self.abort(meta).unwrap();
                                                return Err(e.into()); // abort -- cascading abort
                                            }
                                            // cycle check
                                            if let Err(e) = self.reduced_depth_first_search(
                                                &rlock,
                                                (*thread_id, *txn_id),
                                            ) {
                                                drop(rlock);
                                                let mut guard = rw_table.lock();
                                                guard.erase((prv, Access::Write(meta.clone())));
                                                drop(guard);
                                                lsn.replace(prv + 1);
                                                self.abort(&meta).unwrap(); // abort -- cycle found
                                                return Err(e.into());
                                            }
                                            // wait
                                            wait = true;
                                        }
                                    }
                                }
                            }
                            Access::Read(from) => {}
                        }
                    }
                }
                // go to start and try operation again
                if wait {
                    drop(rlock);
                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Write(meta.clone())));
                    drop(guard);
                    lsn.replace(prv + 1);
                } else {
                    drop(rlock);
                    break; // no delay
                }
            }

            let rlock = self.get_shared_lock(*thread_id);

            // 11. Get snapshot of the access history
            // 6. get handle to access history
            let rw_table = index.get_rw_table(&key).unwrap();
            let guard = rw_table.lock();
            let snapshot: VecDeque<(u64, Access)> = guard.snapshot();
            drop(guard);

            // 12. RW edges; for each access in the rw table;
            for (id, access) in snapshot {
                // if operation was before this transaction;
                if id < prv {
                    // insert WR edge
                    match access {
                        Access::Read(from) => {
                            if let TransactionInfo::BasicSerializationGraph {
                                thread_id: from_thread,
                                txn_id: from_txn,
                            } = from
                            {
                                if let Err(e) = self.add_edge(
                                    &rlock,
                                    (from_thread, from_txn),
                                    (*thread_id, *txn_id),
                                    true,
                                ) {
                                    drop(rlock); // drop read lock on the node
                                    let mut guard = rw_table.lock();
                                    guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                                    drop(guard);
                                    lsn.replace(prv + 1); // increment to next operation
                                    self.abort(meta).unwrap();
                                    return Err(e.into()); // abort -- cascading abort
                                }
                            }
                        }
                        Access::Write(_) => {}
                    }
                    // cycle check
                    if let Err(e) = self.reduced_depth_first_search(&rlock, (*thread_id, *txn_id)) {
                        drop(rlock);

                        let mut guard = rw_table.lock();
                        guard.erase((prv, Access::Write(meta.clone())));
                        drop(guard);
                        lsn.replace(prv + 1);
                        self.abort(&meta).unwrap(); // abort -- cycle found
                        return Err(e.into());
                    }
                }
            }

            // 12. do write
            let rh = match index.get_row(&key) {
                Ok(rh) => rh,
                Err(e) => {
                    drop(rlock);
                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Write(meta.clone())));
                    drop(guard);
                    self.abort(meta).unwrap(); // abort -- row not found
                    return Err(e);
                }
            };
            let mut guard = rh.lock(); // lock row

            let mut res = guard.get_and_set_values(columns, values, meta).unwrap();
            let vals = res.get_values(); // get values

            // 13. register
            let node = rlock.get_transaction(*txn_id); // get handle to node
            node.add_key(index_name.to_string(), key, OperationType::Update);
            drop(guard);
            drop(rlock);
            lsn.replace(prv + 1);
            Ok(vals)
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

            let reads = node.get_keys(OperationType::Read);
            let updates = node.get_keys(OperationType::Update);

            for (name, key) in &reads {
                let index = self.get_index(&name).unwrap();
                if let Ok(rh) = index.get_row(&key) {
                    // get handle to row
                    let rh = index.get_row(&key).unwrap();

                    //  get handle to access history
                    let rw_table = index.get_rw_table(&key).unwrap();

                    // remove access
                    let mut guard = rw_table.lock();
                    guard.erase_all(Access::Read(meta.clone()));
                    drop(guard);
                };
            }

            for (name, key) in &updates {
                let index = self.get_index(&name).unwrap();
                if let Ok(rh) = index.get_row(&key) {
                    // revert changes
                    let rh = index.get_row(&key).unwrap();
                    let mut guard = rh.lock();
                    guard.revert(meta);
                    drop(guard);

                    //  get handle to access history
                    let rw_table = index.get_rw_table(&key).unwrap();

                    // remove access
                    let mut guard = rw_table.lock();
                    guard.erase_all(Access::Write(meta.clone()));
                    drop(guard);
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
                            let reads = node.get_keys(OperationType::Read);
                            let updates = node.get_keys(OperationType::Update);
                            drop(wlock); // drop write lock

                            // important to drop write lock whilst making changes as other wise can deadlock

                            for (name, key) in &reads {
                                let index = self.get_index(&name).unwrap();
                                if let Ok(rh) = index.get_row(&key) {
                                    // get handle to row
                                    let rh = index.get_row(&key).unwrap();

                                    //  get handle to access history
                                    let rw_table = index.get_rw_table(&key).unwrap();

                                    // remove access
                                    let mut guard = rw_table.lock();
                                    guard.erase_all(Access::Read(meta.clone()));
                                    drop(guard);
                                };
                            }

                            for (name, key) in &updates {
                                let index = self.get_index(&name).unwrap();
                                if let Ok(rh) = index.get_row(&key) {
                                    // revert changes
                                    let rh = index.get_row(&key).unwrap();
                                    let mut guard = rh.lock();
                                    guard.commit(meta);
                                    drop(guard);

                                    //  get handle to access history
                                    let rw_table = index.get_rw_table(&key).unwrap();

                                    // remove access
                                    let mut guard = rw_table.lock();
                                    guard.erase_all(Access::Write(meta.clone()));
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
