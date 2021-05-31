use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::common::error::NonFatalError;
use crate::scheduler::sgt::epoch_manager::{EpochGuard, EpochManager};
use crate::scheduler::sgt::error::SerializationGraphError;
use crate::scheduler::sgt::node::{ArcNode, Node, WeakNode};
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::scheduler::Tuple;
use crate::scheduler::{Scheduler, TransactionInfo};
use crate::storage::datatype::Data;
use crate::storage::Access;
use crate::storage::Table;
use crate::workloads::smallbank::SB_SF_MAP;
use crate::workloads::Database;

use crossbeam_epoch as epoch;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thread_local::ThreadLocal;
use tracing::{debug, info};

pub mod transaction_information;

pub mod node;

pub mod epoch_manager;

pub mod error;

type EdgeSets = Vec<(WeakNode, bool)>;

#[derive(Debug)]
pub struct SerializationGraph {
    thread_id: ThreadLocal<String>,
    this_node: ThreadLocal<RefCell<Option<ArcNode>>>,
    txn_ctr: ThreadLocal<RefCell<u64>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    recycled_edge_sets: ThreadLocal<RefCell<Vec<Option<Mutex<EdgeSets>>>>>,
    eg: ThreadLocal<RefCell<EpochGuard>>,
    visited: ThreadLocal<RefCell<HashSet<usize>>>,
    stack: ThreadLocal<RefCell<Vec<WeakNode>>>,
    em: Arc<EpochManager>,
}

impl SerializationGraph {
    pub fn new(size: usize) -> Self {
        info!("Initialise serialization graph with {} thread(s)", size);

        SerializationGraph {
            thread_id: ThreadLocal::new(),
            this_node: ThreadLocal::new(),
            txn_ctr: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
            recycled_edge_sets: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            stack: ThreadLocal::new(),
            eg: ThreadLocal::new(),
            em: Arc::new(EpochManager::new(size as u64)),
        }
    }

    /// Create a node.
    pub fn create_node(&self) -> WeakNode {
        let rec = self
            .recycled_edge_sets
            .get_or(|| RefCell::new(Vec::new()))
            .borrow_mut();

        let node;
        if rec.len() < 2 {
            node = Arc::new(Node::new());
            // TODO: populate
        } else {
            // let incoming = rec.pop().unwrap().unwrap();
            // let outgoing = rec.pop().unwrap().unwrap();
            // node = Arc::new(Node::new_with_sets(incoming, outgoing));
            node = Arc::new(Node::new());
        }

        let weak: WeakNode = Arc::downgrade(&node);
        self.this_node.get().unwrap().borrow_mut().replace(node); // replace local node
        self.eg.get().unwrap().borrow_mut().pin(); // pin to epoch guard
        weak
    }

    /// Cleanup a node.
    pub fn cleanup(&self) {
        let this: ArcNode = self.this_node.get().unwrap().borrow_mut().take().unwrap(); // take

        let mut this_wlock = this.write(); // get write lock
        this_wlock.set_cleaned(); // set as cleaned
        let outgoing = this_wlock.take_outgoing(); // remove edges
        let incoming = this_wlock.take_incoming();
        drop(this_wlock); // drop write lock

        let this_rlock = this.read(); // get write lock

        for (that, rw_edge) in &*outgoing.lock() {
            debug_assert!(
                that.upgrade().is_some(),
                "not found: {}",
                that.as_ptr() as usize
            );
            let that = that.upgrade().unwrap();
            let that_rlock = that.read(); // get read lock on outgoing

            if this_rlock.is_aborted() && !rw_edge {
                that_rlock.set_cascading_abort(); // if this node is aborted and not rw; cascade abort on that node
            } else if !that_rlock.is_cleaned() {
                that_rlock.remove_incoming(Arc::downgrade(&this));
            }

            drop(that_rlock);
        }
        outgoing.lock().clear();

        if this_rlock.is_aborted() {
            incoming.lock().clear();
        }

        drop(this_rlock);

        debug_assert!(incoming.lock().is_empty());
        debug_assert!(outgoing.lock().is_empty());

        self.recycled_edge_sets
            .get_or(|| RefCell::new(Vec::new()))
            .borrow_mut()
            .push(Some(incoming));

        self.recycled_edge_sets
            .get_or(|| RefCell::new(Vec::new()))
            .borrow_mut()
            .push(Some(outgoing));

        self.eg.get().unwrap().borrow_mut().add(this); // add to garabge collector
    }

    /// Insert an incoming edge into (this) node from (from) node, followed by a cycle check.
    pub fn insert_and_check(&self, this: &ArcNode, from: WeakNode, rw: bool) -> bool {
        let this: WeakNode = Arc::downgrade(this);

        if this.ptr_eq(&from) {
            return true; // check for self edge
        }

        let this: ArcNode = this.upgrade().unwrap();
        let this_rlock = this.read(); // get read lock on this_node
        let exists = this_rlock.incoming_edge_exists(from.clone()); // check from_node exists
        drop(this_rlock);

        loop {
            if !exists {
                let this_rlock = this.read();
                let from = from.upgrade().unwrap();
                let from_rlock = from.read();

                if from_rlock.is_aborted() && !rw {
                    this_rlock.set_cascading_abort();
                    drop(from_rlock);
                    drop(this_rlock);
                    return false; // cascading abort
                }

                if from_rlock.is_cleaned() {
                    drop(from_rlock);
                    drop(this_rlock);
                    return true; // from node cleaned
                }

                if from_rlock.is_checked() {
                    drop(from_rlock); // drop read lock
                    drop(this_rlock);
                    continue; // from node checked
                }

                this_rlock.insert_incoming(Arc::downgrade(&from), rw); // insert edge
                from_rlock.insert_outgoing(Arc::downgrade(&this), rw);
                drop(from_rlock);
                drop(this_rlock);
                let is_cycle = self.cycle_check(&this); // cycle check
                return !is_cycle;
            } else {
                return true; // edge exists
            }
        }
    }

    pub fn cycle_check(&self, this: &ArcNode) -> bool {
        let start = Arc::downgrade(&this);
        let mut visited = self
            .visited
            .get_or(|| RefCell::new(HashSet::new()))
            .borrow_mut();

        let mut stack = self.stack.get_or(|| RefCell::new(Vec::new())).borrow_mut();

        visited.clear();
        stack.clear();

        let this_rlock = this.read();
        let outgoing = this_rlock.get_outgoing(true, true);
        let mut out: Vec<WeakNode> = outgoing.into_iter().map(|(node, _)| node).collect();
        stack.append(&mut out);
        drop(this_rlock);

        while let Some(current) = stack.pop() {
            if start.ptr_eq(&current) {
                return true; // cycle found
            }

            let current_ptr = current.as_ptr() as usize;
            if visited.contains(&current_ptr) {
                continue; // already visited
            }
            visited.insert(current_ptr);

            let arc_current = current.upgrade().unwrap();
            let rlock = arc_current.read();
            let val1 = !(rlock.is_committed() || rlock.is_aborted() || rlock.is_cascading_abort());
            if val1 {
                let val2 =
                    !(rlock.is_committed() || rlock.is_aborted() || rlock.is_cascading_abort());
                let outgoing = rlock.get_outgoing(val1, val2);
                let mut out: Vec<WeakNode> = outgoing.into_iter().map(|(node, _)| node).collect();
                stack.append(&mut out);
            }

            drop(rlock);
        }

        false
    }

    /// Check if a transaction needs to abort.
    pub fn needs_abort(&self, this: &ArcNode) -> bool {
        let this_rlock = this.read();
        let aborted = this_rlock.is_aborted();
        let cascading_abort = this_rlock.is_cascading_abort();
        drop(this_rlock);

        aborted || cascading_abort
    }

    /// Set aborted and cleanup.
    pub fn abort_procedure(&self, this: &ArcNode) {
        let this_rlock = this.read();
        this_rlock.set_aborted();
        drop(this_rlock);

        self.cleanup();
    }

    /// Check if a transaction can be committed.
    pub fn check_committed(
        &self,
        this: &ArcNode,
        meta: &TransactionInfo,
        database: &Database,
    ) -> bool {
        if self.needs_abort(&this) {
            return false; // abort check
        }

        let this_wlock = this.write();
        this_wlock.set_checked(true);
        drop(this_wlock);

        let this_rlock = this.read();

        if this_rlock.is_incoming() {
            this_rlock.set_checked(false);
            drop(this_rlock);
            return false;
        }
        drop(this_rlock);

        if self.needs_abort(&this) {
            return false; // abort check
        }

        let success = self.erase_graph_constraints(&this, meta, database);

        if success {
            self.cleanup();
        }

        success
    }

    /// Cycle check then commit.
    pub fn erase_graph_constraints(
        &self,
        this: &ArcNode,
        meta: &TransactionInfo,
        database: &Database,
    ) -> bool {
        let guard = &epoch::pin();
        let is_cycle = self.cycle_check(&this);

        let this_rlock = this.read();

        if is_cycle {
            this_rlock.set_aborted();
            drop(this_rlock);
            return false;
        }

        let ops = self
            .txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get(); // get operations

        for op in ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                prv,
            } = op;

            let table = database.get_table(table_id);
            let tuple = table.get_tuple(column_id, offset);
            let rwtable = table.get_rwtable(offset);

            match op_type {
                OperationType::Read => {
                    rwtable.erase(prv); // remove access
                }
                OperationType::Write => {
                    tuple.get().commit(); // revert
                    rwtable.erase(prv); // remove access
                }
            }
        }

        this_rlock.set_committed();
        drop(this_rlock);
        drop(guard);
        true
    }
}

impl SerializationGraph {
    pub fn begin(&self) -> TransactionInfo {
        let handle = std::thread::current();
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // inc txn ctr

        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // init txn info

        self.eg
            .get_or(|| RefCell::new(EpochGuard::new(Arc::clone(&self.em)))); // init epoch guard

        self.this_node.get_or(|| RefCell::new(None)); // init this_node
                                                      //        self.thread_id.get_or(|| handle.name().unwrap().to_string());
        let weak = self.create_node();

        debug_assert!(self.txn_ctr.get().is_some(), "{:?}", self.txn_ctr);
        debug_assert!(self.txn_info.get().is_some(), "{:?}", self.txn_info);
        debug_assert!(self.this_node.get().is_some(), "{:?}", self.this_node);

        TransactionInfo::SerializationGraph(weak)
    }

    pub fn read_value(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionInfo,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        debug!("read");
        if let TransactionInfo::SerializationGraph(_) = meta {
            let guard = &epoch::pin(); // pin thread to garbage collector

            let this: ArcNode =
                Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap()); // this node

            if self.needs_abort(&this) {
                drop(guard); // unpin
                debug!("needs abort");
                return Err(self.abort(meta, database)); // abort
            }

            let table: &Table = database.get_table(table_id); // get table
            let rw_table = table.get_rwtable(offset); // get rwtable
            let prv = rw_table.push_front(Access::Read(meta.clone())); // append access
            let lsn = table.get_lsn(offset);

            debug!("spin");
            loop {
                let i = lsn.load(Ordering::Relaxed); // current lsn
                if i == prv {
                    break; // break when prv == lsn
                }
            }
            debug!("lsn = prv");
            let snapshot = rw_table.iter(guard); // iterator over access history

            let mut cyclic = false; // insert and check each access
            for (id, access) in snapshot {
                if id < &prv {
                    match access {
                        Access::Write(txn_info) => match txn_info {
                            TransactionInfo::SerializationGraph(from_node) => {
                                if !self.insert_and_check(&this, from_node.clone(), false) {
                                    cyclic = true;
                                    break;
                                }
                            }
                            _ => panic!("unexpected transaction information"),
                        },
                        Access::Read(_) => {}
                    }
                }
            }
            debug!("edges added");

            if cyclic {
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn
                drop(guard); // unpin
                self.abort(meta, database); // abort
                debug!("cycle found");
                return Err(SerializationGraphError::CycleFound.into());
            }

            let vals = table
                .get_tuple(column_id, offset)
                .get()
                .get_value()
                .unwrap()
                .get_value(); // read

            debug!("read vals");
            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Read, table_id, column_id, offset, prv); // record operation
            debug!("registered");
            lsn.store(prv + 1, Ordering::Release); // update lsn
            drop(guard); // unpin
            Ok(vals)
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Write operation.
    pub fn write_value(
        &self,
        value: &Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionInfo,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        debug!("write");
        if let TransactionInfo::SerializationGraph(_) = meta {
            let guard = &epoch::pin(); // pin thread to garbage collector
            let this: ArcNode =
                Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap()); // this node

            let table = database.get_table(table_id);
            let rw_table = table.get_rwtable(offset);
            let lsn = table.get_lsn(offset);
            let mut prv; // init prv

            loop {
                if self.needs_abort(&this) {
                    drop(guard);
                    return Err(self.abort(meta, database));
                }

                prv = rw_table.push_front(Access::Write(meta.clone()));

                loop {
                    let i = lsn.load(Ordering::Relaxed); // current lsn

                    if i == prv {
                        break; // if current = previous then this transaction can execute
                    }
                }

                let snapshot = rw_table.iter(guard);

                let mut wait = false;
                let mut cyclic = false;
                for (id, access) in snapshot {
                    if id < &prv {
                        match access {
                            Access::Write(from) => {
                                if let TransactionInfo::SerializationGraph(from_node) = from {
                                    debug_assert!(
                                        from_node.upgrade().is_some(),
                                        "not found: {}",
                                        from_node.as_ptr() as usize
                                    );

                                    let fm = from_node.upgrade().unwrap();

                                    let rlock = fm.read();
                                    if !rlock.is_committed() {
                                        drop(rlock);
                                        if !self.insert_and_check(&this, from_node.clone(), false) {
                                            cyclic = true;
                                            break;
                                        }
                                        wait = true;

                                        break;
                                    } else {
                                    }
                                }
                            }
                            Access::Read(_) => {}
                        }
                    }
                }

                if cyclic {
                    rw_table.erase(prv); // remove from rw table
                    lsn.store(prv + 1, Ordering::Release); // update lsn
                    drop(guard);
                    self.abort(meta, database);
                    return Err(SerializationGraphError::CycleFound.into());
                }

                if wait {
                    rw_table.erase(prv); // remove from rw table
                    lsn.store(prv + 1, Ordering::Release); // update lsn
                    continue;
                }
                break;
            }

            let snapshot = rw_table.iter(guard);

            let mut cyclic = false;
            for (id, access) in snapshot {
                if id < &prv {
                    match access {
                        Access::Read(from) => {
                            if let TransactionInfo::SerializationGraph(from_node) = from {
                                if !self.insert_and_check(&this, from_node.clone(), true) {
                                    cyclic = true;
                                    break;
                                }
                            }
                        }
                        Access::Write(_) => {}
                    }
                }
            }

            if cyclic {
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn
                drop(guard);
                self.abort(meta, database);
                return Err(SerializationGraphError::CycleFound.into());
            }

            table
                .get_tuple(column_id, offset)
                .get()
                .set_value(value)
                .unwrap();

            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Write, table_id, column_id, offset, prv); // record operation

            lsn.store(prv + 1, Ordering::Release); // update lsn
            drop(guard);
            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit operation.
    pub fn commit(&self, meta: &TransactionInfo, database: &Database) -> Result<(), NonFatalError> {
        let this: ArcNode = Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());

        loop {
            if self.needs_abort(&this) {
                return Err(self.abort(meta, database));
            }

            if self.check_committed(&this, meta, database) {
                break;
            }
        }

        self.eg.get().unwrap().borrow_mut().unpin(); // unpin txn

        Ok(())
    }

    /// Abort operation.
    ///
    /// Call sg abort procedure then remove accesses and revert writes.
    pub fn abort(&self, meta: &TransactionInfo, database: &Database) -> NonFatalError {
        let this: ArcNode = Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());
        let guard = &epoch::pin();

        let ops = self
            .txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get(); // get operations

        self.abort_procedure(&this); // sg abort

        for op in ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                prv,
            } = op;

            let table = database.get_table(table_id);
            let rwtable = table.get_rwtable(offset);
            let tuple = table.get_tuple(column_id, offset);

            match op_type {
                OperationType::Read => {
                    rwtable.erase(prv); // remove access
                }
                OperationType::Write => {
                    tuple.get().revert(); // revert
                    rwtable.erase(prv); // remove access
                }
            }
        }
        drop(guard); // unpin
        self.eg.get().unwrap().borrow_mut().unpin(); // unpin txn

        NonFatalError::NonSerializable // TODO: return the why
    }
}

impl fmt::Display for SerializationGraph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f).unwrap();
        writeln!(
            f,
            "this_node: {}",
            self.this_node.get().unwrap().borrow().as_ref().unwrap()
        )
        .unwrap();
        writeln!(f, "txn_ctr: {}", self.txn_ctr.get().unwrap().borrow()).unwrap();
        Ok(())
    }
}
