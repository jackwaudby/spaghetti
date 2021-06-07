use crate::common::error::NonFatalError;
use crate::scheduler::sgt::error::SerializationGraphError;
use crate::scheduler::sgt::node::{Edge, RwNode};
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use crossbeam_epoch::{self as epoch, Guard};
use rustc_hash::FxHashSet;
use std::cell::RefCell;

use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thread_local::ThreadLocal;
use tracing::{debug, info};

pub mod transaction_information;

pub mod node;

pub mod error;

#[derive(Debug)]
pub struct SerializationGraph<'a> {
    txn_ctr: ThreadLocal<RefCell<u64>>,
    this_node: ThreadLocal<RefCell<Option<&'a RwNode<'a>>>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    stack: ThreadLocal<RefCell<Vec<Edge<'a>>>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

impl<'a> SerializationGraph<'a> {
    pub fn new(size: usize) -> Self {
        info!("Initialise serialization graph with {} thread(s)", size);

        Self {
            txn_ctr: ThreadLocal::new(),
            this_node: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            stack: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
        }
    }

    pub fn create_node(&self) -> usize {
        let node = Box::new(RwNode::new()); // allocated node on the heap
        let id = node::to_usize(node);
        let nref = node::from_usize(id);
        self.this_node
            .get_or(|| RefCell::new(None))
            .borrow_mut()
            .replace(nref); // replace local node reference

        id
    }

    /// Cleanup a node.
    pub fn cleanup<'g>(&self, guard: &'g Guard) {
        let this: &RwNode = self.this_node.get().unwrap().borrow_mut().take().unwrap(); // take

        let mut this_wlock = this.write(); // get write lock
        this_wlock.set_cleaned(); // set as cleaned
        let mut outgoing = this_wlock.take_outgoing(); // remove edges
        let mut incoming = this_wlock.take_incoming();
        drop(this_wlock); // drop write lock

        let this_rlock = this.read(); // read write lock

        for edge in &outgoing {
            match edge {
                Edge::ReadWrite(that) => {
                    let that_rlock = that.read(); // get read lock on outgoing edge
                    if !that_rlock.is_cleaned() {
                        that_rlock.remove_incoming(edge);
                    }
                    drop(that_rlock);
                }
                Edge::Other(that) => {
                    let that_rlock = that.read(); // get read lock on outgoing edge
                    if this_rlock.is_aborted() {
                        that_rlock.set_cascading_abort(); // if this node is aborted and not rw; cascade abort on that node
                    } else if !that_rlock.is_cleaned() {
                        that_rlock.remove_incoming(edge);
                    }
                    drop(that_rlock);
                }
            }
        }
        outgoing.clear();

        if this_rlock.is_aborted() {
            incoming.clear();
        }

        drop(this_rlock);

        let this_ptr: *const RwNode<'a> = this;
        let this_usize = this_ptr as usize;
        let boxed_node = node::to_box(this_usize);

        unsafe {
            guard.defer_unchecked(move || {
                drop(boxed_node);
            });
        }
    }

    /// Insert an incoming edge into (this) node from (from) node, followed by a cycle check.
    pub fn insert_and_check(&self, this: &'a RwNode<'a>, from: &'a RwNode<'a>, rw: bool) -> bool {
        if std::ptr::eq(this, from) {
            return true; // check for self edge
        }

        let this_rlock = this.read(); // get read lock on this_node
        let exists = this_rlock.incoming_edge_exists(from.clone()); // check from_node exists
        drop(this_rlock);

        loop {
            if !exists {
                let this_rlock = this.read();
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

                this_rlock.insert_incoming(from, rw); // insert edge
                from_rlock.insert_outgoing(this, rw);
                drop(from_rlock);
                drop(this_rlock);
                let is_cycle = self.cycle_check(&this); // cycle check
                return !is_cycle;
            } else {
                return true; // edge exists
            }
        }
    }

    pub fn cycle_check(&self, this: &'a RwNode<'a>) -> bool {
        debug!("cycle check");
        let start = this;
        let mut visited = self
            .visited
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut();

        let mut stack = self.stack.get_or(|| RefCell::new(Vec::new())).borrow_mut();

        visited.clear();
        stack.clear();

        let this_rlock = this.read();
        let outgoing = this_rlock.get_outgoing(); // FxHashSet<Edge<'a>>
        let mut out = outgoing.into_iter().collect();
        stack.append(&mut out);
        drop(this_rlock);

        while let Some(edge) = stack.pop() {
            let current = match edge {
                Edge::ReadWrite(node) => node,
                Edge::Other(node) => node,
            };

            if std::ptr::eq(start, current) {
                return true; // cycle found
            }

            let current_addr = current as *const _ as usize;
            if visited.contains(&current_addr) {
                continue; // already visited
            }

            visited.insert(current_addr);

            let rlock = current.read();
            let val1 = !(rlock.is_committed() || rlock.is_aborted() || rlock.is_cascading_abort());
            if val1 {
                let val2 =
                    !(rlock.is_committed() || rlock.is_aborted() || rlock.is_cascading_abort());
                let outgoing = rlock.get_outgoing();
                let mut out = outgoing.into_iter().collect();
                stack.append(&mut out);
            }

            drop(rlock);
        }

        false
    }

    /// Check if a transaction needs to abort.
    pub fn needs_abort(&self, this: &RwNode) -> bool {
        let this_rlock = this.read();
        let aborted = this_rlock.is_aborted();
        let cascading_abort = this_rlock.is_cascading_abort();
        drop(this_rlock);

        aborted || cascading_abort
    }

    /// Set aborted and cleanup.
    pub fn abort_procedure<'g>(&self, this: &RwNode, guard: &'g Guard) {
        let this_rlock = this.read();
        this_rlock.set_aborted();
        drop(this_rlock);

        self.cleanup(guard);
    }

    /// Check if a transaction can be committed.
    pub fn check_committed<'g>(
        &self,
        this: &'a RwNode<'a>,
        database: &Database,
        guard: &'g Guard,
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

        let success = self.erase_graph_constraints(&this, database);

        if success {
            self.cleanup(guard);
        }

        success
    }

    /// Cycle check then commit.
    pub fn erase_graph_constraints(&self, this: &'a RwNode<'a>, database: &Database) -> bool {
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

    pub fn begin(&self) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // increment txn ctr
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info

        let id = self.create_node(); // create node

        TransactionId::SerializationGraph(id)
    }

    pub fn read_value<'g>(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> Result<Data, NonFatalError> {
        debug!("read");
        if let TransactionId::SerializationGraph(_) = meta {
            let this = self
                .this_node
                .get()
                .unwrap()
                .borrow()
                .as_ref()
                .unwrap()
                .clone(); // this node

            if self.needs_abort(&this) {
                drop(guard); // unpin
                debug!("needs abort");
                return Err(self.abort(database, guard)); // abort
            }

            let table: &Table = database.get_table(table_id); // get table
            let rw_table = table.get_rwtable(offset); // get rwtable
            let prv = rw_table.push_front(Access::Read(meta.clone())); // append access
            let lsn = table.get_lsn(offset);

            debug!("spin");
            // loop {
            //     let i = lsn.load(Ordering::Relaxed); // current lsn
            //     if i == prv {
            //         break; // break when prv == lsn
            //     }
            // }

            spin(prv, lsn);
            debug!("lsn = prv");
            let snapshot = rw_table.iter(guard); // iterator over access history

            let mut cyclic = false; // insert and check each access
            for (id, access) in snapshot {
                if id < &prv {
                    match access {
                        Access::Write(txn_info) => match txn_info {
                            TransactionId::SerializationGraph(from_addr) => {
                                let from = node::from_usize(*from_addr); // convert to ptr

                                if !self.insert_and_check(this, from, false) {
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
                self.abort(database, guard); // abort
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
    pub fn write_value<'g>(
        &self,
        value: &Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> Result<(), NonFatalError> {
        debug!("write");
        if let TransactionId::SerializationGraph(_) = meta {
            let this = &self
                .this_node
                .get()
                .unwrap()
                .borrow()
                .as_ref()
                .unwrap()
                .clone(); // this node

            let table = database.get_table(table_id);
            let rw_table = table.get_rwtable(offset);
            let lsn = table.get_lsn(offset);
            let mut prv; // init prv

            loop {
                if self.needs_abort(&this) {
                    drop(guard);
                    return Err(self.abort(database, guard));
                }

                prv = rw_table.push_front(Access::Write(meta.clone()));

                spin(prv, lsn);
                // loop {
                //     let i = lsn.load(Ordering::Relaxed); // current lsn

                //     if i == prv {
                //         break; // if current = previous then this transaction can execute
                //     }
                // }

                let snapshot = rw_table.iter(guard);

                let mut wait = false;
                let mut cyclic = false;
                for (id, access) in snapshot {
                    if id < &prv {
                        match access {
                            Access::Write(from) => {
                                if let TransactionId::SerializationGraph(from_addr) = from {
                                    let from = node::from_usize(*from_addr); // convert to ptr

                                    let rlock = from.read();
                                    if !rlock.is_committed() {
                                        drop(rlock);

                                        if !self.insert_and_check(this, from, false) {
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
                    self.abort(database, guard);
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
                            if let TransactionId::SerializationGraph(from_addr) = from {
                                let from = node::from_usize(*from_addr); // convert to ptr

                                if !self.insert_and_check(this, from, true) {
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
                self.abort(database, guard);
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

            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit operation.
    pub fn commit<'g>(&self, database: &Database, guard: &'g Guard) -> Result<(), NonFatalError> {
        let this = self
            .this_node
            .get()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .clone();

        loop {
            if self.needs_abort(&this) {
                return Err(self.abort(database, guard));
            }
            debug!("commit attempt");

            if self.check_committed(&this, database, guard) {
                break;
            }
        }

        Ok(())
    }

    /// Abort operation.
    ///
    /// Call sg abort procedure then remove accesses and revert writes.
    pub fn abort<'g>(&self, database: &Database, guard: &'g Guard) -> NonFatalError {
        let this = self
            .this_node
            .get()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .clone();

        let ops = self
            .txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get(); // get operations

        self.abort_procedure(&this, guard); // sg abort

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

        NonFatalError::NonSerializable // TODO: return the why
    }
}

// impl fmt::Display for SerializationGraph<'_> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         writeln!(f).unwrap();
//         writeln!(
//             f,
//             "this_node: {}",
//             self.this_node.get().unwrap().borrow().as_ref().unwrap()
//         )
//         .unwrap();
//         writeln!(f, "txn_ctr: {}", self.txn_ctr.get().unwrap().borrow()).unwrap();
//         Ok(())
//     }
// }

fn spin(prv: u64, lsn: &AtomicU64) {
    loop {
        let i = lsn.load(Ordering::Relaxed); // current lsn
        if i == prv {
            break; // break when prv == lsn
        }
    }
}
