use crate::common::error::NonFatalError;
use crate::scheduler::sgt::error::SerializationGraphError;
use crate::scheduler::sgt::node::EdgeSet;
use crate::scheduler::sgt::node::{Edge, RwNode};
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use crossbeam_epoch::Guard;
use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::{debug, info};

pub mod transaction_information;

pub mod node;

pub mod error;

#[derive(Debug)]
pub struct SerializationGraph<'a> {
    txn_ctr: ThreadLocal<RefCell<u64>>,
    this_node: ThreadLocal<RefCell<Option<&'a RwNode<'a>>>>,
    recycled: ThreadLocal<RefCell<Vec<EdgeSet<'a>>>>,
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
            recycled: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            stack: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
        }
    }

    pub fn get_transaction(&self) -> &'a RwNode<'a> {
        self.this_node
            .get()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .clone()
    }

    pub fn get_operations(&self) -> Vec<Operation> {
        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get()
    }

    pub fn create_node(&self) -> usize {
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info

        let mut recycled = self.recycled.get_or(|| RefCell::new(vec![])).borrow_mut();

        let incoming;
        let outgoing;
        if recycled.is_empty() {
            incoming = Mutex::new(FxHashSet::default());
            outgoing = Mutex::new(FxHashSet::default());
        } else {
            incoming = recycled.pop().unwrap();
            outgoing = recycled.pop().unwrap();
        }

        let node = Box::new(RwNode::new_with_sets(incoming, outgoing)); // allocated node on the heap
        let id = node::to_usize(node);
        let nref = node::from_usize(id);
        self.this_node
            .get_or(|| RefCell::new(None))
            .borrow_mut()
            .replace(nref); // replace local node reference

        id
    }

    /// Cleanup a node.
    pub fn cleanup<'g>(&self, this: &'a RwNode<'a>, guard: &'g Guard) {
        let this_wlock = this.write(); // get write lock
        this.set_cleaned(); // set as cleaned
        let outgoing = this.take_outgoing(); // remove edges
        let incoming = this.take_incoming();
        drop(this_wlock); // drop write lock

        let mut g = outgoing.lock();
        let edge_set = g.iter();

        for edge in edge_set {
            match edge {
                Edge::ReadWrite(that) => {
                    let that_rlock = that.read(); // get read lock on outgoing edge
                    if !that.is_cleaned() {
                        that.remove_incoming(&Edge::ReadWrite(this)); // remove incoming from this node
                    }
                    drop(that_rlock);
                }
                Edge::Other(that) => {
                    if this.is_aborted() {
                        that.set_cascading_abort(); // if this node is aborted and not rw; cascade abort on that node
                    } else {
                        let that_rlock = that.read(); // get read lock on outgoing edge
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::Other(this));
                        }
                        drop(that_rlock);
                    }
                }
            }
        }
        g.clear();
        drop(g);

        if this.is_aborted() {
            incoming.lock().clear();
        }

        let mut recycled = self.recycled.get_or(|| RefCell::new(vec![])).borrow_mut();
        recycled.push(incoming);
        recycled.push(outgoing);

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

        let exists = this.incoming_edge_exists(from); // check if (from) --> (this) already exists

        loop {
            // if does not exist
            if !exists {
                // if (from) has aborted and is ww/wr edge
                if from.is_aborted() && !rw {
                    this.set_cascading_abort();
                    return false; // then cascadingly abort (this)
                }

                let from_rlock = from.read(); // get shared lock on (from)

                // if (from) cleaned
                if from.is_cleaned() {
                    drop(from_rlock);
                    return true; // do not insert edge
                }

                // if (from) checked
                if from.is_checked() {
                    drop(from_rlock);
                    continue; // in process of terminating so try again
                }

                let this_rlock = this.read(); // get shared lock on (this)
                this.insert_incoming(from, rw); // insert edge
                from.insert_outgoing(this, rw);
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
        let outgoing = this.get_outgoing(); // FxHashSet<Edge<'a>>
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
            let val1 =
                !(current.is_committed() || current.is_aborted() || current.is_cascading_abort());
            if val1 {
                let outgoing = current.get_outgoing();
                let mut out = outgoing.into_iter().collect();
                stack.append(&mut out);
            }

            drop(rlock);
        }

        false
    }

    /// Check if a transaction needs to abort.
    pub fn needs_abort(&self, this: &'a RwNode<'a>) -> bool {
        let aborted = this.is_aborted();
        let cascading_abort = this.is_cascading_abort();

        aborted || cascading_abort
    }

    /// Set aborted and cleanup.
    pub fn abort_procedure<'g>(&self, this: &'a RwNode<'a>, guard: &'g Guard) {
        this.set_aborted();

        self.cleanup(this, guard);
    }

    /// Check if a transaction can be committed.
    pub fn check_committed<'g>(
        &self,
        this: &'a RwNode<'a>,
        database: &Database,
        guard: &'g Guard,
    ) -> bool {
        if self.needs_abort(this) {
            return false; // abort check
        }

        let this_wlock = this.write();
        this.set_checked(true);
        drop(this_wlock);

        let this_rlock = this.read();

        if this.is_incoming() {
            this.set_checked(false);
            drop(this_rlock);
            return false;
        }
        drop(this_rlock);

        if self.needs_abort(this) {
            return false; // abort check
        }

        let success = self.erase_graph_constraints(this, database, guard);

        if success {
            self.cleanup(this, guard);
        }

        success
    }

    /// Cycle check then commit.
    pub fn erase_graph_constraints<'g>(
        &self,
        this: &'a RwNode<'a>,
        database: &Database,
        guard: &'g Guard,
    ) -> bool {
        let is_cycle = self.cycle_check(&this); // cycle check

        if is_cycle {
            this.set_aborted(); // cycle so abort (this)
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
                    rwtable.erase(prv, guard); // remove access
                }
                OperationType::Write => {
                    tuple.get().commit(); // revert
                    rwtable.erase(prv, guard); // remove access
                }
            }
        }

        this.set_committed();

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
            let prv = rw_table.push_front(Access::Read(meta.clone()), guard); // append access
            let lsn = table.get_lsn(offset);

            spin(prv, lsn);

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

            if cyclic {
                rw_table.erase(prv, guard); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn
                self.abort(database, guard); // abort

                return Err(SerializationGraphError::CycleFound.into());
            }

            let vals = table
                .get_tuple(column_id, offset)
                .get()
                .get_value()
                .unwrap()
                .get_value(); // read

            lsn.store(prv + 1, Ordering::Release); // update lsn

            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Read, table_id, column_id, offset, prv); // record operation

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
        if let TransactionId::SerializationGraph(_) = meta {
            let this = self.get_transaction();
            let table = database.get_table(table_id);
            let rw_table = table.get_rwtable(offset);
            let lsn = table.get_lsn(offset);
            let mut prv; // init prv

            loop {
                if self.needs_abort(this) {
                    return Err(self.abort(database, guard)); // check if needs abort
                }

                prv = rw_table.push_front(Access::Write(meta.clone()), guard); // add access

                spin(prv, lsn); // wait until my turn

                let snapshot = rw_table.iter(guard);

                let mut wait = false;
                let mut cyclic = false;

                // for each access
                // if is not committed
                // if write access then wait;
                for (id, access) in snapshot {
                    if id < &prv {
                        match access {
                            Access::Write(from) => {
                                if let TransactionId::SerializationGraph(from_addr) = from {
                                    let from = node::from_usize(*from_addr); // convert to ptr

                                    if !from.is_committed() {
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
                    rw_table.erase(prv, guard); // remove from rw table
                    lsn.store(prv + 1, Ordering::Release); // update lsn
                    self.abort(database, guard);
                    return Err(SerializationGraphError::CycleFound.into());
                }

                if wait {
                    rw_table.erase(prv, guard); // remove from rw table
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
                rw_table.erase(prv, guard); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn
                self.abort(database, guard);
                return Err(SerializationGraphError::CycleFound.into());
            }

            if let Err(e) =
                table
                    .get_tuple(column_id, offset)
                    .get()
                    .set_value(value, prv, meta.clone())
            {
                let lsn = lsn.load(Ordering::Acquire);
                let row = table.get_tuple(column_id, offset);
                panic!(
                    "lsn: {}, prv: {}, error: {}, cyclic: {}, row: {}",
                    lsn, prv, e, cyclic, row
                );
            }

            lsn.store(prv + 1, Ordering::Release); // update lsn

            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Write, table_id, column_id, offset, prv); // record operation

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
            let cc = self.check_committed(&this, database, guard);
            if cc {
                break;
            }

            debug!("commit attempt: {}", cc);
        }

        Ok(())
    }

    /// Abort operation.
    ///
    /// Call sg abort procedure then remove accesses and revert writes.
    pub fn abort<'g>(&self, database: &Database, guard: &'g Guard) -> NonFatalError {
        let this = self.get_transaction();
        let ops = self.get_operations();

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
                    rwtable.erase(prv, guard); // remove access
                }
                OperationType::Write => {
                    rwtable.erase(prv, guard); // remove access
                    tuple.get().revert(); // revert
                }
            }
        }

        NonFatalError::NonSerializable // TODO: return the why
    }
}

fn spin(prv: u64, lsn: &AtomicU64) {
    let mut i = 0;
    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_cycle() {
        let n1 = RwNode::new();
        let n2 = RwNode::new();
        n1.read().insert_outgoing(&n2, true);
        n2.read().insert_incoming(&n1, true);

        let sg = SerializationGraph::new(1);

        assert_eq!(sg.cycle_check(&n1), false);
        assert_eq!(sg.cycle_check(&n2), false);
    }

    #[test]
    fn direct_cycle() {
        let n1 = RwNode::new();
        let n2 = RwNode::new();
        n1.read().insert_incoming(&n2, true);
        n1.read().insert_outgoing(&n2, true);

        n2.read().insert_incoming(&n1, true);
        n2.read().insert_outgoing(&n1, true);

        let sg = SerializationGraph::new(1);

        assert_eq!(sg.cycle_check(&n1), true);
        assert_eq!(sg.cycle_check(&n2), true);
    }

    #[test]
    fn trans_cycle() {
        let n1 = RwNode::new();
        let n2 = RwNode::new();
        let n3 = RwNode::new();

        n1.read().insert_outgoing(&n2, true);
        n1.read().insert_incoming(&n3, true);

        n2.read().insert_outgoing(&n3, true);
        n2.read().insert_incoming(&n1, true);

        n3.read().insert_outgoing(&n1, true);
        n3.read().insert_incoming(&n2, true);

        let sg = SerializationGraph::new(1);

        assert_eq!(sg.cycle_check(&n1), true);
        assert_eq!(sg.cycle_check(&n2), true);
        assert_eq!(sg.cycle_check(&n3), true);
    }
}
