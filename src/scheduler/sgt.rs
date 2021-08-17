use crate::common::error::NonFatalError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::sgt::error::SerializationGraphError;
use crate::scheduler::sgt::node::EdgeSet;
use crate::scheduler::sgt::node::{Edge, RwNode};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::Database;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Guard;
use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::{debug, info, instrument, Span};

pub mod node;

pub mod error;

#[derive(Debug)]
pub struct SerializationGraph<'a> {
    txn_ctr: ThreadLocal<RefCell<usize>>,
    this_node: ThreadLocal<RefCell<Option<&'a RwNode>>>,
    recycled: ThreadLocal<RefCell<Vec<EdgeSet>>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    stack: ThreadLocal<RefCell<Vec<Edge>>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

impl<'a> SerializationGraph<'a> {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
    }

    /// Initialise a serialization graph scheduler.
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

    /// Returns a shared reference to the transaction currently executing on this thread.
    pub fn get_transaction(&self) -> &'a RwNode {
        self.this_node
            .get()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .clone()
    }

    /// Returns the operations successfully executed by the transaction currently executing on this thread.
    pub fn get_operations(&self) -> Vec<Operation> {
        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get()
    }

    /// Record an operation.
    pub fn record(
        &self,
        op_type: OperationType,
        table_id: usize,
        column_id: usize,
        offset: usize,
        prv: u64,
    ) {
        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .add(op_type, table_id, column_id, offset, prv);
    }

    /// Create a node in the graph.
    pub fn create_node(&self) -> usize {
        let thread_id: usize = std::thread::current().name().unwrap().parse().unwrap();
        let thread_ctr = *self.txn_ctr.get().unwrap().borrow();
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new());

        let mut _recycled = self.recycled.get_or(|| RefCell::new(vec![])).borrow_mut(); // TODO

        let incoming;
        let outgoing;
        // if recycled.is_empty() {
        incoming = Mutex::new(FxHashSet::default());
        outgoing = Mutex::new(FxHashSet::default());
        // } else {
        //     incoming = recycled.pop().unwrap();
        //     outgoing = recycled.pop().unwrap();
        // }

        let node = Box::new(RwNode::new_with_sets(
            thread_id, thread_ctr, incoming, outgoing,
        ));
        let id = node::to_usize(node);
        let nref = node::from_usize(id);

        nref.set_id(id);

        self.this_node
            .get_or(|| RefCell::new(None))
            .borrow_mut()
            .replace(nref); // replace local node reference

        id
    }

    /// Cleanup node after committed or aborted.
    #[instrument(level = "debug", skip(self, this))]
    pub fn cleanup(&self, this: &'a RwNode) {
        // Assumption: read/writes can still be found, thus, edges can still be detected.
        // A node's cleaned flag acts as a barrier for edge insertion.
        let this_wlock = this.write(); // get write lock
        this.set_cleaned(); // set as cleaned
        drop(this_wlock); // drop write lock

        let outgoing = this.take_outgoing(); // remove edge sets
        let incoming = this.take_incoming();

        let mut g = outgoing.lock(); // lock on outgoing edge set

        let this_id = node::ref_to_usize(this); // node id
        let outgoing_set = g.iter(); // iterator over outgoing edge set

        for edge in outgoing_set {
            match edge {
                // (this) -[rw]-> (that)
                Edge::ReadWrite(that_id) => {
                    // Get read lock on outgoing node - prevents node from committing.
                    let that = node::from_usize(*that_id);
                    let that_rlock = that.read();

                    // If active then the node will not be cleaned and edge must be removed.
                    // Else, the node is cleaned and must have aborted.
                    if !that.is_cleaned() {
                        that.remove_incoming(&Edge::ReadWrite(this_id)); // remove incoming from this node
                    }

                    drop(that_rlock); // Release read lock.
                }
                // (this) -[ww]-> (that)
                Edge::WriteWrite(that_id) => {
                    // Get read lock on outgoing node - prevents node from committing.
                    let that = node::from_usize(*that_id);

                    // If this node aborted then the outgoing node must also abort.
                    // Else, this node is committed.
                    if this.is_aborted() {
                        that.set_cascading_abort();
                    } else {
                        let that_rlock = that.read();
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::WriteWrite(this_id));
                        }
                        drop(that_rlock);
                    }
                }
                Edge::WriteRead(that) => {
                    let that = node::from_usize(*that);
                    if this.is_aborted() {
                        if that.is_committed() {
                            panic!("outgoing incorrectly committed");
                        }
                        that.set_cascading_abort(); // if this node is aborted and not rw; cascade abort on that node
                    } else {
                        let that_rlock = that.read(); // get read lock on outgoing edge
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::WriteRead(this_id));
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
        // recycled.push(outgoing);

        let this_ptr: *const RwNode = this;
        let this_usize = this_ptr as usize;
        let boxed_node = node::to_box(this_usize);

        if boxed_node.is_cascading_abort() && boxed_node.is_committed() {
            panic!("transaction should have aborted");
        }

        let cnt = *self.txn_ctr.get_or(|| RefCell::new(0)).borrow();

        SerializationGraph::EG.with(|x| unsafe {
            x.borrow().as_ref().unwrap().defer_unchecked(move || {
                drop(boxed_node);
            });

            if cnt % 64 == 0 {
                x.borrow().as_ref().unwrap().flush();
            }

            let guard = x.borrow_mut().take();
            drop(guard)
        });
    }

    /// Insert an incoming edge into (this) node from (from) node, followed by a cycle check.
    pub fn insert_and_check(&self, this_ref: &'a RwNode, from: Edge) -> bool {
        let this_id = node::ref_to_usize(this_ref); // id of this node

        match from {
            Edge::ReadWrite(from_id) => {
                if this_id == from_id {
                    return true; // check for self edge
                }

                let exists = this_ref.incoming_edge_exists(&from); // check if (from) --> (this) already exists
                loop {
                    if exists {
                        return true; // don't add same edge twice
                    } else {
                        let from_ref = node::from_usize(from_id);
                        let from_rlock = from_ref.read(); // get shared lock on (from)

                        if from_ref.is_cleaned() {
                            drop(from_rlock);
                            return true; // if from is cleaned then it has terminated do not insert edge
                        }

                        if from_ref.is_checked() {
                            drop(from_rlock);
                            continue; // if (from) checked in process of terminating so try again
                        }

                        let this_rlock = this_ref.read(); // get shared lock on (this)

                        this_ref.insert_incoming(Edge::ReadWrite(from_id));
                        from_ref.insert_outgoing(Edge::ReadWrite(this_id));
                        drop(from_rlock);
                        drop(this_rlock);

                        let is_cycle = self.cycle_check(this_ref); // cycle check

                        return !is_cycle;
                    }
                }
            }
            Edge::WriteWrite(from_id) => {
                if this_id == from_id {
                    return true; // check for self edge
                }

                let exists = this_ref.incoming_edge_exists(&from); // check if (from) --> (this) already exists
                loop {
                    if exists {
                        return true; // don't add same edge twice
                    } else {
                        let from_ref = node::from_usize(from_id);

                        if from_ref.is_aborted() || from_ref.is_cascading_abort() {
                            this_ref.set_cascading_abort();
                            return false; // then cascadingly abort (this)
                        }

                        let from_rlock = from_ref.read(); // get shared lock on (from)

                        if from_ref.is_cleaned() {
                            drop(from_rlock);
                            return true; // if from is cleaned then it has terminated do not insert edge
                        }

                        if from_ref.is_checked() {
                            drop(from_rlock);
                            continue; // if (from) checked in process of terminating so try again
                        }

                        let this_rlock = this_ref.read(); // get shared lock on (this)
                        this_ref.insert_incoming(Edge::WriteWrite(from_id));
                        from_ref.insert_outgoing(Edge::WriteWrite(this_id));

                        drop(from_rlock);
                        drop(this_rlock);

                        let is_cycle = self.cycle_check(this_ref); // cycle check

                        return !is_cycle;
                    }
                }
            }

            Edge::WriteRead(from_id) => {
                if this_id == from_id {
                    return true; // check for self edge
                }

                let exists = this_ref.incoming_edge_exists(&from); // check if (from) --> (this) already exists
                loop {
                    if exists {
                        return true; // don't add same edge twice
                    } else {
                        let from_ref = node::from_usize(from_id);

                        let from_rlock = from_ref.read(); // get shared lock on (from)

                        if from_ref.is_aborted() || from_ref.is_cascading_abort() {
                            this_ref.set_cascading_abort();
                            drop(from_rlock);
                            return false; // then cascadingly abort (this)
                        }

                        if from_ref.is_cleaned() {
                            drop(from_rlock);
                            return true; // if from is cleaned then it has terminated do not insert edge
                        }

                        if from_ref.is_checked() {
                            drop(from_rlock);
                            continue; // if (from) checked in process of terminating so try again
                        }

                        let this_rlock = this_ref.read(); // get shared lock on (this)
                        this_ref.insert_incoming(Edge::WriteRead(from_id));
                        from_ref.insert_outgoing(Edge::WriteRead(this_id));

                        drop(from_rlock);
                        drop(this_rlock);

                        let is_cycle = self.cycle_check(this_ref); // cycle check

                        return !is_cycle;
                    }
                }
            }
        }
    }

    pub fn cycle_check(&self, this: &'a RwNode) -> bool {
        let start_id = node::ref_to_usize(this);
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
                Edge::WriteWrite(node) => node,
                Edge::WriteRead(node) => node,
            };

            if start_id == current {
                return true; // cycle found
            }

            if visited.contains(&current) {
                continue; // already visited
            }

            visited.insert(current);

            let current = node::from_usize(current);
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
    pub fn needs_abort(&self, this: &'a RwNode) -> bool {
        let aborted = this.is_aborted();
        let cascading_abort = this.is_cascading_abort();
        aborted || cascading_abort
    }

    /// Begin a transaction.
    #[instrument(level = "debug", skip(self), fields(id))]
    pub fn begin(&self) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // increment txn ctr
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info

        let id = self.create_node(); // create node

        let guard = epoch::pin(); // pin thread

        SerializationGraph::EG.with(|x| x.borrow_mut().replace(guard));

        Span::current().record("id", &id);
        debug!("Begin");

        TransactionId::SerializationGraph(id)
    }

    /// Read operation.
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn read_value(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        match meta {
            TransactionId::SerializationGraph(id) => {
                Span::current().record("id", &id);
            }
            _ => panic!("unexpected txn id"),
        };

        let this = self.get_transaction();

        if this.is_cascading_abort() {
            self.abort(meta, database);
            return Err(SerializationGraphError::CascadingAbort.into());
        }

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let prv = rw_table.push_front(Access::Read(meta.clone()));
        let lsn = table.get_lsn(offset);

        // Safety: ensures exculsive access to the record.
        unsafe { spin(prv, lsn) }; // busy wait

        // On acquiring the 'lock' on the record can be clean or dirty.
        // Dirty is ok here as we allow reads uncommitted data; SGT protects against serializability violations.
        let guard = &epoch::pin(); // pin thread
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        for (id, access) in snapshot {
            // only interested in accesses before this one and that are write operations.
            if id < &prv {
                match access {
                    // W-R conflict
                    Access::Write(from) => {
                        if let TransactionId::SerializationGraph(from_id) = from {
                            if !self.insert_and_check(this, Edge::WriteRead(*from_id)) {
                                cyclic = true;
                                break;
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
            self.abort(meta, database); // abort
            return Err(SerializationGraphError::CycleFound.into());
        }

        let vals = table
            .get_tuple(column_id, offset)
            .get()
            .get_value()
            .unwrap()
            .get_value(); // read

        lsn.store(prv + 1, Ordering::Release); // update lsn

        self.record(OperationType::Read, table_id, column_id, offset, prv); // record operation

        debug!("Read Succeeded");
        Ok(vals)
    }

    /// Write operation.
    ///
    /// A write is executed iff there are no uncommitted writes on a record, else the operation is delayed.
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn write_value(
        &self,
        value: &mut Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        match meta {
            TransactionId::SerializationGraph(id) => {
                Span::current().record("id", &id);
            }
            _ => panic!("unexpected txn id"),
        };

        let this = self.get_transaction();
        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let mut prv;

        loop {
            if self.needs_abort(this) {
                debug!("write failed: cascading abort");
                self.abort(meta, database);
                return Err(SerializationGraphError::CascadingAbort.into()); // check for cascading abort
            }

            prv = rw_table.push_front(Access::Write(meta.clone())); // get ticket

            //   debug!("waiting for access");
            // Safety: ensures exculsive access to the record.
            unsafe { spin(prv, lsn) }; // busy wait
                                       //            debug!("access granted");

            // On acquiring the 'lock' on the record it is possible another transaction has an uncommitted write on this record.
            // In this case the operation is restarted after a cycle check.
            let guard = &epoch::pin(); // pin thread
            let snapshot = rw_table.iter(guard);

            let mut wait = false; // flag indicating if there is an uncommitted write
            let mut cyclic = false; // flag indicating if a cycle has been found

            for (id, access) in snapshot {
                // only interested in accesses before this one and that are write operations.
                if id < &prv {
                    match access {
                        // W-W conflict
                        Access::Write(from) => {
                            if let TransactionId::SerializationGraph(from_addr) = from {
                                let from = node::from_usize(*from_addr); // convert to ptr

                                // check if write access is uncommitted
                                if !from.is_complete() {
                                    // if not in cycle then wait
                                    if !self.insert_and_check(this, Edge::WriteWrite(*from_addr)) {
                                        cyclic = true;
                                        break; // no reason to check other accesses
                                    }

                                    wait = true; // retry operation
                                    break;
                                } else {
                                }
                            }
                        }
                        Access::Read(_) => {}
                    }
                }
            }

            // (i) transaction is in a cycle (cycle = T)
            // abort transaction
            if cyclic {
                debug!("write failed: cycle found");
                rw_table.erase(prv); // remove from rw table
                self.abort(meta, database);
                lsn.store(prv + 1, Ordering::Release); // update lsn
                return Err(SerializationGraphError::CycleFound.into());
            }

            // (ii) there is an uncommitted write (wait = T)
            // restart operation
            if wait {
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn
                debug!("uncommitted write: retry operation");
                continue;
            }

            // (iii) no w-w conflicts -> clean record (both F)
            // check for cascading abort
            if self.needs_abort(this) {
                debug!("write failed: cascading abort");
                rw_table.erase(prv); // remove from rw table
                self.abort(meta, database);
                lsn.store(prv + 1, Ordering::Release); // update lsn

                return Err(SerializationGraphError::CascadingAbort.into());
            }

            break;
        }

        // Now handle R-W conflicts
        let guard = &epoch::pin(); // pin thread
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        // only interested in accesses before this one and that are read operations.
        for (id, access) in snapshot {
            // check for cascading abort
            if self.needs_abort(this) {
                rw_table.erase(prv); // remove from rw table

                self.abort(meta, database);
                lsn.store(prv + 1, Ordering::Release); // update lsn
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            if id < &prv {
                match access {
                    Access::Read(from) => {
                        if let TransactionId::SerializationGraph(from_addr) = from {
                            if !self.insert_and_check(this, Edge::ReadWrite(*from_addr)) {
                                cyclic = true;
                                break;
                            }
                        }
                    }
                    Access::Write(_) => {}
                }
            }
        }

        // (iv) transaction is in a cycle (cycle = T)
        // abort transaction
        if cyclic {
            rw_table.erase(prv); // remove from rw table
            self.abort(meta, database);
            lsn.store(prv + 1, Ordering::Release); // update lsn
            return Err(SerializationGraphError::CycleFound.into());
        }

        if let Err(e) = table.get_tuple(column_id, offset).get().set_value(value) {
            panic!("{}", e); // Assert: never write to an uncommitted value.
        }

        lsn.store(prv + 1, Ordering::Release); // update lsn, giving next operation access.
        self.record(OperationType::Write, table_id, column_id, offset, prv); // record operation

        debug!("write succeeded");
        Ok(())
    }

    /// Commit operation.
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn commit(&self, meta: &TransactionId, database: &Database) -> Result<(), NonFatalError> {
        match meta {
            TransactionId::SerializationGraph(id) => {
                Span::current().record("id", &id);
            }
            _ => panic!("unexpected txn id"),
        };
        debug!("begin commit");

        let this = self.get_transaction();

        loop {
            if this.is_cascading_abort() || this.is_aborted() {
                debug!("commit failed: cascading abort");
                self.abort(meta, database);
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            let this_wlock = this.write();
            this.set_checked(true); // prevents edge insertions
            drop(this_wlock);

            // no lock taken as only outgoing edges can be added from here
            if this.is_incoming() {
                this.set_checked(false); // if incoming then flip back to unchecked
                let is_cycle = self.cycle_check(&this); // cycle check
                if is_cycle {
                    debug!("commit failed: cycle found");
                    this.set_aborted(); // cycle so abort (this)
                }
                continue;
            }
            debug!("commit successful: no incoming edges");

            // no incoming edges and no cycle so commit
            self.tidyup(database, true);
            this.set_committed();
            self.cleanup(this);

            break;
        }

        this.set_complete();

        debug!("committed");
        Ok(())
    }

    /// Abort operation.
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn abort(&self, meta: &TransactionId, database: &Database) -> NonFatalError {
        match meta {
            TransactionId::SerializationGraph(id) => {
                Span::current().record("id", &id);
            }
            _ => panic!("unexpected txn id"),
        };
        debug!("begin abort");

        let this = self.get_transaction();
        this.set_aborted();
        self.cleanup(this);
        self.tidyup(database, false);
        this.set_complete();

        debug!("aborted");
        NonFatalError::NonSerializable // TODO: return the why
    }

    /// Tidyup rwtables and tuples
    #[instrument(level = "debug", skip(self, database, commit))]
    pub fn tidyup(&self, database: &Database, commit: bool) {
        let ops = self.get_operations();

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
                    rwtable.erase(prv);
                }
                OperationType::Write => {
                    if commit {
                        tuple.get().commit();
                    } else {
                        tuple.get().revert();
                    }

                    rwtable.erase(prv);
                }
            }
        }
        debug!("changes committed/reverted: {}", commit);
    }
}

// Busy wait until prv matches lsn.
unsafe fn spin(prv: u64, lsn: &AtomicU64) {
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
        let n1 = RwNode::new(1, 1);
        let id1 = node::to_usize(Box::new(n1));
        let node1 = node::from_usize(id1);

        let n2 = RwNode::new(2, 1);
        let id2 = node::to_usize(Box::new(n2));
        let node2 = node::from_usize(id2);

        node1.insert_outgoing(Edge::WriteWrite(id2));
        node2.insert_incoming(Edge::WriteWrite(id1));

        let sg = SerializationGraph::new(1);

        assert_eq!(sg.cycle_check(node1), false);
        assert_eq!(sg.cycle_check(node2), false);
    }

    #[test]
    fn direct_cycle() {
        let n1 = RwNode::new(1, 1);
        let id1 = node::to_usize(Box::new(n1));
        let node1 = node::from_usize(id1);

        let n2 = RwNode::new(2, 1);
        let id2 = node::to_usize(Box::new(n2));
        let node2 = node::from_usize(id2);

        node1.insert_outgoing(Edge::WriteWrite(id2));
        node2.insert_incoming(Edge::WriteWrite(id1));

        node1.insert_incoming(Edge::WriteWrite(id2));
        node2.insert_outgoing(Edge::WriteWrite(id1));

        let sg = SerializationGraph::new(1);

        assert_eq!(sg.cycle_check(node1), true);
        assert_eq!(sg.cycle_check(node2), true);
    }

    #[test]
    fn trans_cycle() {
        let n1 = RwNode::new(1, 1);
        let id1 = node::to_usize(Box::new(n1));
        let node1 = node::from_usize(id1);

        let n2 = RwNode::new(2, 1);
        let id2 = node::to_usize(Box::new(n2));
        let node2 = node::from_usize(id2);

        let n3 = RwNode::new(3, 1);
        let id3 = node::to_usize(Box::new(n3));
        let node3 = node::from_usize(id3);

        node1.insert_outgoing(Edge::WriteWrite(id2));
        node2.insert_incoming(Edge::WriteWrite(id1));

        node3.insert_incoming(Edge::WriteWrite(id2));
        node2.insert_outgoing(Edge::WriteWrite(id3));

        node3.insert_outgoing(Edge::WriteWrite(id1));
        node1.insert_incoming(Edge::WriteWrite(id3));

        let sg = SerializationGraph::new(1);

        assert_eq!(sg.cycle_check(node1), true);
        assert_eq!(sg.cycle_check(node2), true);
        assert_eq!(sg.cycle_check(node3), true);
    }
}
