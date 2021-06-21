use crate::common::error::NonFatalError;
use crate::scheduler::sgt::error::SerializationGraphError;
use crate::scheduler::sgt::node::EdgeSet;
use crate::scheduler::sgt::node::{Edge, RwNode};
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
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
    txn_ctr: ThreadLocal<RefCell<usize>>,
    this_node: ThreadLocal<RefCell<Option<&'a RwNode>>>,
    recycled: ThreadLocal<RefCell<Vec<EdgeSet>>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    stack: ThreadLocal<RefCell<Vec<Edge>>>,
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

    pub fn get_transaction(&self) -> &'a RwNode {
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
            .add(op_type, table_id, column_id, offset, prv); // record operation
    }

    pub fn begin(&self) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // increment txn ctr
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info

        let id = self.create_node(); // create node

        debug!("start {} ", id);

        TransactionId::SerializationGraph(id)
    }

    pub fn create_node(&self) -> usize {
        let thread_id: usize = std::thread::current().name().unwrap().parse().unwrap();
        let thread_ctr = *self.txn_ctr.get().unwrap().borrow();

        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info

        let mut _recycled = self.recycled.get_or(|| RefCell::new(vec![])).borrow_mut();

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
        )); // allocated node on the heap
        let id = node::to_usize(node);
        let nref = node::from_usize(id);

        nref.set_id(id);

        self.this_node
            .get_or(|| RefCell::new(None))
            .borrow_mut()
            .replace(nref); // replace local node reference

        id
    }

    /// Cleanup node.
    /// When this method is called the outcome of the transaction is known.
    ///
    pub fn cleanup<'g>(&self, this: &'a RwNode, guard: &'g Guard) {
        // Assert: node must be committed or aborted, but not both.
        assert!(
            (this.is_committed() && !this.is_aborted())
                || (!this.is_committed() && this.is_aborted())
        );

        // Assumption: read/writes can still be found, thus, edges can still be detected.
        // A node's cleaned flag acts as a barrier for edge insertion.
        let this_wlock = this.write(); // get write lock
        this.set_cleaned(); // set as cleaned
        drop(this_wlock); // drop write lock

        let outgoing = this.take_outgoing(); // remove edge sets
        let incoming = this.take_incoming();

        let mut g = outgoing.lock(); // lock on outgoing edge set

        // --- debugging ---
        let outgoing_clone = g.clone(); // log a copy of what outgoing edge set was
        unsafe {
            this.outgoing_clone
                .get()
                .as_mut()
                .unwrap()
                .replace(outgoing_clone)
        };
        // -------------------

        let this_id = node::ref_to_usize(this); // node id
        let outgoing_set = g.iter(); // iterator over outgoing edge set

        for edge in outgoing_set {
            match edge {
                // (this) -[rw]-> (that)
                Edge::ReadWrite(that_id) => {
                    // Get read lock on outgoing node - prevents node from committing.
                    let that = node::from_usize(*that_id);
                    let that_rlock = that.read();

                    // Assert: outgoing node may be aborted or active, but will not be committed.
                    assert!(!that.is_committed());

                    // If active then the node will not be cleaned and edge must be removed.
                    // Else, the node is cleaned and must have aborted.
                    if !that.is_cleaned() {
                        that.remove_incoming(&Edge::ReadWrite(this_id)); // remove incoming from this node
                        unsafe { this.removed.get().as_mut().unwrap().push(edge.clone()) };
                    } else {
                        assert!(that.is_aborted());
                        unsafe {
                            this.outgoing_cleaned
                                .get()
                                .as_mut()
                                .unwrap()
                                .push(edge.clone())
                        };
                    }

                    // Release read lock.
                    drop(that_rlock);
                }
                // (this) -[ww]-> (that)
                Edge::WriteWrite(that_id) => {
                    // Get read lock on outgoing node - prevents node from committing.
                    let that = node::from_usize(*that_id);

                    // Assert: outgoing node may be aborted or active, but will not be committed.
                    assert!(!that.is_committed());

                    // If this node aborted then the outgoing node must also abort.
                    // Else, this node is committed.
                    if this.is_aborted() {
                        unsafe { this.skipped.get().as_mut().unwrap().push(edge.clone()) };
                        that.set_cascading_abort();
                    } else {
                        let that_rlock = that.read();
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::WriteWrite(this_id));
                            unsafe { this.removed.get().as_mut().unwrap().push(edge.clone()) };
                        } else {
                            unsafe {
                                this.outgoing_cleaned
                                    .get()
                                    .as_mut()
                                    .unwrap()
                                    .push(edge.clone())
                            };
                        }
                        drop(that_rlock);
                    }
                }
                Edge::WriteRead(that) => {
                    let that = node::from_usize(*that);
                    if this.is_aborted() {
                        unsafe { this.skipped.get().as_mut().unwrap().push(edge.clone()) };
                        that.set_cascading_abort(); // if this node is aborted and not rw; cascade abort on that node
                    } else {
                        let that_rlock = that.read(); // get read lock on outgoing edge
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::WriteRead(this_id));
                            unsafe { this.removed.get().as_mut().unwrap().push(edge.clone()) };
                        } else {
                            unsafe {
                                this.outgoing_cleaned
                                    .get()
                                    .as_mut()
                                    .unwrap()
                                    .push(edge.clone())
                            };
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

        unsafe {
            guard.defer_unchecked(move || {
                drop(boxed_node);
            });
        }
    }

    /// Insert an incoming edge into (this) node from (from) node, followed by a cycle check.
    pub fn insert_and_check(
        &self,
        this_ref: &'a RwNode,
        from: Edge,
        table_id: usize,
        column_id: usize,
        offset: usize,
    ) -> bool {
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

                        assert!(!from_ref.is_checked());
                        assert!(!from_ref.is_cleaned());
                        assert!(!from_ref.is_complete());

                        let this_rlock = this_ref.read(); // get shared lock on (this)
                        debug!("inserted {}-[rw]->{}", from_id, this_id);
                        this_ref.insert_incoming(Edge::ReadWrite(from_id));
                        unsafe {
                            this_ref.inserted.get().as_mut().unwrap().push(format!(
                                "{}-({},{},{})",
                                Edge::ReadWrite(from_id),
                                table_id,
                                column_id,
                                offset,
                            ))
                        };

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
                        debug!("inserted {}-[o]->{}", from_id, this_id);
                        unsafe {
                            this_ref.inserted.get().as_mut().unwrap().push(format!(
                                "{}-({},{},{})",
                                Edge::WriteWrite(from_id),
                                table_id,
                                column_id,
                                offset,
                            ))
                        };

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
                        this_ref.insert_incoming(Edge::WriteRead(from_id));
                        from_ref.insert_outgoing(Edge::WriteRead(this_id));
                        debug!("inserted {}-[o]->{}", from_id, this_id);
                        unsafe {
                            this_ref.inserted.get().as_mut().unwrap().push(format!(
                                "{}-({},{},{})",
                                Edge::WriteRead(from_id),
                                table_id,
                                column_id,
                                offset
                            ))
                        };

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

            // let current_addr = current as *const _ as usize;
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

    pub fn read_value<'g>(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> Result<Data, NonFatalError> {
        let this = self.get_transaction();

        if this.is_cascading_abort() {
            self.abort(database, guard);
            return Err(SerializationGraphError::CascadingAbort.into());
        }

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let prv = rw_table.push_front(Access::Read(meta.clone()), guard);
        let lsn = table.get_lsn(offset);

        // Safety: ensures exculsive access to the record.
        unsafe { spin(prv, lsn) }; // busy wait

        // On acquiring the 'lock' on the record can be clean or dirty.
        // Dirty is ok here as we allow reads uncommitted data; SGT protects against serializability violations.
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        for (id, access) in snapshot {
            // only interested in accesses before this one and that are write operations.
            if id < &prv {
                match access {
                    // W-R conflict
                    Access::Write(from) => {
                        if let TransactionId::SerializationGraph(from_id) = from {
                            if !self.insert_and_check(
                                this,
                                Edge::WriteRead(*from_id),
                                table_id,
                                column_id,
                                offset,
                            ) {
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

        self.record(OperationType::Read, table_id, column_id, offset, prv); // record operation

        Ok(vals)
    }

    /// Write operation.
    ///
    /// A write can executed iff there are no uncommitted writes on a record, else the operation is delayed.
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
        let this = self.get_transaction();
        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let mut prv;
        let mut attempts = 0;
        let mut prvs = Vec::new();
        let mut delays = Vec::new();
        let mut cs = Vec::new();
        let mut seen = Vec::new();

        loop {
            // check for cascading abort
            if self.needs_abort(this) {
                self.abort(database, guard);
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            prv = rw_table.push_front(Access::Write(meta.clone()), guard); // get ticket

            prvs.push(prv);

            // Safety: ensures exculsive access to the record.
            unsafe { spin(prv, lsn) }; // busy wait

            // On acquiring the 'lock' on the record it is possible another transaction has an uncommitted write on this record.
            // In this case the operation is restarted after a cycle check.
            let snapshot = rw_table.iter(guard);

            let mut wait = false; // flag indicating if there is an uncommitted write
            let mut cyclic = false; // flag indicating if a cycle has been found

            let mut conflicts = Vec::new();
            let mut saw = Vec::new();
            saw.push(format!("attempt: {}", attempts));
            for (id, access) in snapshot {
                saw.push(format!("{}-{}", id, access));
                // only interested in accesses before this one and that are write operations.
                if id < &prv {
                    match access {
                        // W-W conflict
                        Access::Write(from) => {
                            conflicts.push(format!("{}-{}", attempts, from));
                            if let TransactionId::SerializationGraph(from_addr) = from {
                                let from = node::from_usize(*from_addr); // convert to ptr

                                // check if write access is uncommitted
                                // if !from.is_committed() {
                                if !from.is_complete() {
                                    // if not in cycle then wait
                                    if !self.insert_and_check(
                                        this,
                                        Edge::WriteWrite(*from_addr),
                                        table_id,
                                        column_id,
                                        offset,
                                    ) {
                                        cyclic = true;

                                        cs.push(conflicts);

                                        break; // no reason to check other accesses
                                    }

                                    wait = true; // retry operation
                                    cs.push(conflicts);

                                    break;
                                } else {
                                }
                            }
                        }
                        Access::Read(_) => {}
                    }
                }
            }
            seen.push(saw);

            // (i) transaction is in a cycle (cycle = T)
            // abort transaction
            if cyclic {
                rw_table.erase(prv, guard); // remove from rw table
                self.abort(database, guard);
                lsn.store(prv + 1, Ordering::Release); // update lsn
                return Err(SerializationGraphError::CycleFound.into());
            }

            // (ii) there is an uncommitted write (wait = T)
            // restart operation
            if wait {
                rw_table.erase(prv, guard); // remove from rw table

                lsn.store(prv + 1, Ordering::Release); // update lsn
                attempts += 1;
                delays.push(wait);
                continue;
            }

            // (iii) no w-w conflicts -> clean record (both F)
            // check for cascading abort
            if self.needs_abort(this) {
                rw_table.erase(prv, guard); // remove from rw table
                self.abort(database, guard);
                lsn.store(prv + 1, Ordering::Release); // update lsn
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            attempts += 1;
            delays.push(wait);
            break;
        }

        let tuple = table.get_tuple(column_id, offset); // handle to tuple
        let (dirty, tstate) = tuple.get().is_dirty();
        // Assert: there must be not an uncommitted write, the record must be clean.
        // assert_eq!(
        //     dirty, false,
        //     "\ntuple: ({},{},{}) \nstate: {:?} \nwriting node :{} \nattempts made: {} \nrwtable: {} \nprvs: {:?} \ndelays: {:?} \nconflicts: {:?} \ntuple_state: {}",
        //     table_id,column_id,offset,  this, attempts, rw_table, prvs, delays, cs, tuple
        // );

        assert_eq!(
            dirty, false,
            "\ntuple: ({},{},{}) \nstate: {:?} \nwriting node :{} \nattempts made: {} \nconflicts: {:?} \nrwtable: {} \nseen: {:?}",
            table_id, column_id, offset, tstate, this, attempts, cs, rw_table, seen
        );

        // assert_eq!(
        //     dirty, false,
        //     "\ntuple: ({},{},{}) \nnode :{} \nattempts: {} \nrwtable: {:?}",
        //     table_id, column_id, offset, this, attempts, rw_table
        // );

        // Now, handle R-W conflicts
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        // only interested in accesses before this one and that are read operations.
        for (id, access) in snapshot {
            // check for cascading abort
            if self.needs_abort(this) {
                rw_table.erase(prv, guard); // remove from rw table

                self.abort(database, guard);
                lsn.store(prv + 1, Ordering::Release); // update lsn
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            if id < &prv {
                match access {
                    Access::Read(from) => {
                        if let TransactionId::SerializationGraph(from_addr) = from {
                            if !self.insert_and_check(
                                this,
                                Edge::ReadWrite(*from_addr),
                                table_id,
                                column_id,
                                offset,
                            ) {
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
            rw_table.erase(prv, guard); // remove from rw table
            self.abort(database, guard);
            lsn.store(prv + 1, Ordering::Release); // update lsn
            return Err(SerializationGraphError::CycleFound.into());
        }

        if let Err(e) = table
            .get_tuple(column_id, offset)
            .get()
            .set_value(value, prv, meta.clone())
        {
            panic!("{}", e); // ASSERT: never write to an uncommitted value.
        }

        // update lsn, giving next operation access.
        lsn.store(prv + 1, Ordering::Release);

        self.record(OperationType::Write, table_id, column_id, offset, prv); // record operation

        Ok(())
    }

    /// Commit operation.
    pub fn commit<'g>(&self, database: &Database, guard: &'g Guard) -> Result<(), NonFatalError> {
        let this = self.get_transaction();

        loop {
            if self.needs_abort(&this) {
                self.abort(database, guard);
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
                    this.set_aborted(); // cycle so abort (this)
                }
                continue;
            }

            let is_cycle = self.cycle_check(&this); // final cycle check

            if is_cycle {
                this.set_aborted(); // abort
                continue;
            }

            // no incoming edges and no cycle so commit
            self.tidyup(database, guard, true);
            this.set_committed();
            self.cleanup(this, guard);

            break;
        }

        this.set_complete();
        Ok(())
    }

    /// Abort operation.
    pub fn abort<'g>(&self, database: &Database, guard: &'g Guard) -> NonFatalError {
        let this = self.get_transaction();
        this.set_aborted();
        self.cleanup(this, guard);
        self.tidyup(database, guard, false);
        this.set_complete();

        NonFatalError::NonSerializable // TODO: return the why
    }

    /// Tidyup rwtables and tuples
    pub fn tidyup<'g>(&self, database: &Database, guard: &'g Guard, commit: bool) {
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
                    rwtable.erase(prv, guard);
                }
                OperationType::Write => {
                    if commit {
                        tuple.get().commit();
                    } else {
                        tuple.get().revert();
                    }
                    let (dirty, _) = tuple.get().is_dirty();
                    assert!(!dirty);
                    rwtable.erase(prv, guard);
                }
            }
        }
    }
}

// Busy wait until prv matches lsn.
unsafe fn spin(prv: u64, lsn: &AtomicU64) {
    let current = lsn.load(Ordering::Relaxed);

    // ASSERT: lsn values should be monotonically increasing.
    assert!(current <= prv);

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
        let id1 = node::to_usize(Box::new(n1));
        let node1 = node::from_usize(id1);

        let n2 = RwNode::new();
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
        let n1 = RwNode::new();
        let id1 = node::to_usize(Box::new(n1));
        let node1 = node::from_usize(id1);

        let n2 = RwNode::new();
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
        let n1 = RwNode::new();
        let id1 = node::to_usize(Box::new(n1));
        let node1 = node::from_usize(id1);

        let n2 = RwNode::new();
        let id2 = node::to_usize(Box::new(n2));
        let node2 = node::from_usize(id2);

        let n3 = RwNode::new();
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
