use crate::common::error::{NonFatalError, SerializationGraphError};
use crate::common::statistics::local::LocalStatistics;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::{
    common::{Edge, Node},
    StatsBucket, ValueId,
};
use crate::storage::{
    access::{Access, TransactionId},
    datatype::Data,
    Database,
};

use crossbeam_epoch::{self as epoch, Guard};
use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use std::cell::{RefCell, RefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct SerializationGraph {
    txn_ctr: ThreadLocal<RefCell<usize>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    visit_path: ThreadLocal<RefCell<FxHashSet<usize>>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

impl SerializationGraph {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
        static NODE: RefCell<Option<*mut Node>> = RefCell::new(None);
    }

    pub fn new(size: usize) -> Self {
        info!("Initialise serialization graph with {} thread(s)", size);

        Self {
            txn_ctr: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            visit_path: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
        }
    }

    pub fn get_transaction(&self) -> *mut Node {
        SerializationGraph::NODE.with(|x| *x.borrow().as_ref().unwrap())
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
            .add(op_type, table_id, column_id, offset, prv);
    }

    /// Insert an edge: (from) --> (this)
    pub fn insert_and_check(&self, from: Edge, stats: &mut LocalStatistics) -> bool {
        let this_ref = unsafe { &*self.get_transaction() };
        let this_id = self.get_transaction() as usize;

        // prepare
        let (from_id, rw, out_edge) = match from {
            Edge::ReadWrite(from_id) => (from_id, true, Edge::ReadWrite(this_id)),
            Edge::WriteRead(from_id) => (from_id, false, Edge::WriteRead(this_id)),
            Edge::WriteWrite(from_id) => (from_id, false, Edge::WriteWrite(this_id)),
        };

        if this_id == from_id {
            return true; // check for (this) --> (this)
        }

        loop {
            if this_ref.incoming_edge_exists(&from) {
                return true; // check if (from) --> (this) already exists
            };

            let from_ref = unsafe { &*(from_id as *const Node) };

            if (from_ref.is_aborted() || from_ref.is_cascading_abort()) && !rw {
                this_ref.set_cascading_abort();
                // let fid = from_ref.get_full_id();
                // this_ref.set_abort_through(fid);
                return false; // cascadingly abort (this)
            }

            let from_rlock = from_ref.read();
            if from_ref.is_cleaned() {
                drop(from_rlock);
                return true; // if from is cleaned then it has terminated do not insert edge
            }

            if from_ref.is_checked() {
                drop(from_rlock);
                continue; // if (from) checked in process of terminating so try again
            }

            stats.inc_edges_inserted();

            from_ref.insert_outgoing(out_edge); // (from)
            this_ref.insert_incoming(from); // (to)
            drop(from_rlock);
            let is_cycle = self.cycle_cycle_init(this_id);

            // cycle_check(); // cycle check

            return !is_cycle;
        }
    }

    fn get_visited(&self) -> RefMut<FxHashSet<usize>> {
        self.visited
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut()
    }

    fn get_visit_path(&self) -> RefMut<FxHashSet<usize>> {
        self.visit_path
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut()
    }

    fn check_cycle_naive(
        &self,
        cur: usize,
        visited: &mut RefMut<FxHashSet<usize>>,
        visit_path: &mut RefMut<FxHashSet<usize>>,
    ) -> bool {
        // let mut visited = self.get_visited();
        // let mut visit_path = self.get_visit_path();

        visited.insert(cur);
        visit_path.insert(cur);

        let cur = unsafe { &*(cur as *const Node) };
        let g = cur.read();
        if !cur.is_cleaned() {
            let incoming = cur.get_incoming();
            for edge in incoming {
                let id = edge.extract_id() as usize;
                if visit_path.contains(&id) {
                    drop(g);
                    return true;
                } else {
                    if self.check_cycle_naive(id, visited, visit_path) {
                        drop(g);
                        return true;
                    }
                }
            }
        }

        drop(g);
        let cur = cur.get_id() as usize;
        visit_path.remove(&cur);

        return false;
    }

    fn cycle_cycle_init(&self, this_node: usize) -> bool {
        let mut visited = self.get_visited();
        let mut visit_path = self.get_visit_path();

        visited.clear();
        visit_path.clear();

        let mut check = false;
        if !visited.contains(&this_node) {
            check = self.check_cycle_naive(this_node, &mut visited, &mut visit_path);
        }
        return check;
    }

    // pub fn cycle_check(&self) -> bool {
    //     let start_id = self.get_transaction() as usize;

    //     let this = unsafe { &*self.get_transaction() };

    //     let mut visited = self
    //         .visited
    //         .get_or(|| RefCell::new(FxHashSet::default()))
    //         .borrow_mut();

    //     let mut stack = self.stack.get_or(|| RefCell::new(Vec::new())).borrow_mut();

    //     visited.clear();
    //     stack.clear();

    //     let this_rlock = this.read();
    //     let outgoing = this.get_incoming(); // FxHashSet<Edge<'a>>
    //     let mut out = outgoing.into_iter().collect();

    //     stack.append(&mut out);

    //     drop(this_rlock);

    //     while let Some(edge) = stack.pop() {
    //         let current = match edge {
    //             Edge::ReadWrite(node) => node,
    //             Edge::WriteWrite(node) => node,
    //             Edge::WriteRead(node) => node,
    //         };

    //         if start_id == current {
    //             return true; // cycle found
    //         }

    //         if visited.contains(&current) {
    //             continue; // already visited
    //         }

    //         visited.insert(current);

    //         let current = unsafe { &*(current as *const Node) };

    //         let rlock = current.read();
    //         let val1 =
    //             !(current.is_committed() || current.is_aborted() || current.is_cascading_abort());
    //         if val1 {
    //             let outgoing = current.get_incoming();
    //             let mut out = outgoing.into_iter().collect();
    //             stack.append(&mut out);
    //         }

    //         drop(rlock);
    //     }

    //     false
    // }

    /// Check if a transaction needs to abort.
    pub fn needs_abort(&self) -> bool {
        let this = unsafe { &*self.get_transaction() };

        let aborted = this.is_aborted();
        let cascading_abort = this.is_cascading_abort();
        aborted || cascading_abort
    }

    pub fn begin(&self) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // increment txn ctr
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info

        let ref_id = self.create_node(); // create node

        let guard = epoch::pin(); // pin thread

        SerializationGraph::EG.with(|x| x.borrow_mut().replace(guard));

        TransactionId::SerializationGraph(ref_id)
    }

    pub fn create_node(&self) -> usize {
        let incoming = Mutex::new(FxHashSet::default());
        let outgoing = Mutex::new(FxHashSet::default());

        let node = Box::new(Node::new(incoming, outgoing, None)); // allocate node
        let ptr: *mut Node = Box::into_raw(node); // convert to raw ptr
        let id = ptr as usize; // get id
        unsafe { (*ptr).set_id(id) }; // set id on node

        SerializationGraph::NODE.with(|x| x.borrow_mut().replace(ptr)); // store in thread local

        id
    }

    pub fn read_value(
        &self,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
        stats: &mut LocalStatistics,
    ) -> Result<Data, NonFatalError> {
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();

        let this = unsafe { &*self.get_transaction() };

        // if this.is_cascading_abort() {
        //     self.abort(meta, database);
        //     return Err(SerializationGraphError::ReadOpCascasde.into());
        // }

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let prv = rw_table.push_front(Access::Read(meta.get_transaction_id()));
        let lsn = table.get_lsn(offset);

        // Safety: ensures exculsive access to the record.
        unsafe { spin(prv, lsn) }; // busy wait

        // On acquiring the 'lock' on the record can be clean or dirty.
        // Dirty is ok here as we allow reads uncommitted data; SGT protects against serializability violations.
        // let guard = &epoch::pin(); // pin thread
        // let snapshot = rw_table.iter(guard);

        // let mut cyclic = false;

        // for (id, access) in snapshot {
        //     // only interested in accesses before this one and that are write operations.
        //     if id < &prv {
        //         match access {
        //             // W-R conflict
        //             Access::Write(from) => {
        //                 if let TransactionId::SerializationGraph(from_id) = from {
        //                     stats.inc_conflict_detected();

        //                     if !self.insert_and_check(Edge::WriteRead(*from_id), stats) {
        //                         cyclic = true;
        //                         break;
        //                     }
        //                 }
        //             }
        //             Access::Read(_) => {}
        //         }
        //     }
        // }

        // if cyclic {
        //     rw_table.erase(prv); // remove from rw table
        //     lsn.store(prv + 1, Ordering::Release); // update lsn
        //     self.abort(meta, database); // abort
        //     return Err(SerializationGraphError::ReadOpCycleFound.into());
        // }

        let tuple = table.get_tuple(column_id, offset).get();
        let value = tuple.get_value().unwrap().get_value();

        lsn.store(prv + 1, Ordering::Release); // update lsn

        self.record(OperationType::Read, table_id, column_id, offset, prv); // record operation

        Ok(value)
    }

    /// Write operation.
    ///
    /// A write is executed iff there are no uncommitted writes on a record, else the operation is delayed.
    pub fn write_value(
        &self,
        value: &mut Data,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
        stats: &mut LocalStatistics,
    ) -> Result<(), NonFatalError> {
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();
        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let mut prv;

        loop {
            if self.needs_abort() {
                self.abort(meta, database);
                return Err(SerializationGraphError::WriteOpCascasde.into()); // check for cascading abort
            }

            prv = rw_table.push_front(Access::Write(meta.get_transaction_id())); // get ticket

            unsafe { spin(prv, lsn) }; // Safety: ensures exculsive access to the record

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
                            stats.inc_conflict_detected();

                            if let TransactionId::SerializationGraph(from_addr) = from {
                                let from = unsafe { &*(*from_addr as *const Node) };

                                // check if write access is uncommitted
                                if !from.is_committed() {
                                    // if not in cycle then wait
                                    // println!("added 1 edge: {} --> X", from.get_full_id());

                                    if !self.insert_and_check(Edge::WriteWrite(*from_addr), stats) {
                                        cyclic = true;
                                        break; // no reason to check other accesses
                                    }

                                    wait = true; // retry operation
                                    break;
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
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn
                self.abort(meta, database);
                return Err(SerializationGraphError::WriteOpCycleFound.into());
            }

            // (ii) there is an uncommitted write (wait = T)
            // restart operation
            if wait {
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn

                continue;
            }

            break;
        }

        // Now handle R-W conflicts
        let guard = &epoch::pin(); // pin thread
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        for (id, access) in snapshot {
            if id < &prv {
                match access {
                    Access::Read(from) => {
                        stats.inc_conflict_detected();

                        if let TransactionId::SerializationGraph(from_addr) = from {
                            let from = unsafe { &*(*from_addr as *const Node) };
                            if !from.is_committed() {
                                if !self.insert_and_check(Edge::ReadWrite(*from_addr), stats) {
                                    cyclic = true;
                                    break;
                                }
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
            lsn.store(prv + 1, Ordering::Release); // update lsn
            self.abort(meta, database);

            return Err(SerializationGraphError::CycleFound.into());
        }

        if let Err(_) = table.get_tuple(column_id, offset).get().set_value(value) {
            panic!(
                "{} attempting to write over uncommitted value on ({},{},{})",
                meta.get_transaction_id(),
                table_id,
                column_id,
                offset,
            ); // Assert: never write to an uncommitted value.
        }

        lsn.store(prv + 1, Ordering::Release); // update lsn, giving next operation access.
        self.record(OperationType::Write, table_id, column_id, offset, prv); // record operation

        Ok(())
    }

    /// Commit operation.
    pub fn commit(&self, meta: &mut StatsBucket, database: &Database) -> Result<(), NonFatalError> {
        let this = unsafe { &*self.get_transaction() };

        loop {
            if this.is_cascading_abort() {
                self.abort(meta, database);
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            if this.is_aborted() {
                self.abort(meta, database);
                return Err(SerializationGraphError::CycleFound.into());
            }

            let this_wlock = this.write();
            this.set_checked(true); // prevents edge insertions
            drop(this_wlock);

            // no lock taken as only outgoing edges can be added from here
            if this.has_incoming() {
                this.set_checked(false); // if incoming then flip back to unchecked
                let is_cycle = self.cycle_cycle_init(this.get_id()); // cycle check
                if is_cycle {
                    this.set_aborted(); // cycle so abort (this)
                }
                continue;
            }

            // no incoming edges and no cycle so commit
            let ops = self.get_operations();

            self.commit_writes(database, true, &ops);
            this.set_committed();
            self.cleanup(this);
            self.remove_accesses(database, &ops);

            SerializationGraph::EG.with(|x| {
                let guard = x.borrow_mut().take();
                drop(guard)
            });
            break;
        }

        Ok(())
    }

    /// Abort operation.
    pub fn abort(&self, _meta: &mut StatsBucket, database: &Database) {
        let ops = self.get_operations();

        self.commit_writes(database, false, &ops);

        let this = unsafe { &*self.get_transaction() };
        this.set_aborted();

        self.cleanup(this);

        self.remove_accesses(database, &ops);

        SerializationGraph::EG.with(|x| {
            let guard = x.borrow_mut().take();
            drop(guard)
        });
    }

    /// Cleanup node after committed or aborted.
    pub fn cleanup(&self, this: &Node) {
        let this_id = self.get_transaction() as usize; // node id

        // accesses can still be found, thus, outgoing edge inserts may be attempted: (this) --> (to)
        let this_wlock = this.write();
        this.set_cleaned(); // cleaned acts as a barrier for edge insertion.
        drop(this_wlock);

        // remove edge sets:
        // - no incoming edges will be added as this node is terminating: (from) --> (this)
        // - no outgoing edges will be added from this node due to cleaned flag: (this) --> (to)
        let outgoing = this.take_outgoing();
        let incoming = this.take_incoming();

        let mut g = outgoing.lock(); // lock on outgoing edge set
        let outgoing_set = g.iter(); // iterator over outgoing edge set

        for edge in outgoing_set {
            match edge {
                // (this) -[rw]-> (to)
                Edge::ReadWrite(that_id) => {
                    let that = unsafe { &*(*that_id as *const Node) };
                    let that_rlock = that.read(); // prevent (to) from committing
                    if !that.is_cleaned() {
                        that.remove_incoming(&Edge::ReadWrite(this_id));
                        // if (to) is not cleaned remove incoming edge
                    }
                    drop(that_rlock);
                }

                // (this) -[ww]-> (to)
                Edge::WriteWrite(that_id) => {
                    let that = unsafe { &*(*that_id as *const Node) };
                    if this.is_aborted() {
                        // let fid = this.get_full_id();
                        // that.set_abort_through(fid);
                        that.set_cascading_abort();
                    } else {
                        let that_rlock = that.read();
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::WriteWrite(this_id));
                        }
                        drop(that_rlock);
                    }
                }
                // (this) -[wr]-> (to)
                Edge::WriteRead(that) => {
                    let that = unsafe { &*(*that as *const Node) };
                    if this.is_aborted() {
                        // let fid = this.get_full_id();
                        // that.set_abort_through(fid);
                        that.set_cascading_abort();
                    } else {
                        let that_rlock = that.read();
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::WriteRead(this_id));
                        }
                        drop(that_rlock);
                    }
                }
            }
        }
        g.clear(); // clear (this) outgoing
        drop(g);

        if this.is_aborted() {
            incoming.lock().clear();
        }

        let this = self.get_transaction();
        let cnt = *self.txn_ctr.get_or(|| RefCell::new(0)).borrow();

        SerializationGraph::EG.with(|x| unsafe {
            x.borrow().as_ref().unwrap().defer_unchecked(move || {
                let boxed_node = Box::from_raw(this); // garbage collect
                drop(boxed_node);
            });

            if cnt % 64 == 0 {
                x.borrow().as_ref().unwrap().flush();
            }

            // let guard = x.borrow_mut().take();
            // drop(guard)
        });
    }

    fn commit_writes(&self, database: &Database, commit: bool, ops: &Vec<Operation>) {
        for op in ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                ..
            } = op;

            let table = database.get_table(*table_id);
            let tuple = table.get_tuple(*column_id, *offset);

            if let OperationType::Write = op_type {
                if commit {
                    tuple.get().commit();
                } else {
                    tuple.get().revert();
                }
            }
        }
    }

    fn remove_accesses(&self, database: &Database, ops: &Vec<Operation>) {
        // remove accesses
        for op in ops {
            let Operation {
                op_type,
                table_id,
                offset,
                prv,
                ..
            } = op;

            let table = database.get_table(*table_id);
            let rwtable = table.get_rwtable(*offset);

            match op_type {
                OperationType::Read => {
                    rwtable.erase(*prv);
                }
                OperationType::Write => {
                    rwtable.erase(*prv);
                }
            }
        }
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
