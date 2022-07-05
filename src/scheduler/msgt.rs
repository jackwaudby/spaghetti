use crate::common::error::{NonFatalError, SerializationGraphError};
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::common::{Edge, Node};
use crate::scheduler::StatsBucket;
use crate::scheduler::ValueId;
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::IsolationLevel;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Guard;
use rustc_hash::FxHashSet;
use scc::HashSet;
use std::cell::RefCell;
use std::collections::hash_map::RandomState;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct MixedSerializationGraph {
    txn_ctr: ThreadLocal<RefCell<usize>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    stack: ThreadLocal<RefCell<Vec<Edge>>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    relevant_cycle_check: bool,
}

impl MixedSerializationGraph {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
        static NODE: RefCell<Option<*mut Node>> = RefCell::new(None);
    }

    pub fn new(size: usize, relevant_cycle_check: bool) -> Self {
        info!("Initialise msg with {} thread(s)", size);
        info!("Relevant cycle check: {}", relevant_cycle_check);

        Self {
            txn_ctr: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            stack: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
            relevant_cycle_check,
        }
    }

    pub fn get_transaction(&self) -> *mut Node {
        MixedSerializationGraph::NODE.with(|x| *x.borrow().as_ref().unwrap())
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
    pub fn insert_and_check(&self, from: Edge) -> bool {
        let this_ref = unsafe { &*self.get_transaction() };
        let this_id = self.get_transaction() as usize;

        // prepare
        let (from_id, rw, out_edge) = match from {
            // WW edges always get inserted
            Edge::WriteWrite(from_id) => (from_id, false, Edge::WriteWrite(this_id)),

            // WR edge inserted if this node is PL2/3
            Edge::WriteRead(from_id) => {
                if let IsolationLevel::ReadUncommitted = this_ref.get_isolation_level() {
                    return true;
                }

                (from_id, false, Edge::WriteRead(this_id))
            }

            // RW edge inserted if from node is PL3
            Edge::ReadWrite(from_id) => {
                let from_ref = unsafe { &*(from_id as *const Node) };
                match from_ref.get_isolation_level() {
                    IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => {
                        return true;
                    }
                    IsolationLevel::Serializable => (from_id, true, Edge::ReadWrite(this_id)),
                }
            }
        };

        if this_id == from_id {
            return true; // check for (this) --> (this)
        }

        let isolation_level = this_ref.get_isolation_level();

        loop {
            // check if (from) --> (this) already exists
            if this_ref.incoming_edge_exists(&from) {
                let is_cycle = self.cycle_check(isolation_level); // cycle check
                return !is_cycle;
            };

            let from_ref = unsafe { &*(from_id as *const Node) };
            if (from_ref.is_aborted() || from_ref.is_cascading_abort()) && !rw {
                this_ref.set_cascading_abort();
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

            from_ref.insert_outgoing(out_edge); // (from)
            this_ref.insert_incoming(from); // (to)
            drop(from_rlock);
            let is_cycle = self.cycle_check(isolation_level); // cycle check
            return !is_cycle;
        }
    }

    pub fn cycle_check(&self, isolation: IsolationLevel) -> bool {
        // let start_id = self.get_transaction() as usize;
        // let this = unsafe { &*self.get_transaction() };

        // let mut visited = self
        //     .visited
        //     .get_or(|| RefCell::new(FxHashSet::default()))
        //     .borrow_mut();

        // let mut stack = self.stack.get_or(|| RefCell::new(Vec::new())).borrow_mut();

        // visited.clear();
        // stack.clear();

        // let this_rlock = this.read();
        // let outgoing = this.get_outgoing(); // FxHashSet<Edge<'a>>
        // let mut out = outgoing.into_iter().collect();
        // stack.append(&mut out);
        // drop(this_rlock);

        // while let Some(edge) = stack.pop() {
        //     let current = if self.relevant_cycle_check {
        //         // traverse only relevant edges
        //         match isolation {
        //             IsolationLevel::ReadUncommitted => {
        //                 if let Edge::WriteWrite(node) = edge {
        //                     node
        //                 } else {
        //                     continue;
        //                 }
        //             }
        //             IsolationLevel::ReadCommitted => match edge {
        //                 Edge::ReadWrite(_) => {
        //                     continue;
        //                 }
        //                 Edge::WriteWrite(node) => node,
        //                 Edge::WriteRead(node) => node,
        //             },
        //             IsolationLevel::Serializable => match edge {
        //                 Edge::ReadWrite(node) => node,
        //                 Edge::WriteWrite(node) => node,
        //                 Edge::WriteRead(node) => node,
        //             },
        //         }
        //     } else {
        //         // traverse any edge
        //         match edge {
        //             Edge::ReadWrite(node) => node,
        //             Edge::WriteWrite(node) => node,
        //             Edge::WriteRead(node) => node,
        //         }
        //     };

        //     if start_id == current {
        //         return true; // cycle found
        //     }

        //     if visited.contains(&current) {
        //         continue; // already visited
        //     }

        //     visited.insert(current);

        //     let current = unsafe { &*(current as *const Node) };

        //     let rlock = current.read();
        //     let val1 =
        //         !(current.is_committed() || current.is_aborted() || current.is_cascading_abort());
        //     if val1 {
        //         let outgoing = current.get_outgoing();
        //         let mut out = outgoing.into_iter().collect();
        //         stack.append(&mut out);
        //     }

        //     drop(rlock);
        // }

        false
    }

    pub fn needs_abort(&self) -> bool {
        let this = unsafe { &*self.get_transaction() };

        let aborted = this.is_aborted();
        let cascading_abort = this.is_cascading_abort();
        aborted || cascading_abort
    }

    pub fn begin(&self, isolation_level: IsolationLevel) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // increment txn ctr
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info
        let ref_id = self.create_node(isolation_level); // create node
        let guard = epoch::pin(); // pin thread
        MixedSerializationGraph::EG.with(|x| x.borrow_mut().replace(guard)); // add to guard

        TransactionId::SerializationGraph(ref_id)
    }

    pub fn create_node(&self, isolation_level: IsolationLevel) -> usize {
        let incoming = HashSet::new(100, RandomState::new());
        let outgoing = HashSet::new(100, RandomState::new());
        let iso = Some(isolation_level);
        let node = Box::new(Node::new(incoming, outgoing, iso)); // allocate node
        let ptr: *mut Node = Box::into_raw(node); // convert to raw pt
        let id = ptr as usize;
        unsafe { (*ptr).set_id(id) }; // set id on node
        MixedSerializationGraph::NODE.with(|x| x.borrow_mut().replace(ptr)); // store in thread local

        id
    }

    pub fn read_value(
        &self,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();

        let this = unsafe { &*self.get_transaction() };

        if this.is_cascading_abort() {
            self.abort(database);
            return Err(SerializationGraphError::CascadingAbort.into());
        }

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let prv = rw_table.push_front(Access::Read(meta.get_transaction_id()));
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
                            if !self.insert_and_check(Edge::WriteRead(*from_id)) {
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
            self.abort(database); // abort
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
    /// A write is executed iff there are no uncommitted writes on a record, else the operation is delayed.
    pub fn write_value(
        &self,
        value: &mut Data,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
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
                self.abort(database);
                return Err(SerializationGraphError::CascadingAbort.into());
                // check for cascading abort
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
                            if let TransactionId::SerializationGraph(from_addr) = from {
                                let from = unsafe { &*(*from_addr as *const Node) };

                                // check if write access is uncommitted
                                if !from.is_committed() {
                                    // if not in cycle then wait
                                    if !self.insert_and_check(Edge::WriteWrite(*from_addr)) {
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
                self.abort(database);
                return Err(SerializationGraphError::CycleFound.into());
            }

            // (ii) there is an uncommitted write (wait = T)
            // restart operation
            if wait {
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn

                continue;
            }

            // (iii) no w-w conflicts -> clean record (both F)
            // check for cascading abort
            if self.needs_abort() {
                rw_table.erase(prv); // remove from rw table
                self.abort(database);
                lsn.store(prv + 1, Ordering::Release); // update lsn

                return Err(SerializationGraphError::CascadingAbort.into());
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
                        if let TransactionId::SerializationGraph(from_addr) = from {
                            if !self.insert_and_check(Edge::ReadWrite(*from_addr)) {
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
            lsn.store(prv + 1, Ordering::Release); // update lsn
            self.abort(database);

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
    pub fn commit(
        &self,
        _meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let this = unsafe { &*self.get_transaction() };

        loop {
            if this.is_cascading_abort() || this.is_aborted() {
                self.abort(database);
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            let this_wlock = this.write();
            this.set_checked(true); // prevents edge insertions
            drop(this_wlock);

            // no lock taken as only outgoing edges can be added from here
            if this.has_incoming() {
                this.set_checked(false);
                let is_cycle = self.cycle_check(this.get_isolation_level());
                if is_cycle {
                    this.set_aborted();
                }
                continue;
            }

            // no incoming edges and no cycle so commit
            self.tidyup(database, true);
            this.set_committed();
            self.cleanup();

            break;
        }

        Ok(())
    }

    pub fn abort(&self, database: &Database) -> NonFatalError {
        let this = unsafe { &*self.get_transaction() };
        this.set_aborted();
        self.cleanup();
        self.tidyup(database, false);

        SerializationGraphError::CycleFound.into() // TODO: return the why
    }

    pub fn cleanup(&self) {
        // let this = unsafe { &*self.get_transaction() };
        // let this_id = self.get_transaction() as usize;

        // // accesses can still be found, thus, outgoing edge inserts may be attempted: (this) --> (to)
        // let this_wlock = this.write();
        // this.set_cleaned();
        // drop(this_wlock);

        // // remove edge sets:
        // // - no incoming edges will be added as this node is terminating: (from) --> (this)
        // // - no outgoing edges will be added from this node due to cleaned flag: (this) --> (to
        // let outgoing = this.take_outgoing();
        // let incoming = this.take_incoming();

        // let mut g = outgoing.lock();
        // let outgoing_set = g.iter();

        // for edge in outgoing_set {
        //     match edge {
        //         Edge::ReadWrite(that_id) => {
        //             let that = unsafe { &*(*that_id as *const Node) };
        //             let that_rlock = that.read();
        //             if !that.is_cleaned() {
        //                 that.remove_incoming(&Edge::ReadWrite(this_id));
        //             }
        //             drop(that_rlock);
        //         }

        //         Edge::WriteWrite(that_id) => {
        //             let that = unsafe { &*(*that_id as *const Node) };
        //             if this.is_aborted() {
        //                 that.set_cascading_abort();
        //             } else {
        //                 let that_rlock = that.read();
        //                 if !that.is_cleaned() {
        //                     that.remove_incoming(&Edge::WriteWrite(this_id));
        //                 }
        //                 drop(that_rlock);
        //             }
        //         }

        //         Edge::WriteRead(that_id) => {
        //             let that = unsafe { &*(*that_id as *const Node) };
        //             if this.is_aborted() {
        //                 that.set_cascading_abort();
        //             } else {
        //                 let that_rlock = that.read();
        //                 if !that.is_cleaned() {
        //                     that.remove_incoming(&Edge::WriteRead(this_id));
        //                 }
        //                 drop(that_rlock);
        //             }
        //         }
        //     }
        // }
        // g.clear();
        // drop(g);

        // if this.is_aborted() {
        //     incoming.lock().clear();
        // }

        // let this = self.get_transaction();
        // let cnt = *self.txn_ctr.get_or(|| RefCell::new(0)).borrow();

        // MixedSerializationGraph::EG.with(|x| unsafe {
        //     x.borrow().as_ref().unwrap().defer_unchecked(move || {
        //         let boxed_node = Box::from_raw(this);
        //         drop(boxed_node);
        //     });

        //     if cnt % 64 == 0 {
        //         x.borrow().as_ref().unwrap().flush();
        //     }

        //     let guard = x.borrow_mut().take();
        //     drop(guard)
        // });
    }

    pub fn tidyup(&self, database: &Database, commit: bool) {
        let ops = self.get_operations();

        // commit and revert state
        for op in &ops {
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

        for op in ops {
            let Operation {
                op_type,
                table_id,
                offset,
                prv,
                ..
            } = op;

            let table = database.get_table(table_id);
            let rwtable = table.get_rwtable(offset);

            match op_type {
                OperationType::Read => {
                    rwtable.erase(prv);
                }
                OperationType::Write => {
                    rwtable.erase(prv);
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
