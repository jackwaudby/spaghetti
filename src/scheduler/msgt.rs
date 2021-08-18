use crate::common::error::NonFatalError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::msgt::error::MixedSerializationGraphError;
use crate::scheduler::msgt::node::{Edge, Incoming, Node};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::IsolationLevel;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Guard;
use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::info;

pub mod node;

pub mod error;

#[derive(Debug)]
pub struct MixedSerializationGraph {
    txn_ctr: ThreadLocal<RefCell<usize>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    stack: ThreadLocal<RefCell<Vec<Edge>>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

impl MixedSerializationGraph {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
        static NODE: RefCell<Option<*mut Node>> = RefCell::new(None);
        static EARLY: RefCell<Vec<*mut Node>> = RefCell::new(Vec::new());
    }

    pub fn new(size: usize) -> Self {
        info!(
            "Initialise mixed serialization graph with {} thread(s)",
            size
        );

        Self {
            txn_ctr: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            stack: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
        }
    }

    pub fn get_transaction(&self) -> *mut Node {
        MixedSerializationGraph::NODE.with(|x| *x.borrow().as_ref().unwrap())
    }

    pub fn get_operations(&self) -> Vec<Operation> {
        let ti = self.txn_info.get().unwrap();
        ti.borrow_mut().as_mut().unwrap().get()
    }

    pub fn record(
        &self,
        op_type: OperationType,
        table_id: usize,
        column_id: usize,
        offset: usize,
        prv: u64,
    ) {
        let ti = self.txn_info.get().unwrap();
        ti.borrow_mut()
            .as_mut()
            .unwrap()
            .add(op_type, table_id, column_id, offset, prv); // record operation
    }

    pub fn insert_and_check(&self, from: Edge) -> bool {
        let this_ref = unsafe { &*self.get_transaction() };
        let this_id = self.get_transaction() as usize;

        // prepare
        let (from_id, rw, out_edge) = match from {
            // only add if (from) is PL3
            Edge::ReadWrite(from_id) => {
                let from_ref = unsafe { &*(from_id as *const Node) };
                match from_ref.get_isolation_level() {
                    IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => {
                        return true;
                    }
                    IsolationLevel::Serializable => (from_id, true, Edge::ReadWrite(this_id)),
                }
            }
            Edge::WriteWrite(from_id) => (from_id, false, Edge::WriteWrite(this_id)),
            // only insert edge if (to) is PL2/PL3
            Edge::WriteRead(from_id) => {
                if let IsolationLevel::ReadUncommitted = this_ref.get_isolation_level() {
                    return true;
                }

                (from_id, false, Edge::WriteRead(this_id))
            }
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
            let is_cycle = self.cycle_check(); // cycle check
            return !is_cycle;
        }
    }

    pub fn cycle_check(&self) -> bool {
        let start_id = self.get_transaction() as usize;
        let this = unsafe { &*self.get_transaction() };

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

            let current = unsafe { &*(current as *const Node) };

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

    pub fn needs_abort(&self) -> bool {
        let this = unsafe { &*self.get_transaction() };

        let aborted = this.is_aborted();
        let cascading_abort = this.is_cascading_abort();
        aborted || cascading_abort
    }

    pub fn begin(&self, isolation_level: IsolationLevel) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1;
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new());

        let (ref_id, thread_id, thread_ctr) = self.create_node(isolation_level);

        let guard = epoch::pin();

        MixedSerializationGraph::EG.with(|x| x.borrow_mut().replace(guard));

        TransactionId::SerializationGraph(ref_id, thread_id, thread_ctr)
    }

    pub fn create_node(&self, isolation_level: IsolationLevel) -> (usize, usize, usize) {
        let thread_id: usize = std::thread::current().name().unwrap().parse().unwrap();

        let thread_ctr = *self.txn_ctr.get().unwrap().borrow();

        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new());

        let incoming = Mutex::new(FxHashSet::default());
        let outgoing = Mutex::new(FxHashSet::default());
        let node = Box::new(Node::new(
            thread_id,
            thread_ctr,
            incoming,
            outgoing,
            isolation_level,
        ));
        let ptr: *mut Node = Box::into_raw(node);
        let id = ptr as usize;
        unsafe { (*ptr).set_id(id) };
        MixedSerializationGraph::NODE.with(|x| x.borrow_mut().replace(ptr));

        (id, thread_id, thread_ctr)
    }

    pub fn read_value(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let this = unsafe { &*self.get_transaction() };

        if this.is_cascading_abort() {
            self.abort(database);
            return Err(MixedSerializationGraphError::CascadingAbort.into());
        }

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let prv = rw_table.push_front(Access::Read(meta.clone()));
        let lsn = table.get_lsn(offset);

        unsafe { spin(prv, lsn) };

        let guard = &epoch::pin(); // pin thread

        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        for (id, access) in snapshot {
            // only interested in accesses before this one and that are write operations.
            if id < &prv {
                match access {
                    // W-R conflict
                    Access::Write(from) => {
                        if let TransactionId::SerializationGraph(from_id, _, _) = from {
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
            return Err(MixedSerializationGraphError::CycleFound.into());
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

    pub fn write_value(
        &self,
        value: &mut Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let mut prv;

        loop {
            if self.needs_abort() {
                self.abort(database);
                return Err(MixedSerializationGraphError::CascadingAbort.into());
            }

            prv = rw_table.push_front(Access::Write(meta.clone()));

            unsafe { spin(prv, lsn) };

            let guard = &epoch::pin();
            let snapshot = rw_table.iter(guard);

            let mut wait = false;
            let mut cyclic = false;

            for (id, access) in snapshot {
                if id < &prv {
                    match access {
                        // W-W conflict
                        Access::Write(from) => {
                            if let TransactionId::SerializationGraph(from_addr, _, _) = from {
                                let from = unsafe { &*(*from_addr as *const Node) };
                                if !from.is_committed() {
                                    if !self.insert_and_check(Edge::WriteWrite(*from_addr)) {
                                        cyclic = true;
                                        break;
                                    }
                                    wait = true;
                                    break;
                                } else {
                                    if table.get_tuple(column_id, offset).get().is_dirty() {
                                        wait = true; // TODO: hack
                                    }
                                }
                            }
                        }
                        Access::Read(_) => {}
                    }
                }
            }

            if cyclic {
                rw_table.erase(prv); // remove from rw table
                self.abort(database);
                lsn.store(prv + 1, Ordering::Release); // update lsn
                return Err(MixedSerializationGraphError::CycleFound.into());
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
                return Err(MixedSerializationGraphError::CascadingAbort.into());
            }

            break;
        }

        // Now, handle R-W conflicts
        let guard = &epoch::pin(); // pin thread
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        for (id, access) in snapshot {
            if self.needs_abort() {
                rw_table.erase(prv); // remove from rw table

                self.abort(database);
                lsn.store(prv + 1, Ordering::Release); // update lsn
                return Err(MixedSerializationGraphError::CascadingAbort.into());
            }

            if id < &prv {
                match access {
                    Access::Read(from) => {
                        if let TransactionId::SerializationGraph(from_addr, _, _) = from {
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
            self.abort(database);
            lsn.store(prv + 1, Ordering::Release); // update lsn
            return Err(MixedSerializationGraphError::CycleFound.into());
        }

        if let Err(e) = table.get_tuple(column_id, offset).get().set_value(value) {
            panic!("{}", e);
        }

        lsn.store(prv + 1, Ordering::Release);
        self.record(OperationType::Write, table_id, column_id, offset, prv); // record operation

        Ok(())
    }

    pub fn commit(&self, database: &Database) -> Result<(), NonFatalError> {
        let this = unsafe { &*self.get_transaction() };

        loop {
            if self.needs_abort() {
                self.abort(database);
                return Err(MixedSerializationGraphError::CascadingAbort.into());
            }

            let this_wlock = this.write();
            this.set_checked(true);
            drop(this_wlock);

            match this.has_incoming() {
                Incoming::SomeRelevant => {
                    this.set_checked(false);
                    let is_cycle = self.cycle_check();
                    if is_cycle {
                        this.set_aborted();
                    }
                    continue;
                }
                Incoming::SomeNotRelevant | Incoming::None => {
                    let node = self.get_transaction();
                    MixedSerializationGraph::EARLY.with(|x| x.borrow_mut().push(node));
                }
            }

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
        self.abort_cleanup();
        self.tidyup(database, false);

        NonFatalError::NonSerializable // TODO: return the why
    }

    pub fn cleanup(&self) {
        // let this = unsafe { &*self.get_transaction() };
        // let this_id = self.get_transaction() as usize;

        MixedSerializationGraph::EARLY.with(|x| {
            let mut lcl = x.borrow_mut();

            let mut i = 0;
            while i < lcl.len() {
                if let Incoming::None = unsafe { (*lcl[i]).has_incoming() } {
                    let ptr = lcl.remove(i);
                    let this = unsafe { &*ptr };
                    let this_id = ptr as usize;

                    let this_wlock = this.write();
                    this.set_cleaned();
                    drop(this_wlock);

                    let outgoing = this.take_outgoing(); // remove edge sets
                    let incoming = this.take_incoming();

                    let mut g = outgoing.lock(); // lock on outgoing edge set
                    let outgoing_set = g.iter(); // iterator over outgoing edge set

                    for edge in outgoing_set {
                        match edge {
                            Edge::ReadWrite(that_id) => {
                                let that = unsafe { &*(*that_id as *const Node) };
                                let that_rlock = that.read();
                                if !that.is_cleaned() {
                                    that.remove_incoming(&Edge::ReadWrite(this_id));
                                }
                                drop(that_rlock);
                            }

                            Edge::WriteWrite(that_id) => {
                                let that = unsafe { &*(*that_id as *const Node) };
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
                                let that = unsafe { &*(*that as *const Node) };
                                if this.is_aborted() {
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
                    g.clear();
                    drop(g);

                    if this.is_aborted() {
                        incoming.lock().clear();
                    }

                    let this = self.get_transaction();
                    let cnt = *self.txn_ctr.get_or(|| RefCell::new(0)).borrow();

                    MixedSerializationGraph::EG.with(|x| unsafe {
                        x.borrow().as_ref().unwrap().defer_unchecked(move || {
                            let boxed_node = Box::from_raw(this);
                            drop(boxed_node);
                        });

                        if cnt % 64 == 0 {
                            x.borrow().as_ref().unwrap().flush();
                        }

                        let guard = x.borrow_mut().take();
                        drop(guard)
                    });
                } else {
                    i += 1;
                }
            }
        });
    }

    pub fn abort_cleanup(&self) {
        let this = unsafe { &*self.get_transaction() };
        let this_id = self.get_transaction() as usize;

        let this_wlock = this.write();
        this.set_cleaned();
        drop(this_wlock);

        let outgoing = this.take_outgoing(); // remove edge sets
        let incoming = this.take_incoming();

        let mut g = outgoing.lock(); // lock on outgoing edge set
        let outgoing_set = g.iter(); // iterator over outgoing edge set

        for edge in outgoing_set {
            match edge {
                Edge::ReadWrite(that_id) => {
                    let that = unsafe { &*(*that_id as *const Node) };
                    let that_rlock = that.read();
                    if !that.is_cleaned() {
                        that.remove_incoming(&Edge::ReadWrite(this_id));
                    }
                    drop(that_rlock);
                }

                Edge::WriteWrite(that_id) => {
                    let that = unsafe { &*(*that_id as *const Node) };
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
                    let that = unsafe { &*(*that as *const Node) };
                    if this.is_aborted() {
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
        g.clear();
        drop(g);

        if this.is_aborted() {
            incoming.lock().clear();
        }

        let this = self.get_transaction();
        let cnt = *self.txn_ctr.get_or(|| RefCell::new(0)).borrow();

        MixedSerializationGraph::EG.with(|x| unsafe {
            x.borrow().as_ref().unwrap().defer_unchecked(move || {
                let boxed_node = Box::from_raw(this);
                drop(boxed_node);
            });

            if cnt % 64 == 0 {
                x.borrow().as_ref().unwrap().flush();
            }

            let guard = x.borrow_mut().take();
            drop(guard)
        });
    }

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
    }
}

unsafe fn spin(prv: u64, lsn: &AtomicU64) {
    let mut i = 0;
    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }
    }
}
