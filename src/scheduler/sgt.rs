use crate::common::error::{NonFatalError, SerializationGraphError};
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

    fn get_transaction(&self) -> *mut Node {
        SerializationGraph::NODE.with(|x| *x.borrow().as_ref().unwrap())
    }

    fn get_operations(&self) -> Vec<Operation> {
        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get_clone()
    }

    fn record(&self, op_type: OperationType, vid: ValueId, prv: u64) {
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();

        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .add(op_type, table_id, column_id, offset, prv);
    }

    fn insert_and_check(&self, this_ref: &Node, meta: &mut StatsBucket, from: Edge) -> bool {
        let this_id = self.get_transaction() as usize;

        let to = match from {
            Edge::ReadWrite(_) => Edge::ReadWrite(this_id),
            Edge::NotReadWrite(_) => Edge::NotReadWrite(this_id),
        };

        let from_id = from.extract_id();
        let from_ref = unsafe { &*(from_id as *const Node) };

        if is_self_edge(this_ref, from_ref) {
            return true;
        }

        loop {
            if this_ref.incoming_edge_exists(&from) {
                return true;
            };

            if let Edge::NotReadWrite(_) = from {
                if from_ref.is_aborted() || from_ref.is_cascading_abort() {
                    this_ref.set_cascading_abort();
                    this_ref.set_abort_through(from_id);
                    meta.set_abort_through(from_id);

                    return false;
                }
            }

            let from_rlock = from_ref.read();
            if from_ref.is_cleaned() {
                drop(from_rlock);
                return true;
            }

            if from_ref.is_checked() {
                drop(from_rlock);
                continue;
            }

            from_ref.insert_outgoing(to);
            this_ref.insert_incoming(from);
            drop(from_rlock);

            let is_cycle = self.cycle_cycle_init(this_id);

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
        visited.insert(cur);
        visit_path.insert(cur);

        let cur = unsafe { &*(cur as *const Node) };
        let g = cur.read();
        if !cur.is_cleaned() {
            let incoming = cur.get_incoming();
            for edge in incoming {
                let id = edge.extract_id();
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

    pub fn needs_abort(&self, this_ref: &Node) -> bool {
        this_ref.is_aborted() || this_ref.is_cascading_abort()
    }

    fn create_node(&self) -> usize {
        let incoming = Mutex::new(FxHashSet::default());
        let outgoing = Mutex::new(FxHashSet::default());

        let node = Box::new(Node::new(incoming, outgoing, None)); // allocate node
        let ptr: *mut Node = Box::into_raw(node); // convert to raw ptr
        let id = ptr as usize; // get id
        unsafe { (*ptr).set_id(id) }; // set id on node
        SerializationGraph::NODE.with(|x| x.borrow_mut().replace(ptr)); // store in thread local

        id
    }

    fn check_committed(&self, this_ref: &Node, database: &Database, ops: &Vec<Operation>) -> bool {
        if self.needs_abort(this_ref) {
            return false;
        }

        let read_lock = this_ref.read();
        this_ref.set_checked(true);
        drop(read_lock);

        let write_lock = this_ref.write();
        drop(write_lock);

        let read_lock = this_ref.read();
        if this_ref.has_incoming() {
            this_ref.set_checked(false);
            drop(read_lock);

            return false;
        }
        drop(read_lock);

        if self.needs_abort(this_ref) {
            return false;
        }

        let success = self.erase_graph_constraints(this_ref, database, ops);

        if success {
            self.cleanup(this_ref);
        }

        return success;
    }

    fn erase_graph_constraints(
        &self,
        this_node: &Node,
        database: &Database,
        ops: &Vec<Operation>,
    ) -> bool {
        self.commit_writes(database, true, &ops);
        this_node.set_committed();

        true
    }

    pub fn begin(&self) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1;
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new());
        let transaction_id = self.create_node();
        let guard = epoch::pin();
        SerializationGraph::EG.with(|x| x.borrow_mut().replace(guard));

        TransactionId::SerializationGraph(transaction_id)
    }

    pub fn read_value(
        &self,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let this_ref = unsafe { &*self.get_transaction() };

        if self.needs_abort(this_ref) {
            let id = this_ref.get_abort_through();
            meta.set_abort_through(id);

            return Err(SerializationGraphError::CascadingAbort.into());
        }

        let table_id = vid.get_table_id();
        let table = database.get_table(table_id);

        let offset = vid.get_offset();
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);

        let tid = meta.get_transaction_id();
        let prv = rw_table.push_front(Access::Read(tid));

        spin(prv, lsn);

        let guard = &epoch::pin();
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        for (id, access) in snapshot {
            if id < &prv {
                if let Access::Write(from_tid) = access {
                    meta.inc_wr_conflicts();
                    if let TransactionId::SerializationGraph(from_id) = from_tid {
                        let from = Edge::NotReadWrite(*from_id);

                        if !self.insert_and_check(this_ref, meta, from) {
                            cyclic = true;
                            break;
                        }
                    }
                }
            }
        }
        drop(guard);

        if cyclic {
            rw_table.erase(prv); // remove from rw table
            lsn.store(prv + 1, Ordering::Release); // update lsn

            return Err(SerializationGraphError::CycleFound.into());
        }

        let column_id = vid.get_column_id();
        let tuple = table.get_tuple(column_id, offset).get();
        let value = tuple.get_value().unwrap().get_value();

        lsn.store(prv + 1, Ordering::Release);
        self.record(OperationType::Read, vid, prv);

        Ok(value)
    }

    pub fn write_value(
        &self,
        value: &mut Data,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let table_id = vid.get_table_id();
        let offset = vid.get_offset();
        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let mut prv;
        let this_ref = unsafe { &*self.get_transaction() };
        let tid = meta.get_transaction_id();

        loop {
            if self.needs_abort(this_ref) {
                let id = this_ref.get_abort_through();
                meta.set_abort_through(id);
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            prv = rw_table.push_front(Access::Write(tid));

            spin(prv, lsn);

            let guard = &epoch::pin();
            let snapshot = rw_table.iter(guard);

            let mut wait = false;
            let mut cyclic = false;

            for (id, access) in snapshot {
                if id < &prv {
                    if let Access::Write(from_tid) = access {
                        meta.inc_ww_conflicts();
                        if let TransactionId::SerializationGraph(from_id) = from_tid {
                            let from_ref = unsafe { &*(*from_id as *const Node) };
                            if !from_ref.is_committed() {
                                let from = Edge::NotReadWrite(*from_id);
                                if !self.insert_and_check(this_ref, meta, from) {
                                    cyclic = true;
                                    break;
                                }

                                wait = true;
                                break;
                            }
                        }
                    }
                }
            }

            drop(guard);

            if cyclic {
                rw_table.erase(prv);
                lsn.store(prv + 1, Ordering::Release);

                return Err(SerializationGraphError::CycleFound.into());
            }

            if wait {
                rw_table.erase(prv);
                lsn.store(prv + 1, Ordering::Release);

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
                if let Access::Read(from_tid) = access {
                    meta.inc_rw_conflicts();
                    if let TransactionId::SerializationGraph(from_id) = from_tid {
                        let from = Edge::ReadWrite(*from_id);
                        if !self.insert_and_check(this_ref, meta, from) {
                            cyclic = true;
                            break;
                        }
                    }
                }
            }
        }

        drop(guard);

        if cyclic {
            rw_table.erase(prv);
            lsn.store(prv + 1, Ordering::Release);

            return Err(SerializationGraphError::CycleFound.into());
        }

        let column_id = vid.get_column_id();
        if let Err(_) = table.get_tuple(column_id, offset).get().set_value(value) {
            panic!(
                "{} attempting to write over uncommitted value on ({},{},{})",
                meta.get_transaction_id(),
                table_id,
                column_id,
                offset,
            ); // Assert: never write to an uncommitted value.
        }

        lsn.store(prv + 1, Ordering::Release);
        self.record(OperationType::Write, vid, prv);

        Ok(())
    }

    pub fn commit(
        &self,
        _meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let this_node = unsafe { &*self.get_transaction() };
        let ops = self.get_operations();

        let mut all_pending_transactions_committed = false;
        while !all_pending_transactions_committed {
            if this_node.is_cascading_abort() {
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            if this_node.is_aborted() {
                return Err(SerializationGraphError::CycleFound.into());
            }

            all_pending_transactions_committed = self.check_committed(this_node, database, &ops);

            if all_pending_transactions_committed {
                self.remove_accesses(database, &ops);

                SerializationGraph::EG.with(|x| {
                    let guard = x.borrow_mut().take();
                    drop(guard)
                });
            }
        }

        Ok(())
    }

    pub fn abort(&self, meta: &mut StatsBucket, database: &Database) {
        let ops = self.get_operations();
        self.commit_writes(database, false, &ops);
        let this = unsafe { &*self.get_transaction() };
        this.set_aborted();

        let incoming = this.get_incoming();
        for edge in incoming {
            if let Edge::NotReadWrite(id) = edge {
                meta.add_problem_transaction(id);
            }
        }

        self.cleanup(this);
        self.remove_accesses(database, &ops);

        SerializationGraph::EG.with(|x| {
            let guard = x.borrow_mut().take();
            drop(guard)
        });
    }

    /// Cleanup node after committed or aborted.
    fn cleanup(&self, this: &Node) {
        let this_id = self.get_transaction() as usize; // node id

        // accesses can still be found, thus, outgoing edge inserts may be attempted: (this) --> (to)
        let rlock = this.read();
        this.set_cleaned(); // cleaned acts as a barrier for edge insertion.
        drop(rlock);

        let wlock = this.write();
        drop(wlock);

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
                Edge::NotReadWrite(that_id) => {
                    let that = unsafe { &*(*that_id as *const Node) };
                    if this.is_aborted() {
                        let id = this.get_id();
                        that.set_abort_through(id);
                        that.set_cascading_abort();
                    } else {
                        let that_rlock = that.read();
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::NotReadWrite(this_id));
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

            if cnt % 16 == 0 {
                x.borrow().as_ref().unwrap().flush();
            }
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

fn spin(prv: u64, lsn: &AtomicU64) {
    let mut i = 0;
    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }
    }
}

fn is_self_edge(this_ref: &Node, from_ref: &Node) -> bool {
    let this_id = this_ref.get_id();
    let from_id = from_ref.get_id();

    this_id == from_id
}
