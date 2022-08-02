use crate::common::{
    error::{NonFatalError, SerializationGraphError},
    isolation_level::IsolationLevel,
    transaction_information::{Operation, OperationType, TransactionInformation},
};
use crate::scheduler::{
    common::{MsgEdge as Edge, MsgNode as Node},
    StatsBucket, ValueId,
};
use crate::storage::{
    access::{Access, TransactionId},
    datatype::Data,
    Database,
};

use core::panic;
use crossbeam_epoch::{self as epoch, Guard};
use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use std::borrow::BorrowMut;
use std::cell::{RefCell, RefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use thread_local::ThreadLocal;
use tracing::info;

static ATTEMPTS: u64 = 10000000;

#[derive(Debug, Clone)]
pub enum Cycle {
    G0,
    G1c,
    G2,
}

#[derive(Debug)]
enum CycleCheckingStrategy {
    // starts from this_node and looks for any cycle
    Reduced,
    // starts from this_node and only finds cycles involving this_node
    Restricted,
    // starts from this_node, only finds cycles involving this_node,
    // and when inserting a RW edge if finds a G2 anomaly, don't abort PL-1/2, abort the PL-3
    Relevant,
}

#[derive(Debug)]
pub struct MixedSerializationGraph {
    txn_ctr: ThreadLocal<RefCell<usize>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    visit_path: ThreadLocal<RefCell<Vec<usize>>>,
    edge_path: ThreadLocal<RefCell<Vec<Edge>>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    cycle_checking_strategy: CycleCheckingStrategy,
}

impl MixedSerializationGraph {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
        static NODE: RefCell<Option<*mut Node>> = RefCell::new(None);
    }

    pub fn new(size: usize, cycle_checking_strategy: &str) -> Self {
        info!("Initialise msgt with {} cores(s)", size);
        info!("Cycle checking strategy: {}", cycle_checking_strategy);

        let cycle_checking_strategy = match cycle_checking_strategy {
            "reduced" => CycleCheckingStrategy::Reduced,
            "restricted" => CycleCheckingStrategy::Restricted,
            "relevant" => CycleCheckingStrategy::Relevant,
            _ => panic!(
                "unrecognized cycle checking strategy: {}",
                cycle_checking_strategy
            ),
        };

        Self {
            txn_ctr: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            visit_path: ThreadLocal::new(),
            edge_path: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
            cycle_checking_strategy,
        }
    }

    fn get_transaction(&self) -> *mut Node {
        MixedSerializationGraph::NODE.with(|x| *x.borrow().as_ref().unwrap())
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

        let txn_info = self.txn_info.get().unwrap();
        txn_info
            .borrow_mut()
            .as_mut()
            .unwrap()
            .add(op_type, table_id, column_id, offset, prv);
    }

    fn insert_and_check(&self, this_ref: &Node, meta: &mut StatsBucket, from: Edge) -> bool {
        // "from" gets added into this.incoming
        //  "to"  gets added into from.outgoing
        let to = match is_relevant_or_obligatory(&from, this_ref) {
            Some(to) => to,
            None => return true,
        };

        let from_id = from.extract_id();
        let from_ref = unsafe { &*(from_id as *const Node) };

        if is_self_edge(this_ref, from_ref) {
            return true;
        }

        let mut attempts = 0;
        loop {
            if attempts > ATTEMPTS {
                panic!(
                    "{} ({}) stuck inserting {:?}. Incoming {:?}",
                    this_ref.get_id().unwrap(),
                    this_ref.get_isolation_level(),
                    from,
                    this_ref.get_incoming(),
                );
            }

            if this_ref.incoming_edge_exists(&from) {
                return true;
            };

            if let Edge::WriteWrite(_) | Edge::WriteRead(_) = from {
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
                attempts += 1;

                continue;
            }

            match from {
                Edge::WriteWrite(_) => meta.inc_ww_conflicts(),
                Edge::WriteRead(_) => meta.inc_wr_conflicts(),
                Edge::ReadWrite(_) => meta.inc_rw_conflicts(),
            }

            from_ref.insert_outgoing(to);
            this_ref.insert_incoming(from.clone());
            drop(from_rlock);

            match self.cycle_checking_strategy {
                CycleCheckingStrategy::Reduced => {
                    let is_cycle = self.cycle_check_reduced_init(this_ref);

                    match is_cycle {
                        Some(edge_path) => {
                            let cycle_type = classify_cycle(&edge_path);
                            meta.set_cycle_type(cycle_type);

                            let path_len = edge_path.len();
                            meta.set_path_len(path_len);

                            return false;
                        }
                        None => return true,
                    }
                }

                CycleCheckingStrategy::Restricted => {
                    let (is_cycle, _, edge_path) = self.cycle_check_restricted_init(this_ref);

                    if is_cycle {
                        let cycle_type = classify_cycle(&edge_path);
                        meta.set_cycle_type(cycle_type);

                        let path_len = edge_path.len();
                        meta.set_path_len(path_len);
                    }

                    return !is_cycle;
                }

                CycleCheckingStrategy::Relevant => {
                    if let Edge::ReadWrite(_) = from {
                        let mut attempts = 0;

                        loop {
                            if self.needs_abort(this_ref) {
                                return false;
                            }

                            let (is_cycle, visit_path, edge_path) =
                                self.cycle_check_restricted_init(this_ref);

                            if is_cycle {
                                // the cycle found includes the inserted RW edge

                                if visit_path.contains(&from_id) {
                                    let cycle_type = classify_cycle(&edge_path);
                                    meta.set_cycle_type(cycle_type);

                                    let path_len = edge_path.len();
                                    meta.set_path_len(path_len);

                                    let this_iso = this_ref.get_isolation_level();

                                    if let IsolationLevel::Serializable = this_iso {
                                        return false; // abort this node
                                    } else {
                                        from_ref.set_cascading_abort();
                                        return true; // abort that node
                                    }
                                } else {
                                    // TODO: this cycle is never counted in the stats
                                    // the cycle found did not include the inserted RW edge
                                    let cycle_type = classify_cycle(&edge_path);

                                    // if was a G2 we can help out by aborting a PL-3 in the path
                                    let mut aborted = 0;
                                    if let Cycle::G2 = cycle_type {
                                        for node_id in visit_path {
                                            let cur = unsafe { &*(node_id as *const Node) };
                                            if let IsolationLevel::Serializable =
                                                cur.get_isolation_level()
                                            {
                                                cur.set_cascading_abort();
                                                aborted = node_id;
                                                break;
                                            }
                                        }
                                    }

                                    if attempts > ATTEMPTS {
                                        panic!(
                                                "{} ({}) stuck in cycle loop. Incoming {:?}, Found cycle: {:?}, aborted: {}",
                                                this_ref.get_id().unwrap(),
                                                this_ref.get_isolation_level(),
                                                this_ref.get_incoming(),
                                                cycle_type,
                                                aborted

                                            );
                                    }

                                    // try find my cycle (if there is one)
                                }
                            } else {
                                // no cycle found
                                return true;
                            }
                            attempts += 1;
                        }
                    } else {
                        // edge was a WW/WR edge
                        let (is_cycle, _, edge_path) = self.cycle_check_restricted_init(this_ref);

                        if is_cycle {
                            let cycle_type = classify_cycle(&edge_path);
                            meta.set_cycle_type(cycle_type);

                            let path_len = edge_path.len();
                            meta.set_path_len(path_len);
                        }

                        return !is_cycle;
                    }
                }
            }
        }
    }

    fn get_visited(&self) -> RefMut<FxHashSet<usize>> {
        self.visited
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut()
    }

    fn get_visit_path(&self) -> RefMut<Vec<usize>> {
        self.visit_path
            .get_or(|| RefCell::new(Vec::new()))
            .borrow_mut()
    }

    fn get_edge_path(&self) -> RefMut<Vec<Edge>> {
        self.edge_path
            .get_or(|| RefCell::new(Vec::new()))
            .borrow_mut()
    }

    fn cycle_check_restricted_init(&self, this_node: &Node) -> (bool, Vec<usize>, Vec<Edge>) {
        let mut visited = self.get_visited();
        let mut visit_path = self.get_visit_path();
        let mut edge_path = self.get_edge_path();

        let this_id = this_node.get_id();
        let root_id = this_node.get_id();

        let root_lvl = this_node.get_isolation_level();

        visited.clear();
        visit_path.clear();
        edge_path.clear();

        let mut check = false;
        if !visited.contains(&this_id.unwrap()) {
            check = self.check_cycle_restricted(
                this_id.unwrap(),
                root_lvl,
                &mut visited,
                &mut visit_path,
                &mut edge_path,
                root_id.unwrap(),
            );
        }

        return (check, visit_path.to_vec(), edge_path.to_vec());
    }

    fn check_cycle_restricted(
        &self,
        cur: usize,
        root_lvl: IsolationLevel,
        visited: &mut RefMut<FxHashSet<usize>>,
        visit_path: &mut RefMut<Vec<usize>>,
        edge_path: &mut RefMut<Vec<Edge>>,
        root_id: usize,
    ) -> bool {
        visited.insert(cur);
        visit_path.push(cur);

        let cur = unsafe { &*(cur as *const Node) };
        let g = cur.read();
        if !cur.is_cleaned() {
            let incoming = cur.get_incoming();
            for edge in incoming {
                edge_path.push(edge.clone());
                let id = edge.extract_id() as usize;

                if id == root_id {
                    visit_path.push(id);
                    drop(g);
                    return true;
                } else if !visited.contains(&id) {
                    if self.check_cycle_restricted(
                        id, root_lvl, visited, visit_path, edge_path, root_id,
                    ) {
                        drop(g);
                        return true;
                    }
                }

                let path_len = edge_path.len();
                edge_path.remove(path_len - 1);
            }
        }

        drop(g);
        let cur = cur.get_id().unwrap() as usize;
        let index = visit_path.iter().position(|x| *x == cur).unwrap();
        visit_path.remove(index);

        return false;
    }

    fn cycle_check_reduced_init(&self, this_ref: &Node) -> Option<Vec<Edge>> {
        let mut visited = self.get_visited();
        let mut visit_path = self.get_visit_path();
        let mut edge_path = self.get_edge_path();

        let this_id = this_ref.get_id();

        let root_lvl = this_ref.get_isolation_level();

        visited.clear();
        visit_path.clear();
        edge_path.clear();

        let mut check = false;
        if !visited.contains(&this_id.unwrap()) {
            check = self.check_cycle_reduced(
                this_id.unwrap(),
                root_lvl,
                &mut visited,
                &mut visit_path,
                &mut edge_path,
            );
        }

        if check {
            Some(edge_path.to_vec())
        } else {
            None
        }
    }

    fn check_cycle_reduced(
        &self,
        cur: usize,
        root_lvl: IsolationLevel,
        visited: &mut RefMut<FxHashSet<usize>>,
        visit_path: &mut RefMut<Vec<usize>>,
        edge_path: &mut RefMut<Vec<Edge>>,
    ) -> bool {
        visited.insert(cur);
        visit_path.push(cur);

        let cur = unsafe { &*(cur as *const Node) };
        let g = cur.read();
        if !cur.is_cleaned() {
            let incoming = cur.get_incoming();
            for edge in incoming {
                edge_path.push(edge.clone());
                let id = edge.extract_id() as usize;

                if visit_path.contains(&id) {
                    drop(g);
                    return true;
                } else {
                    if self.check_cycle_reduced(id, root_lvl, visited, visit_path, edge_path) {
                        drop(g);
                        return true;
                    }
                }

                let path_len = edge_path.len();
                edge_path.remove(path_len - 1);
            }
        }

        drop(g);
        let cur = cur.get_id().unwrap() as usize;
        let index = visit_path.iter().position(|x| *x == cur).unwrap();
        visit_path.remove(index);

        return false;
    }

    fn needs_abort(&self, this_ref: &Node) -> bool {
        let aborted = this_ref.is_aborted();
        let cascading_abort = this_ref.is_cascading_abort();

        aborted || cascading_abort
    }

    fn create_node(&self, isolation_level: IsolationLevel) -> usize {
        let incoming = Mutex::new(FxHashSet::default()); // init edge sets
        let outgoing = Mutex::new(FxHashSet::default());
        let iso = Some(isolation_level);
        let node = Box::new(Node::new(incoming, outgoing, iso)); // allocate node
        let ptr: *mut Node = Box::into_raw(node); // convert to raw pt
        let id = ptr as usize;
        unsafe { (*ptr).set_id(id) }; // set id on node
        MixedSerializationGraph::NODE.with(|x| x.borrow_mut().replace(ptr)); // store in thread local

        id
    }

    fn check_committed(
        &self,
        this_ref: &Node,
        database: &Database,
        ops: &Vec<Operation>,
        meta: &mut StatsBucket,
    ) -> bool {
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

            if !meta.could_have_committed_early() {
                if let IsolationLevel::ReadCommitted | IsolationLevel::ReadUncommitted =
                    this_ref.get_isolation_level()
                {
                    let incoming = this_ref.get_incoming();
                    if incoming.len() == 1 {
                        for edge in incoming.iter() {
                            if let Edge::ReadWrite(_) = edge {
                                // could have been an early commit rule
                                meta.early_commit_possible();
                            }
                        }
                    }
                }
            }

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

    pub fn begin(&self, isolation_level: IsolationLevel) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // increment txn ctr
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new()); // reset txn info
        let ref_id = self.create_node(isolation_level); // create node
        let guard = epoch::pin(); // pin thread
        MixedSerializationGraph::EG.with(|x| x.borrow_mut().replace(guard)); // add to guard

        TransactionId::SerializationGraph(ref_id)
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

        let abort = self.spin(prv, lsn, this_ref, database, meta);

        if abort {
            let ops = self.get_operations();

            rw_table.erase(prv);
            lsn.store(prv + 1, Ordering::Release);

            self.remove_accesses(database, &ops);
            MixedSerializationGraph::EG.with(|x| {
                let guard = x.borrow_mut().take();
                drop(guard)
            });

            return Err(NonFatalError::NoccError); // TODO: abort called externally
        }

        let guard = &epoch::pin();
        let snapshot = rw_table.iter(guard);

        let mut cyclic = false;

        for (id, access) in snapshot {
            if id < &prv {
                if let Access::Write(from_tid) = access {
                    if let TransactionId::SerializationGraph(from_id) = from_tid {
                        let from = Edge::WriteRead(*from_id);

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
        let tuple = table.get_tuple(column_id, offset);
        let vals = tuple.get().get_value().unwrap().get_value();

        lsn.store(prv + 1, Ordering::Release);
        self.record(OperationType::Read, vid, prv);

        Ok(vals)
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

        let mut attempts = 0;
        loop {
            if attempts > ATTEMPTS {
                let this_node = unsafe { &*self.get_transaction() };
                panic!(
                    "{} ({}) stuck writing. Incoming {:?}",
                    this_node.get_id().unwrap(),
                    this_node.get_isolation_level(),
                    this_node.get_incoming(),
                );
            }

            if self.needs_abort(this_ref) {
                let id = this_ref.get_abort_through();
                meta.set_abort_through(id);
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            prv = rw_table.push_front(Access::Write(tid));

            let abort = self.spin(prv, lsn, this_ref, database, meta);

            if abort {
                let ops = self.get_operations();

                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn

                self.remove_accesses(database, &ops);
                MixedSerializationGraph::EG.with(|x| {
                    let guard = x.borrow_mut().take();
                    drop(guard)
                });
                return Err(NonFatalError::NoccError); // TODO: abort called externally
            }

            // On acquiring the 'lock' on the record it is possible another transaction has an uncommitted write on this record.
            // In this case the operation is restarted after a cycle check.
            let guard = &epoch::pin(); // pin thread
            let snapshot = rw_table.iter(guard);

            let mut wait = false;
            let mut cyclic = false;

            for (id, access) in snapshot {
                if id < &prv {
                    if let Access::Write(from_tid) = access {
                        if let TransactionId::SerializationGraph(from_id) = from_tid {
                            let from_ref = unsafe { &*(*from_id as *const Node) };
                            if !from_ref.is_committed() {
                                let from = Edge::WriteWrite(*from_id);
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
                attempts += 1;

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

    pub fn commit(&self, meta: &mut StatsBucket, database: &Database) -> Result<(), NonFatalError> {
        let this_node = unsafe { &*self.get_transaction() };

        this_node.set_commit_phase();
        let ops = self.get_operations();

        let mut all_pending_transactions_committed = false;
        let mut attempts = 0;
        while !all_pending_transactions_committed {
            if this_node.is_cascading_abort() {
                return Err(SerializationGraphError::CascadingAbort.into());
            }

            if this_node.is_aborted() {
                return Err(SerializationGraphError::CycleFound.into());
            }

            all_pending_transactions_committed =
                self.check_committed(this_node, database, &ops, meta);

            if all_pending_transactions_committed {
                self.remove_accesses(database, &ops);

                MixedSerializationGraph::EG.with(|x| {
                    let guard = x.borrow_mut().take();
                    drop(guard)
                });
            }

            attempts += 1;

            if attempts > ATTEMPTS {
                panic!(
                    "{} ({}) stuck committing. Incoming {:?}. Cascading {:?}",
                    this_node.get_id().unwrap(),
                    this_node.get_isolation_level(),
                    this_node.get_incoming(),
                    this_node.is_cascading_abort()
                );
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
            match edge {
                Edge::WriteWrite(id) => {
                    meta.add_problem_transaction(id);
                }
                Edge::WriteRead(id) => {
                    meta.add_problem_transaction(id);
                }
                Edge::ReadWrite(_) => {}
            }
        }

        self.cleanup(this);
        self.remove_accesses(database, &ops);
        MixedSerializationGraph::EG.with(|x| {
            let guard = x.borrow_mut().take();
            drop(guard)
        });
    }

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
                Edge::WriteWrite(that_id) => {
                    let that = unsafe { &*(*that_id as *const Node) };
                    if this.is_aborted() {
                        let id = this.get_id();
                        that.set_abort_through(id.unwrap());
                        that.set_cascading_abort();
                    } else {
                        let that_rlock = that.read();
                        if !that.is_cleaned() {
                            that.remove_incoming(&Edge::WriteWrite(this_id));
                        }
                        drop(that_rlock);
                    }
                } // (this) -[wr]-> (to)
                Edge::WriteRead(that) => {
                    let that = unsafe { &*(*that as *const Node) };
                    if this.is_aborted() {
                        let id = this.get_id();
                        that.set_abort_through(id.unwrap());
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

        MixedSerializationGraph::EG.with(|x| unsafe {
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

    fn spin(
        &self,
        prv: u64,
        lsn: &AtomicU64,
        this: &Node,
        database: &Database,
        meta: &mut StatsBucket,
    ) -> bool {
        let mut i = 0;
        let mut attempts = 0;

        let mut abort = false;

        while lsn.load(Ordering::Relaxed) != prv {
            // only execute this one
            if let CycleCheckingStrategy::Relevant = self.cycle_checking_strategy {
                if !abort {
                    if self.needs_abort(this) {
                        let ops = self.get_operations();
                        self.commit_writes(database, false, &ops);
                        let this = unsafe { &*self.get_transaction() };
                        this.set_aborted();

                        let incoming = this.get_incoming();
                        for edge in incoming {
                            match edge {
                                Edge::WriteWrite(id) => {
                                    meta.add_problem_transaction(id);
                                }
                                Edge::WriteRead(id) => {
                                    meta.add_problem_transaction(id);
                                }
                                Edge::ReadWrite(_) => {}
                            }
                        }

                        self.cleanup(this);
                        abort = true;
                    }
                }
            }

            i += 1;
            if i >= 10000 {
                std::thread::yield_now();
            }

            if attempts > ATTEMPTS {
                panic!(
                    "{} ({}) stuck spinning. Incoming {:?}, Cascading: {:?}",
                    this.get_id().unwrap(),
                    this.get_isolation_level(),
                    this.get_incoming(),
                    this.is_cascading_abort()
                );
            }

            attempts += 1;
        }
        abort
    }
}

fn is_self_edge(this_ref: &Node, from_ref: &Node) -> bool {
    let this_id = this_ref.get_id();
    let from_id = from_ref.get_id();

    this_id == from_id
}

fn is_relevant_or_obligatory(from: &Edge, this_ref: &Node) -> Option<Edge> {
    let this_id = this_ref.get_id().unwrap();

    match *from {
        // WW edges always get inserted
        Edge::WriteWrite(_) => Some(Edge::WriteWrite(this_id)),

        // WR edge inserted if this node is PL-2/3
        Edge::WriteRead(_) => {
            let this_iso = this_ref.get_isolation_level();

            if let IsolationLevel::ReadUncommitted = this_iso {
                None
            } else {
                Some(Edge::WriteRead(this_id))
            }
        }

        // RW edge inserted if from node is PL3
        Edge::ReadWrite(from_id) => {
            let from_ref = unsafe { &*(from_id as *const Node) };
            let from_iso = from_ref.get_isolation_level();
            match from_iso {
                IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => None,
                IsolationLevel::Serializable => Some(Edge::ReadWrite(this_id)),
            }
        }
    }
}

fn _print_node_path(visit_path: &mut RefMut<Vec<usize>>) -> String {
    let vv: Vec<_> = visit_path.borrow_mut().iter().collect::<Vec<_>>();
    let path_len = vv.len();
    let mut path = String::new();
    for node in &vv[0..path_len - 1] {
        let id = **node;
        let cur = unsafe { &*(id as *const Node) };

        let id = cur.get_id();
        path.push_str(&format!(
            "({:x}:{}),",
            id.unwrap(),
            cur.get_isolation_level()
        ));
    }
    let last = vv[path_len - 1];
    let cur = unsafe { &*(*last as *const Node) };
    path.push_str(&format!(
        "({:x}:{})",
        cur.get_id().unwrap(),
        cur.get_isolation_level()
    ));
    path
}

fn classify_cycle(edge_path: &Vec<Edge>) -> Cycle {
    let mut wr = 0;
    let mut rw = 0;

    for edge in edge_path {
        match edge {
            Edge::WriteWrite(_) => {}
            Edge::WriteRead(_) => wr += 1,
            Edge::ReadWrite(_) => rw += 1,
        }
    }

    if rw > 0 {
        Cycle::G2
    } else if wr > 0 {
        Cycle::G1c
    } else {
        Cycle::G0
    }
}
