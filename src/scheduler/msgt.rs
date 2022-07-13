use crate::common::error::{NonFatalError, SerializationGraphError};
use crate::common::isolation_level::IsolationLevel;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
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

static ATTEMPTS: u64 = 1000000;

enum Cycle {
    G0,
    G1c,
    G2,
}

#[derive(Debug)]
pub struct MixedSerializationGraph {
    txn_ctr: ThreadLocal<RefCell<usize>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    visit_path: ThreadLocal<RefCell<Vec<usize>>>,
    edge_path: ThreadLocal<RefCell<Vec<Edge>>>,
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
            visit_path: ThreadLocal::new(),
            edge_path: ThreadLocal::new(),
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
            .get_clone()
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

    pub fn insert_and_check(&self, meta: &mut StatsBucket, from: Edge) -> bool {
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
            return true;
        }

        let mut attempts = 0;
        loop {
            if attempts > ATTEMPTS {
                let this_node = unsafe { &*self.get_transaction() };
                panic!(
                    "{:x} ({}) stuck inserting {:?}. Incoming {:?}",
                    this_node.get_id(),
                    this_node.get_isolation_level(),
                    from,
                    this_node.get_incoming(),
                );
            }

            if this_ref.incoming_edge_exists(&from) {
                return true;
            };

            let from_ref = unsafe { &*(from_id as *const Node) };

            if (from_ref.is_aborted() || from_ref.is_cascading_abort()) && !rw {
                this_ref.set_cascading_abort();
                let fid = from_ref.get_id();
                this_ref.set_abort_through(fid);
                meta.set_abort_through(fid);
                return false;
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

            from_ref.insert_outgoing(out_edge.clone()); // (from)
            this_ref.insert_incoming(from.clone()); // (to)
            drop(from_rlock);

            if self.relevant_cycle_check {
                let is_g0_cycle = self.g0_cycle_check_init(this_ref);
                let (is_g1_cycle, _) = self.g1_cycle_check_init(this_ref);
                let (is_g2_cycle, memb) = self.cycle_check_init(this_ref);

                let is_cycle = is_g0_cycle || is_g1_cycle || is_g2_cycle;

                if is_cycle {
                    if let IsolationLevel::ReadCommitted = this_ref.get_isolation_level() {
                        if is_g0_cycle || is_g1_cycle {
                            return false; // abort self
                        } else {
                            // from_ref.set_cascading_abort();

                            for node_id in &memb {
                                if *node_id != this_ref.get_id() {
                                    let cur = unsafe { &*(*node_id as *const Node) };
                                    match cur.get_isolation_level() {
                                        IsolationLevel::Serializable => {
                                            cur.set_cascading_abort();

                                            let tj_in_cycle = memb.contains(&from_ref.get_id());
                                            if !tj_in_cycle {
                                                println!(
                                                "I'm {} aborting: {}. The edge is {:?}. The path is {:?}, Tj in path: {}",
                                                this_ref.get_id(),
                                                cur.get_id(),
                                                from,
                                                memb,
                                                tj_in_cycle
                                            );
                                            }
                                            break;
                                        }
                                        IsolationLevel::ReadUncommitted
                                        | IsolationLevel::ReadCommitted => {}
                                    }
                                }
                            }
                        }
                    } else {
                        return false;
                    }
                }
                return true; // if no cycle or if aborted else

            // return self.complex(this_ref, &from);
            } else {
                let (is_cycle, _) = self.cycle_check_init(this_ref);
                return !is_cycle; // false equals cycle so flip
            }
        }
    }

    // fn complex(&self, this_ref: &Node, from: &Edge) -> bool {
    //     let this_node_isolation_level = this_ref.get_isolation_level();
    //     match *from {
    //         Edge::WriteWrite(_) => {
    //             let is_g0_cycle = self.g0_cycle_check_init(this_ref);
    //             if is_g0_cycle {
    //                 return false; // this abort this node
    //             }

    //             let (is_g1_cycle, visit_path) = self.g1_cycle_check_init(this_ref);
    //             if is_g1_cycle {
    //                 match this_node_isolation_level {
    //                     IsolationLevel::ReadUncommitted => {
    //                         // abort first PL2/3
    //                         for node_id in visit_path {
    //                             let cur = unsafe { &*(node_id as *const Node) };
    //                             match cur.get_isolation_level() {
    //                                 IsolationLevel::Serializable
    //                                 | IsolationLevel::ReadCommitted => {
    //                                     cur.set_cascading_abort();
    //                                     break;
    //                                 }
    //                                 IsolationLevel::ReadUncommitted => {}
    //                             }
    //                         }
    //                         return true;
    //                     }
    //                     IsolationLevel::ReadCommitted | IsolationLevel::Serializable => {
    //                         // abort this node
    //                         return false;
    //                     }
    //                 }
    //             }

    //             let (is_g2_cycle, visit_path) = self.cycle_check_init(this_ref);
    //             if is_g2_cycle {
    //                 match this_node_isolation_level {
    //                     IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => {
    //                         // abort first PL-3
    //                         for node_id in visit_path {
    //                             let cur = unsafe { &*(node_id as *const Node) };
    //                             match cur.get_isolation_level() {
    //                                 IsolationLevel::Serializable => {
    //                                     cur.set_cascading_abort();
    //                                     break;
    //                                 }
    //                                 IsolationLevel::ReadUncommitted
    //                                 | IsolationLevel::ReadCommitted => {}
    //                             }
    //                         }
    //                         return true;
    //                     }
    //                     IsolationLevel::Serializable => {
    //                         // abort this node
    //                         return false;
    //                     }
    //                 }
    //             }
    //         }
    //         Edge::WriteRead(_) => {
    //             // can't introduce a relevant cycle (g0)
    //             let (is_g1_cycle, visit_path) = self.g1_cycle_check_init(this_ref);
    //             if is_g1_cycle {
    //                 match this_node_isolation_level {
    //                     IsolationLevel::ReadUncommitted => {
    //                         // abort first PL2/3
    //                         for node_id in visit_path {
    //                             let cur = unsafe { &*(node_id as *const Node) };
    //                             match cur.get_isolation_level() {
    //                                 IsolationLevel::Serializable
    //                                 | IsolationLevel::ReadCommitted => {
    //                                     cur.set_cascading_abort();
    //                                     break;
    //                                 }
    //                                 IsolationLevel::ReadUncommitted => {}
    //                             }
    //                         }
    //                         return true;
    //                     }
    //                     IsolationLevel::ReadCommitted | IsolationLevel::Serializable => {
    //                         // abort this node
    //                         return false;
    //                     }
    //                 }
    //             }

    //             let (is_g2_cycle, visit_path) = self.cycle_check_init(this_ref);
    //             if is_g2_cycle {
    //                 match this_node_isolation_level {
    //                     IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => {
    //                         // abort first PL-3
    //                         for node_id in visit_path {
    //                             let cur = unsafe { &*(node_id as *const Node) };
    //                             match cur.get_isolation_level() {
    //                                 IsolationLevel::Serializable => {
    //                                     cur.set_cascading_abort();
    //                                     break;
    //                                 }
    //                                 IsolationLevel::ReadUncommitted
    //                                 | IsolationLevel::ReadCommitted => {}
    //                             }
    //                         }
    //                         return true;
    //                     }
    //                     IsolationLevel::Serializable => {
    //                         // abort this node
    //                         return false;
    //                     }
    //                 }
    //             }
    //         }
    //         Edge::ReadWrite(_) => {
    //             let (is_g2_cycle, visit_path) = self.cycle_check_init(this_ref);
    //             if is_g2_cycle {
    //                 match this_node_isolation_level {
    //                     IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => {
    //                         // abort first PL-3
    //                         for node_id in visit_path {
    //                             let cur = unsafe { &*(node_id as *const Node) };
    //                             match cur.get_isolation_level() {
    //                                 IsolationLevel::Serializable => {
    //                                     cur.set_cascading_abort();
    //                                     break;
    //                                 }
    //                                 IsolationLevel::ReadUncommitted
    //                                 | IsolationLevel::ReadCommitted => {}
    //                             }
    //                         }
    //                     }
    //                     IsolationLevel::Serializable => {
    //                         // abort this node
    //                         return false;
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

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

    // A transaction can only introduce a G0 anomaly by inserting a WW edge.
    fn g0_cycle_check_init(&self, this_node: &Node) -> bool {
        let mut visited = self.get_visited();
        let mut visit_path = self.get_visit_path();
        let mut edge_path = self.get_edge_path();

        let root_id = this_node.get_id();

        visited.clear();
        visit_path.clear();
        edge_path.clear();

        let mut check = false;
        if !visited.contains(&root_id) {
            check = self.g0_cycle_check_naive(
                root_id,
                &mut visited,
                &mut visit_path,
                &mut edge_path,
                root_id,
            );
        }

        return check;
    }

    fn g0_cycle_check_naive(
        &self,
        cur: usize,
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
                // only traverse WW/WR edges

                let traverse = match edge {
                    Edge::WriteWrite(_) => true,
                    Edge::WriteRead(_) => false,
                    Edge::ReadWrite(_) => false,
                };

                if traverse {
                    let id = edge.extract_id() as usize;

                    edge_path.push(edge.clone());

                    // if back to root then a WW cycle has been found
                    if id == root_id {
                        visit_path.push(id);
                        drop(g);
                        return true;
                    } else if !visited.contains(&id) {
                        // if not yet visited go to the next node
                        if self.g0_cycle_check_naive(id, visited, visit_path, edge_path, root_id) {
                            drop(g);
                            return true;
                        }
                    }

                    // if already visited need to remove the edge from the path (it is last added)
                    let path_len = edge_path.len();
                    edge_path.remove(path_len - 1);
                }
            }
        }

        // no more nodes to visit
        drop(g);
        let cur = cur.get_id() as usize;
        // not on visit path
        let index = visit_path.iter().position(|x| *x == cur).unwrap();
        visit_path.remove(index);

        return false;
    }

    fn g1_cycle_check_init(&self, this_node: &Node) -> (bool, Vec<usize>) {
        let mut visited = self.get_visited();
        let mut visit_path = self.get_visit_path();
        let mut edge_path = self.get_edge_path();

        let root_id = this_node.get_id();

        visited.clear();
        visit_path.clear();
        edge_path.clear();

        let mut check = false;
        if !visited.contains(&root_id) {
            check = self.g1_cycle_check_naive(
                root_id,
                &mut visited,
                &mut visit_path,
                &mut edge_path,
                root_id,
            );
        }

        return (check, visit_path.to_vec());
    }

    fn g1_cycle_check_naive(
        &self,
        cur: usize,
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
                // only traverse WW/WR edges

                let traverse = match edge {
                    Edge::WriteWrite(_) => true,
                    Edge::WriteRead(_) => true,
                    Edge::ReadWrite(_) => false,
                };

                if traverse {
                    let id = edge.extract_id() as usize;

                    edge_path.push(edge.clone());

                    // if back to root then a WW cycle has been found
                    if id == root_id {
                        visit_path.push(id);
                        drop(g);
                        return true;
                    } else if !visited.contains(&id) {
                        // if not yet visited go to the next node
                        if self.g1_cycle_check_naive(id, visited, visit_path, edge_path, root_id) {
                            drop(g);
                            return true;
                        }
                    }

                    // if already visited need to remove the edge from the path (it is last added)
                    let path_len = edge_path.len();
                    edge_path.remove(path_len - 1);
                }
            }
        }

        // no more nodes to visit
        drop(g);
        let cur = cur.get_id() as usize;
        // not on visit path
        let index = visit_path.iter().position(|x| *x == cur).unwrap();
        visit_path.remove(index);

        return false;
    }

    fn cycle_check_init(&self, this_node: &Node) -> (bool, Vec<usize>) {
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
        if !visited.contains(&this_id) {
            check = self.check_cycle_naive(
                this_id,
                root_lvl,
                &mut visited,
                &mut visit_path,
                &mut edge_path,
                root_id,
            );
        }

        return (check, visit_path.to_vec());
    }

    fn check_cycle_naive(
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
                    if self.check_cycle_naive(id, root_lvl, visited, visit_path, edge_path, root_id)
                    {
                        drop(g);
                        return true;
                    }
                }

                let path_len = edge_path.len();
                edge_path.remove(path_len - 1);
            }
        }

        drop(g);
        let cur = cur.get_id() as usize;
        let index = visit_path.iter().position(|x| *x == cur).unwrap();
        visit_path.remove(index);

        return false;
    }

    pub fn needs_abort(&self) -> bool {
        let this = unsafe { &*self.get_transaction() };

        let aborted = this.is_aborted();
        let cascading_abort = this.is_cascading_abort();
        aborted || cascading_abort
    }

    pub fn create_node(&self, isolation_level: IsolationLevel) -> usize {
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

    fn check_committed(&self, this_node: &Node, database: &Database, ops: &Vec<Operation>) -> bool {
        if this_node.is_aborted() || this_node.is_cascading_abort() {
            return false;
        }

        let read_lock = this_node.read();
        this_node.set_checked(true);
        drop(read_lock);

        let write_lock = this_node.write();
        drop(write_lock);

        let read_lock = this_node.read();
        if this_node.has_incoming() {
            this_node.set_checked(false);
            drop(read_lock);

            return false;
        }
        drop(read_lock);

        if this_node.is_aborted() || this_node.is_cascading_abort() {
            return false;
        }

        let success = self.erase_graph_constraints(this_node, database, ops);

        if success {
            self.cleanup(this_node);
        }

        return success;
    }

    fn erase_graph_constraints(
        &self,
        this_node: &Node,
        database: &Database,
        ops: &Vec<Operation>,
    ) -> bool {
        // let id = this_node.get_id();
        // let is_cycle = self.cycle_check_init(this_node);
        // if is_cycle {
        //     this_node.set_aborted();
        // }

        // NFN: different
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
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();

        if self.needs_abort() {
            let this = unsafe { &*self.get_transaction() };
            let id = this.get_abort_through();
            meta.set_abort_through(id);

            return Err(SerializationGraphError::CascadingAbort.into()); // check for cascading abort
        }

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let prv = rw_table.push_front(Access::Read(meta.get_transaction_id()));
        let lsn = table.get_lsn(offset);

        // Safety: ensures exculsive access to the record.
        let this = unsafe { &*self.get_transaction() };

        unsafe { spin(prv, lsn, this) }; // busy wait

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
                            if !self.insert_and_check(meta, Edge::WriteWrite(*from_id)) {
                                cyclic = true;
                                break;
                            }
                        }
                    }
                    Access::Read(_) => {}
                }
            }
        }

        drop(guard);

        if cyclic {
            rw_table.erase(prv); // remove from rw table
            lsn.store(prv + 1, Ordering::Release); // update lsn

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

        let mut attempts = 0;
        loop {
            if attempts > ATTEMPTS {
                let this_node = unsafe { &*self.get_transaction() };
                panic!(
                    "{:x} ({}) stuck writing. Incoming {:?}",
                    this_node.get_id(),
                    this_node.get_isolation_level(),
                    this_node.get_incoming(),
                );
            }

            if self.needs_abort() {
                let this = unsafe { &*self.get_transaction() };
                let id = this.get_abort_through();
                meta.set_abort_through(id);
                return Err(SerializationGraphError::CascadingAbort.into());
                // check for cascading abort
            }

            prv = rw_table.push_front(Access::Write(meta.get_transaction_id())); // get ticket
            let this = unsafe { &*self.get_transaction() };

            unsafe { spin(prv, lsn, this) }; // Safety: ensures exculsive access to the record

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
                                    if !self.insert_and_check(meta, Edge::WriteWrite(*from_addr)) {
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

            drop(guard);

            // (i) transaction is in a cycle (cycle = T)
            // abort transaction
            if cyclic {
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn

                return Err(SerializationGraphError::CycleFound.into());
            }

            // (ii) there is an uncommitted write (wait = T)
            // restart operation
            if wait {
                rw_table.erase(prv); // remove from rw table
                lsn.store(prv + 1, Ordering::Release); // update lsn
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
                match access {
                    Access::Read(from) => {
                        if let TransactionId::SerializationGraph(from_addr) = from {
                            if !self.insert_and_check(meta, Edge::ReadWrite(*from_addr)) {
                                cyclic = true;
                                break;
                            }
                        }
                    }
                    Access::Write(_) => {}
                }
            }
        }
        drop(guard);

        // (iv) transaction is in a cycle (cycle = T)
        // abort transaction
        if cyclic {
            rw_table.erase(prv); // remove from rw table
            lsn.store(prv + 1, Ordering::Release); // update lsn
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

    pub fn commit(
        &self,
        _meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
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

            all_pending_transactions_committed = self.check_committed(this_node, database, &ops);

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
                    this_node.get_id(),
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

        let incoming = this.get_incoming().clone();
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
                        that.set_abort_through(id);
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
                        that.set_abort_through(id);
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
}

fn _abort_cleanup(
    this_isolation: IsolationLevel,
    cycle_type: Cycle,
    path: String,
    edge_path: RefMut<Vec<Edge>>,
    visit_path: RefMut<Vec<usize>>,
    check: &mut bool,
) {
    match this_isolation {
        // PL-3 always abort
        IsolationLevel::Serializable => {}
        // if Im PL-2, G1c/0 I abort, else S aborts
        IsolationLevel::ReadCommitted => match cycle_type {
            Cycle::G2 => {
                println!(
                    "Didn't need to abort: RC with a G2 {} {:?}",
                    path, edge_path
                );
                for node in &*visit_path {
                    let id = *node;
                    let cur = unsafe { &*(id as *const Node) };
                    if let IsolationLevel::Serializable = cur.get_isolation_level() {
                        println!("Abort this guy: {:x}", id);
                        cur.set_cascading_abort();
                        *check = false;
                        break;
                    }
                }
            }
            Cycle::G1c => {}
            Cycle::G0 => {}
        },
        // if Im PL-2, G1c/0 I abort, else S aborts
        IsolationLevel::ReadUncommitted => match cycle_type {
            // s aborts
            Cycle::G2 => {
                // for node in &*visit_path {
                //     let id = *node;
                //     let cur = unsafe { &*(id as *const Node) };
                //     if let IsolationLevel::Serializable = cur.get_isolation_level() {
                //         cur.set_cascading_abort();
                //         check = false;
                //         break;
                //     }
                // }
            }
            // s or rc aborts
            Cycle::G1c => {
                // for node in &*visit_path {
                //     let id = *node;
                //     let cur = unsafe { &*(id as *const Node) };

                //     match cur.get_isolation_level() {
                //         IsolationLevel::ReadUncommitted => {}
                //         IsolationLevel::ReadCommitted | IsolationLevel::Serializable => {
                //             cur.set_cascading_abort();
                //             check = false;
                //             break;
                //         }
                //     }
                // }
            }
            Cycle::G0 => {}
        },
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
        path.push_str(&format!("({:x}:{}),", id, cur.get_isolation_level()));
    }
    let last = vv[path_len - 1];
    let cur = unsafe { &*(*last as *const Node) };
    path.push_str(&format!(
        "({:x}:{})",
        cur.get_id(),
        cur.get_isolation_level()
    ));
    path
}

fn _classify_cycle(edge_path: &RefMut<Vec<Edge>>) -> Cycle {
    let mut wr = 0;
    let mut rw = 0;

    for edge in &**edge_path {
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

unsafe fn spin(prv: u64, lsn: &AtomicU64, this: &Node) {
    let mut i = 0;
    let mut attempts = 0;
    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }

        if attempts > ATTEMPTS {
            panic!(
                "{:x} ({}) stuck spinning. Incoming {:?}",
                this.get_id(),
                this.get_isolation_level(),
                this.get_incoming(),
            );
        }

        attempts += 1;
    }
}
