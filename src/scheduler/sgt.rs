use crate::scheduler::sgt::epoch_manager::{EpochGuard, EpochManager};
use crate::scheduler::sgt::node::{ArcNode, Node, WeakNode};
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::scheduler::NonFatalError;
use crate::scheduler::{Scheduler, TransactionInfo};
use crate::storage::datatype::Data;
use crate::storage::row::Access;
use crate::workloads::{PrimaryKey, Workload};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, RwLock};
use thread_local::ThreadLocal;
use tracing::{debug, info};

pub mod transaction_information;

pub mod node;

pub mod epoch_manager;

pub mod error;

#[derive(Debug)]
pub struct SerializationGraph {
    this_node: ThreadLocal<RefCell<Option<ArcNode>>>,
    txn_ctr: ThreadLocal<RefCell<u64>>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    eg: ThreadLocal<RefCell<EpochGuard>>,
    em: Arc<EpochManager>,
    data: Arc<Workload>,
}

impl SerializationGraph {
    /// Initialise a serialization graph.
    pub fn new(size: u32, data: Workload) -> Self {
        info!("Initialise basic serialization graph with {} node(s)", size);
        let this_node = ThreadLocal::new();
        let txn_ctr = ThreadLocal::new();
        let txn_info = ThreadLocal::new();
        let em = Arc::new(EpochManager::new(size.into()));

        let eg = ThreadLocal::new();

        SerializationGraph {
            this_node,
            txn_ctr,
            txn_info,
            eg,
            em,
            data: Arc::new(data),
        }
    }

    /// Create a node.
    pub fn create_node(&self) -> WeakNode {
        debug!("create_node");
        let node = Arc::new(RwLock::new(Node::new()));
        let weak = Arc::downgrade(&node);
        self.this_node.get_or(|| RefCell::new(Some(node))); // set thread local variable
        let eg = self
            .eg
            .get_or(|| RefCell::new(EpochGuard::new(Arc::clone(&self.em)))); // set thread local variable
        eg.borrow_mut().pin();

        weak
    }

    pub fn cleanup(&self) {
        let this_node: ArcNode =
            Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());
        let this_node_rlock = this_node.read().unwrap();

        this_node_rlock.set_checked(true); // set as checked

        let outgoing = this_node_rlock.get_outgoing();
        debug!("remove {} outgoing edges", outgoing.len());

        for (that_node, rw_edge) in outgoing {
            let that_node = that_node.upgrade().unwrap();
            let that_node_rlock = that_node.read().unwrap();
            if this_node_rlock.is_aborted() && !rw_edge {
                that_node_rlock.set_cascading_abort(); // if abort and not rw; cascade abort
            } else {
                if !that_node_rlock.is_cleaned() {
                    that_node_rlock.remove_incoming(Arc::downgrade(&this_node));
                    // if not cleaned; remove that node incoming
                }
            }
            drop(that_node_rlock);
            this_node_rlock.clear_outgoing();
        }

        if this_node_rlock.is_aborted() {
            this_node_rlock.clear_incoming(); // if aborted; clear incoming
        }

        let node = self.this_node.get().unwrap().borrow_mut().take().unwrap();

        self.eg.get().unwrap().borrow_mut().add(node);
    }

    // Returns true operation can proceed; false if transaction should abort.
    pub fn insert_and_check(&self, from_node: WeakNode, rw_edge: bool) -> bool {
        let this_node: WeakNode =
            Arc::downgrade(&self.this_node.get().unwrap().borrow().as_ref().unwrap());

        if this_node.ptr_eq(&from_node) {
            return true; // self edge
        }

        let this_node: ArcNode = this_node.upgrade().unwrap();
        let this_node_rlock = this_node.read().unwrap();
        let exists = this_node_rlock.incoming_edge_exists(from_node.clone());

        loop {
            if !exists {
                let from_node = from_node.upgrade().unwrap();
                let from_node_rlock = from_node.read().unwrap();
                if from_node_rlock.is_aborted() && !rw_edge {
                    from_node_rlock.set_aborted();
                    drop(from_node_rlock);
                    drop(this_node_rlock);

                    return false; // cascading abort
                }

                if from_node_rlock.is_cleaned() {
                    drop(from_node_rlock);
                    drop(this_node_rlock);

                    return true; // from node cleaned
                }

                if from_node_rlock.is_checked() {
                    drop(from_node_rlock);

                    continue; // from node checked
                }

                this_node_rlock.insert_incoming(Arc::downgrade(&from_node), rw_edge); // insert edge
                from_node_rlock.insert_outgoing(Arc::downgrade(&this_node), rw_edge);
                drop(from_node_rlock);
                drop(this_node_rlock);

                let is_cycle = self.cycle_check(); // cycle check
                return !is_cycle;
            }
        }
    }

    pub fn cycle_check(&self) -> bool {
        true // TODO
    }

    pub fn needs_abort(&self) -> bool {
        assert!(self.this_node.get().is_some());
        assert!(
            self.this_node.get().unwrap().borrow().is_some(),
            "{:?}",
            self.this_node
        );

        let this_node = Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());
        let rlock = this_node.read().unwrap();
        let aborted = rlock.is_aborted();
        let cascading_abort = rlock.is_cascading_abort();
        drop(rlock);
        debug!("needs_abort: {}", aborted || cascading_abort);
        aborted || cascading_abort
    }

    pub fn is_committed(&self) -> bool {
        debug!("is_committed");
        let this_node = Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());
        let rlock = this_node.read().unwrap();
        let committed = rlock.is_committed();
        drop(rlock);
        committed
    }

    pub fn abort_procedure(&self) {
        debug!("abort_procedure");
        let this_node = Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());
        let rlock = this_node.read().unwrap();
        rlock.set_aborted();
        drop(rlock);

        self.cleanup();
    }

    pub fn check_committed(&self) -> bool {
        let this_node = Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());
        let rlock = this_node.read().unwrap();

        if rlock.is_aborted() || rlock.is_cascading_abort() {
            drop(rlock);
            return false;
        }

        rlock.set_checked(true);

        if rlock.is_incoming() {
            rlock.set_checked(false);
            drop(rlock);
            return false;
        }

        if rlock.is_aborted() || rlock.is_cascading_abort() {
            drop(rlock);
            return false;
        }

        let success = self.erase_graph_constraints();

        if success {
            self.cleanup();
        }

        drop(rlock);
        debug!("check_comitted: {}", success);
        return success;
    }

    pub fn erase_graph_constraints(&self) -> bool {
        let this_node = Arc::clone(&self.this_node.get().unwrap().borrow().as_ref().unwrap());
        let rlock = this_node.read().unwrap();

        let is_cycle = self.cycle_check();

        if is_cycle {
            rlock.set_aborted();
            drop(rlock);
            return false;
        }

        rlock.set_committed();

        true
    }
}

impl Scheduler for SerializationGraph {
    fn begin(&self) -> TransactionInfo {
        debug!("begin");

        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // inc txn ctr
        *self
            .txn_info
            .get_or(|| RefCell::new(Some(TransactionInformation::new())))
            .borrow_mut() = Some(TransactionInformation::new()); // init txn info

        self.eg
            .get_or(|| RefCell::new(EpochGuard::new(Arc::clone(&self.em))));

        let weak = self.create_node();

        assert!(self.txn_ctr.get().is_some(), "{:?}", self.txn_ctr);
        assert!(self.txn_info.get().is_some(), "{:?}", self.txn_info);
        assert!(self.this_node.get().is_some(), "{:?}", self.this_node);

        debug!("{}", self);

        TransactionInfo::SerializationGraph(weak)
    }

    fn read(
        &self,
        index_id: &str,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        debug!("read");
        if let TransactionInfo::SerializationGraph(_) = meta {
            if self.needs_abort() {
                return Err(self.abort(meta));
            }

            let index = self.data.get_index(index_id).unwrap();
            let lsn = index.get_lsn(&key).unwrap();
            let rw_table = index.get_rw_table(&key).unwrap();

            let mut guard = rw_table.lock();
            let prv = guard.push_front(Access::Read(meta.clone())); // get prv
            debug!("push read to rwtable");
            drop(guard);

            loop {
                let i = lsn.get(); // current lsn
                if i == prv {
                    break; // break when my prv == lsn
                }
            }

            let guard = rw_table.lock();
            let snapshot: VecDeque<(u64, Access)> = guard.snapshot(); // get accesses
            drop(guard);

            let mut cyclic = false; // insert and check each access
            for (id, access) in snapshot {
                if id < prv {
                    match access {
                        Access::Write(txn_info) => match txn_info {
                            TransactionInfo::SerializationGraph(from_node) => {
                                if self.insert_and_check(from_node, false) {
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
                debug!("cycle found");
                let mut guard = rw_table.lock();
                guard.erase((prv, Access::Read(meta.clone()))); // remove from rw table
                drop(guard);
                lsn.replace(prv + 1); // increment to next operation
                return Err(self.abort(meta));
            }
            debug!("no cycle found");

            let row = match index.get_row(&key) {
                Ok(rh) => rh,
                Err(_) => {
                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Read(meta.clone()))); // remove from rw table
                    drop(guard);
                    lsn.replace(prv + 1); // increment to next operation

                    return Err(self.abort(meta));
                }
            };

            let mut guard = row.lock();
            let mut res = guard.get_values(columns).unwrap(); // do read
            drop(guard);
            let vals = res.get_values();

            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Read, key.clone(), index_id.to_string()); // record operation

            lsn.replace(prv + 1); // increment to next operation
            debug!("execute read");
            Ok(vals)
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Write operation.
    fn write(
        &self,
        index_id: &str,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: &dyn Fn(Option<Vec<Data>>, Option<&[Data]>) -> Result<Vec<Data>, NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<Option<Vec<Data>>, NonFatalError> {
        debug!("write");
        if let TransactionInfo::SerializationGraph(_) = meta {
            let index = self.data.get_index(index_id).unwrap(); // get index

            let mut prv;
            let mut lsn;
            loop {
                if self.needs_abort() {
                    return Err(self.abort(meta));
                }

                if let Err(_) = index.get_row(&key) {
                    return Err(self.abort(meta)); // abort -- row not found (TATP only)
                };

                lsn = index.get_lsn(&key).unwrap();

                let rw_table = index.get_rw_table(&key).unwrap();
                let mut guard = rw_table.lock();
                prv = guard.push_front(Access::Write(meta.clone()));
                drop(guard);

                loop {
                    let i = lsn.get(); // current operation number
                    if i == prv {
                        break; // if  current = previous then this transaction can execute
                    }
                }

                let guard = rw_table.lock();
                let snapshot: VecDeque<(u64, Access)> = guard.snapshot();
                drop(guard);

                let mut wait = false;
                let mut cyclic = false;
                for (id, access) in snapshot {
                    if id < prv {
                        match access {
                            Access::Write(from) => {
                                if let TransactionInfo::SerializationGraph(from_node) = from {
                                    let fm = from_node.upgrade().unwrap();
                                    let rlock = fm.read().unwrap();
                                    if !rlock.is_committed() {
                                        if self.insert_and_check(from_node, false) {
                                            cyclic = true;
                                            drop(rlock);
                                            break;
                                        }
                                        wait = true;
                                        drop(rlock);
                                        break;
                                    }
                                }
                            }
                            Access::Read(_) => {}
                        }
                    }
                }

                if cyclic {
                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                    drop(guard);
                    lsn.replace(prv + 1); // increment to next operation
                    return Err(self.abort(meta));
                }

                if wait {
                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                    drop(guard);
                    lsn.replace(prv + 1); // increment to next operation
                    continue;
                }
                break;
            }

            let rw_table = index.get_rw_table(&key).unwrap();
            let mut guard = rw_table.lock();
            prv = guard.push_front(Access::Write(meta.clone()));
            drop(guard);

            let guard = rw_table.lock();
            let snapshot: VecDeque<(u64, Access)> = guard.snapshot();
            drop(guard);

            let mut cyclic = false;
            for (id, access) in snapshot {
                if id < prv {
                    match access {
                        Access::Read(from) => {
                            if let TransactionInfo::SerializationGraph(from_node) = from {
                                if !self.insert_and_check(from_node, true) {
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
                let mut guard = rw_table.lock();
                guard.erase((prv, Access::Write(meta.clone()))); // remove from rw_table
                drop(guard);
                lsn.replace(prv + 1); // increment to next operation
                return Err(self.abort(meta));
            }

            let row = index.get_row(&key).unwrap();
            let mut guard = row.lock(); // lock row

            let current_values;
            if let Some(columns) = read {
                let mut res = guard.get_values(columns).unwrap();
                current_values = Some(res.get_values());
            } else {
                current_values = None;
            }

            let new_values = match f(current_values.clone(), params) {
                Ok(res) => res,

                Err(_) => {
                    drop(guard);

                    let mut guard = rw_table.lock();
                    guard.erase((prv, Access::Write(meta.clone())));
                    drop(guard);

                    lsn.replace(prv + 1);

                    return Err(self.abort(meta));
                }
            };

            guard.set_values(columns, &new_values).unwrap();

            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Write, key.clone(), index_id.to_string());

            lsn.replace(prv + 1);

            Ok(current_values)
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit operation.
    ///
    /// Loop, checking if transaction needs an abort, then checking if it can be committed.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        debug!("commit");
        loop {
            if self.needs_abort() {
                return Err(self.abort(meta));
            }

            if self.check_committed() {
                let ops = self
                    .txn_info
                    .get()
                    .unwrap()
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .get(); // get operations

                for op in ops {
                    let Operation {
                        op_type,
                        key,
                        index,
                    } = op;
                    let index = self.data.get_index(&index).unwrap(); // get handle to index
                    let rw_table = index.get_rw_table(&key).unwrap(); // get handle to rwtable

                    let mut guard = rw_table.lock();
                    match op_type {
                        OperationType::Read => {
                            guard.erase_all(Access::Read(meta.clone())); // remove access
                            drop(guard);
                        }
                        OperationType::Write => {
                            let row = index.get_row(&key).unwrap(); // get handle to row
                            let mut rguard = row.lock();
                            rguard.commit(); // revert write
                            drop(rguard);

                            guard.erase_all(Access::Write(meta.clone())); // remove access
                            drop(guard);
                        }
                    }
                }

                break;
            }
        }

        self.eg.get().unwrap().borrow_mut().unpin(); // unpin txn

        Ok(())
    }

    /// Abort operation.
    ///
    /// Call sg abort procedure then remove accesses and revert writes.
    fn abort(&self, meta: &TransactionInfo) -> NonFatalError {
        debug!("abort");
        let ops = self
            .txn_info
            .get()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .get(); // get operations

        self.abort_procedure(); // sg abort

        for op in ops {
            let Operation {
                op_type,
                key,
                index,
            } = op;

            let index = self.data.get_index(&index).unwrap(); // get handle to index

            let rw_table = index.get_rw_table(&key).unwrap(); // get handle to rwtable
            let mut guard = rw_table.lock();

            match op_type {
                OperationType::Read => {
                    guard.erase_all(Access::Read(meta.clone())); // remove access
                    drop(guard);
                }
                OperationType::Write => {
                    let row = index.get_row(&key).unwrap(); // get handle to row
                    let mut rguard = row.lock();
                    rguard.revert(); // revert write
                    drop(rguard);

                    guard.erase_all(Access::Read(meta.clone())); // remove access
                    drop(guard);
                }
            }
        }

        self.eg.get().unwrap().borrow_mut().unpin(); // unpin txn

        NonFatalError::NonSerializable
        // TODO: return the why
    }
}

impl fmt::Display for SerializationGraph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "this_node: {:?}",
            self.this_node.get().unwrap().borrow().as_ref().unwrap()
        )
        .unwrap();
        writeln!(f, "txn_ctr: {}", self.txn_ctr.get().unwrap().borrow()).unwrap();
        Ok(())
    }
}
