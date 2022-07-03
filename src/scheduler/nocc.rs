use crate::common::error::NonFatalError;
use crate::common::error::SerializationGraphError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::common::Edge;
use crate::scheduler::common::Node;
use crate::scheduler::StatsBucket;
use crate::scheduler::ValueId;
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Guard;
use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct NoConcurrencyControl {
    txn_info: ThreadLocal<RefCell<TransactionInformation>>,
    txn_ctr: ThreadLocal<RefCell<usize>>,
    visited: ThreadLocal<RefCell<FxHashSet<usize>>>,
    stack: ThreadLocal<RefCell<Vec<Edge>>>,
}

impl NoConcurrencyControl {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
        static NODE: RefCell<Option<*mut Node>> = RefCell::new(None);

    }

    pub fn get_transaction(&self) -> *mut Node {
        NoConcurrencyControl::NODE.with(|x| *x.borrow().as_ref().unwrap())
    }

    /// Create a scheduler with no concurrency control mechanism.
    pub fn new(size: usize) -> Self {
        info!("No concurrency control: {} core(s)", size);

        Self {
            txn_ctr: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
            visited: ThreadLocal::new(),
            stack: ThreadLocal::new(),
        }
    }

    /// Begin a transaction.
    pub fn begin(&self) -> TransactionId {
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1; // increment txn ctr

        *self
            .txn_info
            .get_or(|| RefCell::new(TransactionInformation::new()))
            .borrow_mut() = TransactionInformation::new();

        let thread_id: usize = std::thread::current().name().unwrap().parse().unwrap();
        let thread_ctr = *self.txn_ctr.get().unwrap().borrow();
        let incoming = Mutex::new(FxHashSet::default());
        let outgoing = Mutex::new(FxHashSet::default());
        let n = Node::new(thread_id, thread_ctr, incoming, outgoing, None);
        let node = Box::new(n); // allocate node
        let ptr: *mut Node = Box::into_raw(node); // convert to raw ptr
        let id = ptr as usize; // get id
        unsafe { (*ptr).set_id(id) }; // set id on node

        NoConcurrencyControl::NODE.with(|x| x.borrow_mut().replace(ptr)); // store in thread local

        let guard = epoch::pin(); // pin thread

        NoConcurrencyControl::EG.with(|x| x.borrow_mut().replace(guard));

        TransactionId::NoConcurrencyControl
    }

    /// Insert an edge: (from) --> (this)
    pub fn insert_and_check(&self, from: Edge) -> bool {
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

    /// Read a value in a column at some offset.
    pub fn read_value(
        &self,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();
        let table: &Table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let prv = rw_table.push_front(Access::Read(meta.get_transaction_id()));
        let lsn = table.get_lsn(offset);

        spin(prv, lsn);

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
                        if let TransactionId::SerializationGraph(from_id, _, _) = from {
                            let from = unsafe { &*(*from_id as *const Node) };
                            // if !self.insert_and_check(Edge::WriteRead(*from_id)) {
                            //     cyclic = true;
                            //     break;
                            // }
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

        let tuple = table.get_tuple(column_id, offset).get();
        let value = tuple.get_value().unwrap().get_value();

        lsn.store(prv + 1, Ordering::Release);

        self.txn_info.get().unwrap().borrow_mut().add(
            OperationType::Read,
            table_id,
            column_id,
            offset,
            prv,
        );

        Ok(value)
    }

    /// Write a value in a column at some offset.
    ///
    /// Note, operations effects are committed immediately.
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
        let table = database.get_table(table_id); // index into a vector
        let rw_table = table.get_rwtable(offset); // index into a vector
        let lsn = table.get_lsn(offset); // index into a vector
        let prv = rw_table.push_front(Access::Write(meta.get_transaction_id()));

        spin(prv, lsn);

        let tuple = table.get_tuple(column_id, offset).get(); // index into a vector
        tuple.set_value(value).unwrap();
        tuple.commit();

        lsn.store(prv + 1, Ordering::Release);

        self.txn_info.get().unwrap().borrow_mut().add(
            OperationType::Write,
            table_id,
            column_id,
            offset,
            prv,
        );

        Ok(())
    }

    /// Remove accesses from access history.
    fn tidy_up(&self, database: &Database) {
        let ops = self.txn_info.get().unwrap().borrow_mut().get();

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

    /// Cleanup node after committed or aborted.
    pub fn cleanup(&self) {
        let this = self.get_transaction();
        let cnt = *self.txn_ctr.get_or(|| RefCell::new(0)).borrow();

        NoConcurrencyControl::EG.with(|x| unsafe {
            x.borrow().as_ref().unwrap().defer_unchecked(move || {
                let boxed_node = Box::from_raw(this); // garbage collect
                drop(boxed_node);
            });

            if cnt % 64 == 0 {
                x.borrow().as_ref().unwrap().flush();
            }

            let guard = x.borrow_mut().take();
            drop(guard)
        });
    }

    /// Commit a transaction.
    pub fn commit(
        &self,
        _meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        self.cleanup();

        self.tidy_up(database);

        Ok(())
    }

    /// Abort a transaction.
    pub fn abort(&self, _meta: &mut StatsBucket, database: &Database) -> NonFatalError {
        self.cleanup();
        self.tidy_up(database);

        NonFatalError::NoccError
    }
}

/// Busy wait until operation ticket matches row ticket.
fn spin(prv: u64, lsn: &AtomicU64) {
    let mut i = 0;

    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }
    }
}
