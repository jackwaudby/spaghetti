use crate::common::error::NonFatalError;
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::scheduler::wh::error::WaitHitError;
use crate::scheduler::wh::shared::Shared;
use crate::scheduler::wh::shared::TransactionOutcome;
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use crossbeam_epoch::Guard;
use rustc_hash::FxHashSet;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thread_local::ThreadLocal;
use tracing::info;

pub mod error;
pub mod shared;

#[derive(Debug)]
pub struct WaitHit {
    shared: Arc<Shared>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    pur: ThreadLocal<RefCell<FxHashSet<u64>>>,
    puw: ThreadLocal<RefCell<FxHashSet<u64>>>,
}

impl WaitHit {
    pub fn new(size: usize) -> Self {
        info!("Initialise wait hit with {} thread(s)", size);

        Self {
            shared: Arc::new(Shared::new()),
            txn_info: ThreadLocal::new(),
            pur: ThreadLocal::new(),
            puw: ThreadLocal::new(),
        }
    }

    pub fn get_operations(&self) -> Vec<Operation> {
        let mut txn_info = self.txn_info.get().unwrap().borrow_mut();
        txn_info.as_mut().unwrap().get()
    }

    pub fn record(
        &self,
        op_type: OperationType,
        table_id: usize,
        column_id: usize,
        offset: usize,
        prv: u64,
    ) {
        let mut txn_info = self.txn_info.get().unwrap().borrow_mut();
        let res = txn_info.as_mut().unwrap();
        res.add(op_type, table_id, column_id, offset, prv); // record operation
    }

    pub fn begin(&self) -> TransactionId {
        *self
            .txn_info
            .get_or(|| RefCell::new(Some(TransactionInformation::new())))
            .borrow_mut() = Some(TransactionInformation::new()); // clear txn information

        // clear predecessor lists
        self.pur
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut()
            .clear();
        self.puw
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut()
            .clear();

        // get id
        let id = self.shared.get_id();

        TransactionId::WaitHit(id)
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
        let this_id = match meta {
            TransactionId::WaitHit(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let table: &Table = database.get_table(table_id); // get table
        let rw_table = table.get_rwtable(offset); // get rwtable
        let prv = rw_table.push_front(Access::Read(meta.clone()), guard); // append access
        let lsn = table.get_lsn(offset);

        unsafe { spin(prv, lsn) };

        let snapshot = rw_table.iter(guard); // iterator over access history

        // for each write in the rwtable, add a predecessor upon read to wait list
        for (id, access) in snapshot {
            if id < &prv {
                match access {
                    Access::Write(txn_info) => match txn_info {
                        TransactionId::WaitHit(pred_id) => {
                            if this_id == pred_id {
                                continue; // skip self predecessors
                            } else {
                                self.pur.get().unwrap().borrow_mut().insert(*pred_id);
                            }
                        }
                        _ => panic!("unexpected transaction information"),
                    },
                    Access::Read(_) => {}
                }
            }
        }

        let mut res = table
            .get_tuple(column_id, offset)
            .get()
            .get_value()
            .unwrap();

        lsn.store(prv + 1, Ordering::Release); // update lsn

        self.record(OperationType::Read, table_id, column_id, offset, prv); // record operation

        Ok(res.get_value())
    }

    /// Write operation.
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
        let this_id = match meta {
            TransactionId::WaitHit(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let prv = rw_table.push_front(Access::Write(meta.clone()), guard);

        unsafe { spin(prv, lsn) };

        let tuple = table.get_tuple(column_id, offset); // handle to tuple
        let dirty = tuple.get().is_dirty();

        // if tuple is dirty then abort
        // else for each read in the rwtable, add a predecessor upon write to hit list
        if dirty {
            // ok to have state change under feet here
            rw_table.erase(prv, guard); // remove from rwtable
            lsn.store(prv + 1, Ordering::Release); // update lsn
            self.abort(meta, database, guard); // abort this transaction
            return Err(NonFatalError::RowDirty("todo".to_string()));
        } else {
            let snapshot = rw_table.iter(guard); // iterator over rwtable

            for (id, access) in snapshot {
                if id < &prv {
                    match access {
                        Access::Read(txn_info) => match txn_info {
                            TransactionId::WaitHit(pred_id) => {
                                if this_id == pred_id {
                                    continue; // skip self predecessors
                                } else {
                                    self.puw.get().unwrap().borrow_mut().insert(*pred_id);
                                }
                            }
                            _ => panic!("unexpected transaction information"),
                        },
                        Access::Write(txn_info) => match txn_info {
                            TransactionId::WaitHit(pred_id) => {
                                if this_id == pred_id {
                                    continue; // skip self predecessors
                                } else {
                                    self.puw.get().unwrap().borrow_mut().insert(*pred_id);
                                }
                            }

                            _ => panic!("unexpected transaction information"),
                        },
                    }
                }
            }
        }

        table
            .get_tuple(column_id, offset)
            .get()
            .set_value(value)
            .unwrap();

        lsn.store(prv + 1, Ordering::Release); // update lsn

        self.record(OperationType::Write, table_id, column_id, offset, prv); // record operation

        Ok(())
    }

    /// Commit operation.
    /// Wait phase: for each pur; if terminated and aborted then abort; if active then abort; else committed then continue
    /// Hit phase: if not in hit list then commit and merge all (active) puw into the hit list; else in hit list then abort
    pub fn commit<'g>(
        &self,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> Result<(), NonFatalError> {
        let this_id = match meta {
            TransactionId::WaitHit(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let mut g = self.shared.get_lock();

        let pur = self.pur.get().unwrap().borrow().clone(); // TODO: avoid clone

        for pred in pur {
            if g.has_terminated(pred) {
                if g.get_terminated_outcome(pred) == TransactionOutcome::Aborted {
                    drop(g);
                    self.abort(meta, database, guard);
                    return Err(WaitHitError::PredecessorAborted(*this_id).into());
                }
            } else {
                drop(g);
                self.abort(meta, database, guard);
                return Err(WaitHitError::PredecessorActive(*this_id).into());
            }
        }

        if !g.is_in_hit_list(*this_id) {
            self.tidyup(database, guard, true);

            let puw = self.puw.get().unwrap().borrow().clone(); // TODO: avoid clone

            for pred in puw {
                if !g.has_terminated(pred) {
                    g.add_to_hit_list(pred);
                }
            }
            drop(g);
            Ok(())
        } else {
            drop(g);
            self.abort(meta, database, guard);
            Err(WaitHitError::TransactionInHitList(*this_id).into())
        }
    }

    /// Abort operation.
    /// Remove transaction from the hit list and add to the terminated list.
    pub fn abort<'g>(
        &self,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> NonFatalError {
        let this_id = match meta {
            TransactionId::WaitHit(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let mut g = self.shared.get_lock();

        g.remove_from_hit_list(*this_id);
        g.add_to_terminated_list(*this_id, TransactionOutcome::Aborted);

        self.tidyup(database, guard, false);

        NonFatalError::NonSerializable // TODO: placeholder
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
                    rwtable.erase(prv, guard);
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
