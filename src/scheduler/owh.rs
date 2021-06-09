use crate::common::error::NonFatalError;
use crate::scheduler::owh::error::OptimisedWaitHitError;
use crate::scheduler::owh::transaction::{PredecessorUpon, Transaction, TransactionState};
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::scheduler::Database;
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;

use crossbeam_epoch::Guard;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use thread_local::ThreadLocal;
use tracing::info;

pub mod error;

pub mod transaction;

#[derive(Debug)]
pub struct OptimisedWaitHit<'a> {
    transaction: ThreadLocal<RefCell<Option<&'a Transaction<'a>>>>,
    transaction_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

impl<'a> OptimisedWaitHit<'a> {
    pub fn new(size: usize) -> Self {
        info!("Initialise optimised wait hit with {} thread(s)", size);

        Self {
            transaction: ThreadLocal::new(),
            transaction_info: ThreadLocal::new(),
        }
    }

    pub fn get_transaction(&self) -> &'a Transaction<'a> {
        self.transaction
            .get()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .clone()
    }

    pub fn record_operation(
        &self,
        op_type: OperationType,
        table_id: usize,
        column_id: usize,
        offset: usize,
        prv: u64,
    ) {
        self.transaction_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .add(op_type, table_id, column_id, offset, prv); // record operation
    }

    pub fn get_operations(&self) -> Vec<Operation> {
        self.transaction_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get()
    }

    pub fn begin(&self) -> TransactionId {
        *self
            .transaction_info
            .get_or(|| RefCell::new(None))
            .borrow_mut() = Some(TransactionInformation::new()); // reset txn info

        let transaction = Box::new(Transaction::new()); // create transaction on heap
        let id = transaction::to_usize(transaction); // get id
        let tref = transaction::from_usize(id); // convert to reference
        self.transaction
            .get_or(|| RefCell::new(None))
            .borrow_mut()
            .replace(tref); // replace local transaction reference

        TransactionId::OptimisticWaitHit(id)
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
        let transaction = self.get_transaction(); // transaction active on this thread
        let table: &Table = database.get_table(table_id); // handle to table
        let rw_table = table.get_rwtable(offset); // handle to rwtable
        let prv = rw_table.push_front(Access::Read(meta.clone()), guard); // append access
        let lsn = table.get_lsn(offset); // handle to lsn
        spin(prv, lsn); // delay until prv == lsn
        let snapshot = rw_table.iter(guard); // iterator over access history

        // for each write in the rwtable, add a predecessor upon read to wait list
        for (id, access) in snapshot {
            if id < &prv {
                match access {
                    Access::Write(txn_info) => match txn_info {
                        TransactionId::OptimisticWaitHit(pred_addr) => {
                            let pred = transaction::from_usize(*pred_addr); // convert to ptr

                            if std::ptr::eq(transaction, pred) {
                                continue; // skip self predecessors
                            } else {
                                transaction.add_predecessor(pred, PredecessorUpon::Read);
                            }
                        }
                        _ => panic!("unexpected transaction information"),
                    },
                    Access::Read(_) => {}
                }
            }
        }

        let vals = table
            .get_tuple(column_id, offset)
            .get()
            .get_value()
            .unwrap()
            .get_value(); // read value
        self.record_operation(OperationType::Read, table_id, column_id, offset, prv); // record operation
        lsn.store(prv + 1, Ordering::Release); // update lsn

        Ok(vals)
    }

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
        let transaction: &'a Transaction<'a> = self.get_transaction(); // transaction active on this thread
        let table = database.get_table(table_id); // handle to table
        let rw_table = table.get_rwtable(offset); // handle to rwtable
        let prv = rw_table.push_front(Access::Write(meta.clone()), guard); // append access
        let lsn = table.get_lsn(offset); // handle to lsn
        spin(prv, lsn); // delay until prv == lsn

        let tuple = table.get_tuple(column_id, offset); // handle to tuple
        let dirty = tuple.get().is_dirty();

        // if tuple is dirty then abort
        // else for each read in the rwtable, add a predecessor upon write to hit list
        if dirty {
            assert!(tuple.get().prev.is_some());

            rw_table.erase(prv, guard); // remove from rwtable
            lsn.store(prv + 1, Ordering::Release); // update lsn
            self.abort(database, guard); // abort this transaction
            return Err(NonFatalError::RowDirty);
        } else {
            assert!(tuple.get().prev.is_none());

            let snapshot = rw_table.iter(guard); // iterator over rwtable
            for (id, access) in snapshot {
                if id < &prv {
                    match access {
                        Access::Read(txn_info) => match txn_info {
                            TransactionId::OptimisticWaitHit(pred_addr) => {
                                let pred = transaction::from_usize(*pred_addr); // convert to ptr
                                if std::ptr::eq(transaction, pred) {
                                    continue;
                                } else {
                                    transaction.add_predecessor(pred, PredecessorUpon::Write);
                                }
                            }
                            _ => panic!("unexpected transaction information"),
                        },
                        Access::Write(txn_info) => match txn_info {
                            TransactionId::OptimisticWaitHit(pred_addr) => {
                                let pred: &'a Transaction<'a> = transaction::from_usize(*pred_addr); // convert to ptr
                                if std::ptr::eq(transaction, pred) {
                                    continue;
                                } else {
                                    transaction.add_predecessor(pred, PredecessorUpon::Write);
                                }
                            }
                            _ => panic!("unexpected transaction information"),
                        },
                    }
                }
            }

            table
                .get_tuple(column_id, offset)
                .get()
                .set_value(value)
                .unwrap();

            assert!(tuple.get().prev.is_some());

            self.record_operation(OperationType::Write, table_id, column_id, offset, prv); // record operation
            lsn.store(prv + 1, Ordering::Release); // update lsn

            Ok(())
        }
    }

    pub fn abort<'g>(&self, database: &Database, guard: &'g Guard) -> NonFatalError {
        let this = self.get_transaction();
        let ops = self.get_operations();

        this.set_state(TransactionState::Aborted); // set state

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
                    rwtable.erase(prv, guard); // remove access
                }
                OperationType::Write => {
                    assert!(tuple.get().prev.is_some());
                    tuple.get().revert(); // revert
                    assert!(tuple.get().prev.is_none());
                    rwtable.erase(prv, guard); // remove access
                }
            }
        }

        let this_ptr: *const Transaction<'a> = this;
        let this_usize = this_ptr as usize;
        let boxed_node = transaction::to_box(this_usize);

        unsafe {
            guard.defer_unchecked(move || {
                drop(boxed_node);
            });
        }

        NonFatalError::NonSerializable // placeholder
    }

    /// Commit a transaction.
    pub fn commit<'g>(&self, database: &Database, guard: &'g Guard) -> Result<(), NonFatalError> {
        let transaction = self.get_transaction();

        // CHECK //
        if let TransactionState::Aborted = transaction.get_state() {
            self.abort(database, guard);
            return Err(OptimisedWaitHitError::Hit.into());
        }

        // HIT PHASE //
        let hit_list = transaction.get_predecessors(PredecessorUpon::Write);
        for predecessor in &hit_list {
            // if active then hit
            if let TransactionState::Active = predecessor.get_state() {
                predecessor.set_state(TransactionState::Aborted);
            }
        }

        // CHECK //
        if let TransactionState::Aborted = transaction.get_state() {
            self.abort(database, guard);
            return Err(OptimisedWaitHitError::Hit.into());
        }

        // WAIT PHASE //
        let wait_list = transaction.get_predecessors(PredecessorUpon::Read);
        for predecessor in &wait_list {
            // if active or aborted then abort
            match predecessor.get_state() {
                TransactionState::Active => {
                    self.abort(database, guard);
                    return Err(OptimisedWaitHitError::PredecessorActive.into());
                }
                TransactionState::Aborted => {
                    self.abort(database, guard);
                    return Err(OptimisedWaitHitError::PredecessorAborted.into());
                }
                TransactionState::Committed => {}
            }
        }

        // CHECK //
        if let TransactionState::Aborted = transaction.get_state() {
            self.abort(database, guard);
            return Err(OptimisedWaitHitError::Hit.into());
        }

        // TRY COMMIT //
        let outcome = transaction.try_commit();
        match outcome {
            Ok(_) => {
                // commit changes
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
                    let tuple = table.get_tuple(column_id, offset);
                    let rwtable = table.get_rwtable(offset);

                    match op_type {
                        OperationType::Read => {
                            rwtable.erase(prv, guard); // remove access
                        }
                        OperationType::Write => {
                            assert!(tuple.get().prev.is_some());

                            tuple.get().commit(); // revert
                            assert!(tuple.get().prev.is_none());
                            rwtable.erase(prv, guard); // remove access
                        }
                    }
                }

                let this_ptr: *const Transaction<'a> = transaction;
                let this_usize = this_ptr as usize;
                let boxed_node = transaction::to_box(this_usize);

                unsafe {
                    guard.defer_unchecked(move || {
                        drop(boxed_node);
                    });
                }

                Ok(())
            }
            Err(e) => {
                self.abort(database, guard);
                Err(e)
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
