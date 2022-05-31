use crate::common::error::{NonFatalError, WaitHitError};
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::owh::transaction::{self, PredecessorUpon, Transaction, TransactionState};
use crate::scheduler::{Database, TransactionType};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Guard;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct OptimisedWaitHitTransactionTypes<'a> {
    transaction: ThreadLocal<RefCell<Option<&'a Transaction<'a>>>>,
    transaction_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    transaction_cnt: ThreadLocal<RefCell<u64>>,
}

impl<'a> OptimisedWaitHitTransactionTypes<'a> {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
    }

    pub fn new(size: usize) -> Self {
        info!("Initialise optimised wait hit with {} thread(s)", size);

        Self {
            transaction: ThreadLocal::new(),
            transaction_info: ThreadLocal::new(),
            transaction_cnt: ThreadLocal::new(),
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

        *self.transaction_cnt.get_or(|| RefCell::new(0)).borrow_mut() += 1;

        let transaction = Box::new(Transaction::new()); // create transaction on heap
        let id = transaction::to_usize(transaction); // get id
        let tref = transaction::from_usize(id); // convert to reference
        self.transaction
            .get_or(|| RefCell::new(None))
            .borrow_mut()
            .replace(tref); // replace local transaction reference

        let guard = epoch::pin(); // pin thread

        OptimisedWaitHitTransactionTypes::EG.with(|x| x.borrow_mut().replace(guard));

        TransactionId::OptimisticWaitHit(id)
    }

    pub fn read_value(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let transaction = self.get_transaction(); // transaction active on this thread
        let table: &Table = database.get_table(table_id); // handle to table
        let rw_table = table.get_rwtable(offset); // handle to rwtable
        let prv = rw_table.push_front(Access::Read(meta.clone())); // append access
        let lsn = table.get_lsn(offset); // handle to lsn

        spin(prv, lsn); // delay until prv == lsn

        let guard = &epoch::pin(); // pin thread
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

    pub fn write_value(
        &self,
        value: &mut Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let transaction: &'a Transaction<'a> = self.get_transaction(); // transaction active on this thread
        let table = database.get_table(table_id); // handle to table
        let rw_table = table.get_rwtable(offset); // handle to rwtable
        let prv = rw_table.push_front(Access::Write(meta.clone())); // append access
        let lsn = table.get_lsn(offset); // handle to lsn
        spin(prv, lsn); // delay until prv == lsn

        let tuple = table.get_tuple(column_id, offset); // handle to tuple
        let dirty = tuple.get().is_dirty();

        // if tuple is dirty then abort
        // else for each read in the rwtable, add a predecessor upon write to hit list
        if dirty {
            // ok to have state change under feet here
            rw_table.erase(prv); // remove from rwtable
            lsn.store(prv + 1, Ordering::Release); // update lsn
            self.abort(database); // abort this transaction
            return Err(NonFatalError::RowDirty("todo".to_string()));
        } else {
            let guard = &epoch::pin(); // pin thread
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

            self.record_operation(OperationType::Write, table_id, column_id, offset, prv); // record operation
            lsn.store(prv + 1, Ordering::Release); // update lsn

            Ok(())
        }
    }

    pub fn abort(&self, database: &Database) -> NonFatalError {
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
                    rwtable.erase(prv); // remove access
                }
                OperationType::Write => {
                    tuple.get().revert(); // commit

                    rwtable.erase(prv); // remove access
                }
            }
        }

        let this_ptr: *const Transaction<'a> = this;
        let this_usize = this_ptr as usize;
        let boxed_node = transaction::to_box(this_usize);

        let cnt = *self.transaction_cnt.get_or(|| RefCell::new(0)).borrow();

        OptimisedWaitHitTransactionTypes::EG.with(|x| unsafe {
            x.borrow().as_ref().unwrap().defer_unchecked(move || {
                drop(boxed_node);
            });

            if cnt % 128 == 0 {
                x.borrow().as_ref().unwrap().flush();
            }

            let guard = x.borrow_mut().take();
            drop(guard)
        });

        NonFatalError::NonSerializable // placeholder
    }

    /// Commit a transaction.
    pub fn commit(
        &self,
        database: &Database,
        transaction_type: TransactionType,
    ) -> Result<(), NonFatalError> {
        let transaction = self.get_transaction();

        // CHECK //
        if let TransactionState::Aborted = transaction.get_state() {
            self.abort(database);
            return Err(WaitHitError::Hit.into());
        }

        // HIT PHASE //
        match transaction_type {
            TransactionType::WriteOnly | TransactionType::ReadWrite => {
                let hit_list = transaction.get_predecessors(PredecessorUpon::Write);
                for predecessor in &hit_list {
                    // if active then hit
                    if let TransactionState::Active = predecessor.get_state() {
                        predecessor.set_state(TransactionState::Aborted);
                    }
                }
            }
            _ => {}
        }

        // CHECK //
        if let TransactionState::Aborted = transaction.get_state() {
            self.abort(database);
            return Err(WaitHitError::Hit.into());
        }

        // WAIT PHASE //
        match transaction_type {
            TransactionType::ReadOnly | TransactionType::ReadWrite => {
                let wait_list = transaction.get_predecessors(PredecessorUpon::Read);
                for predecessor in &wait_list {
                    // if active or aborted then abort
                    match predecessor.get_state() {
                        TransactionState::Active => {
                            self.abort(database);
                            return Err(WaitHitError::PredecessorActive.into());
                        }
                        TransactionState::Aborted => {
                            self.abort(database);
                            return Err(WaitHitError::PredecessorAborted.into());
                        }
                        TransactionState::Committed => {}
                    }
                }
            }
            _ => {}
        }

        // CHECK //
        if let TransactionState::Aborted = transaction.get_state() {
            self.abort(database);
            return Err(WaitHitError::Hit.into());
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
                            rwtable.erase(prv); // remove access
                        }
                        OperationType::Write => {
                            tuple.get().commit(); // commit
                            rwtable.erase(prv); // remove access
                        }
                    }
                }

                let this_ptr: *const Transaction<'a> = transaction;
                let this_usize = this_ptr as usize;
                let boxed_node = transaction::to_box(this_usize);

                let cnt = *self.transaction_cnt.get_or(|| RefCell::new(0)).borrow();

                OptimisedWaitHitTransactionTypes::EG.with(|x| unsafe {
                    x.borrow().as_ref().unwrap().defer_unchecked(move || {
                        drop(boxed_node);
                    });

                    if cnt % 128 == 0 {
                        x.borrow().as_ref().unwrap().flush();
                    }

                    let guard = x.borrow_mut().take();
                    drop(guard)
                });

                Ok(())
            }
            Err(e) => {
                self.abort(database);
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
