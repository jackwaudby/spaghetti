use crate::common::error::{AttendezError, NonFatalError};
use crate::common::statistics::protocol_diagnostics::{AttendezDiagnostics, ProtocolDiagnostics};
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::common::utils;
use crate::scheduler::attendez::predecessor_summary::scan_predecessors;
use crate::scheduler::attendez::transaction::{PredecessorSet, Transaction, TransactionState};
use crate::scheduler::Database;
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Guard;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::info;

pub mod transaction;

pub mod predecessor_summary;

#[derive(Debug)]
pub struct Attendez<'a> {
    transaction: ThreadLocal<RefCell<Option<&'a Transaction<'a>>>>,
    transaction_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    transaction_cnt: ThreadLocal<RefCell<u64>>,
    a: u64,
    b: u64,
    watermark: u64,
    no_wait_write: bool,
    delta: u64,
}

impl<'a> Attendez<'a> {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
    }

    pub fn new(
        size: usize,
        watermark: u64,
        a: u64,
        b: u64,
        no_wait_write: bool,
        delta: u64,
    ) -> Self {
        info!("Initialise attendez scheduler with {} thread(s)", size);
        info!(
            "delta = {}, watermark = {}, a = {}, b = {}",
            delta, watermark, a, b
        );
        info!("No wait write operation: {}", no_wait_write);

        Self {
            transaction: ThreadLocal::new(),
            transaction_info: ThreadLocal::new(),
            transaction_cnt: ThreadLocal::new(),
            a,
            b,
            watermark,
            no_wait_write,
            delta,
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
            .add(op_type, table_id, column_id, offset, prv);
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

        Attendez::EG.with(|x| x.borrow_mut().replace(guard));

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

        utils::spin(prv, lsn); // delay until prv == lsn

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
                                transaction.add_predecessor(pred, false);
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
        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);

        let mut ticket;
        if self.no_wait_write {
            ticket = rw_table.push_front(Access::Write(meta.clone()));
            utils::spin(ticket, lsn);
            let tuple = table.get_tuple(column_id, offset); // handle to tuple
            let dirty = tuple.get().is_dirty();

            // if tuple is dirty then abort
            //  else for each read in the rwtable, add a predecessor upon write to hit list
            if dirty {
                // ok to have state change under feet here
                rw_table.erase(ticket); // remove from rwtable
                lsn.store(ticket + 1, Ordering::Release); // update lsn
                self.abort(database); // abort this transaction
                return Err(NonFatalError::RowDirty("todo".to_string()));
            }
        } else {
            let mut prv_ticket = 0;
            let mut window = 0;
            let mut prv_window = 0;
            let watermark = 10;
            let mut delta = 5;
            let mut attempt = 0;
            let mut diff: i64 = 0;
            loop {
                if delta >= watermark {
                    self.abort(database);
                    return Err(NonFatalError::RowDirty("todo".to_string()));
                }

                ticket = rw_table.push_front(Access::Write(meta.clone()));

                if attempt > 0 {
                    window = ticket - prv_ticket;
                }

                if attempt > 1 {
                    diff = window as i64 - prv_window as i64;
                }

                if diff < 0 {
                    delta = delta / 2;
                } else {
                    delta += 1;
                }

                utils::spin(ticket, lsn);

                let tuple = table.get_tuple(column_id, offset);
                let dirty = tuple.get().is_dirty();

                // if tuple is dirty then backoff
                if dirty {
                    rw_table.erase(ticket); // remove from rwtable
                    lsn.store(ticket + 1, Ordering::Release); // update lsn
                    prv_ticket = ticket;
                    prv_window = window;
                    attempt += 1;
                    continue;
                } else {
                    // else for each read in the rwtable, add a predecessor upon write to hit list
                    break;
                }
            }
        }

        let guard = &epoch::pin(); // pin thread
        let snapshot = rw_table.iter(guard); // iterator over rwtable

        for (id, access) in snapshot {
            if id < &ticket {
                match access {
                    Access::Read(txn_info) => match txn_info {
                        TransactionId::OptimisticWaitHit(pred_addr) => {
                            let pred = transaction::from_usize(*pred_addr); // convert to ptr
                            if std::ptr::eq(transaction, pred) {
                                continue;
                            } else {
                                transaction.add_predecessor(pred, true);
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
                                transaction.add_predecessor(pred, false);
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

        self.record_operation(OperationType::Write, table_id, column_id, offset, ticket); // record operation
        lsn.store(ticket + 1, Ordering::Release); // update lsn

        Ok(())
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

        Attendez::EG.with(|x| unsafe {
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

    pub fn commit<'g>(&self, database: &Database) -> Result<ProtocolDiagnostics, NonFatalError> {
        let transaction = self.get_transaction();
        let predecessors = transaction.get_predecessors();
        let num_predecessors = predecessors.len();
        let mut delta = self.delta;

        loop {
            // waited too long -- might be cycle
            if delta >= self.watermark {
                self.abort(database);
                return Err(AttendezError::ExceededWatermark.into());
            }

            // wait cycle
            let summary = scan_predecessors(&predecessors);
            let start_committed = summary.get_committed();

            // all committed
            if start_committed == num_predecessors as u64 {
                break;
            }

            let outcome = wait_cycle(delta, start_committed, &predecessors);

            match outcome {
                WaitOutcome::Abort => {
                    self.abort(database);
                    return Err(AttendezError::PredecessorAborted.into());
                }
                WaitOutcome::Change => {
                    delta = delta / self.b;
                }
                WaitOutcome::NoChange => {
                    delta += self.a;
                }
            }
        }

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

                Attendez::EG.with(|x| unsafe {
                    x.borrow().as_ref().unwrap().defer_unchecked(move || {
                        drop(boxed_node);
                    });

                    if cnt % 128 == 0 {
                        x.borrow().as_ref().unwrap().flush();
                    }

                    let guard = x.borrow_mut().take();
                    drop(guard)
                });

                Ok(ProtocolDiagnostics::Attendez(AttendezDiagnostics::new(
                    num_predecessors,
                )))
            }
            Err(e) => {
                self.abort(database);
                Err(e)
            }
        }
    }
}

fn wait_cycle<'a>(
    delta: u64,
    start_committed: u64,
    predecessors: &PredecessorSet<'a>,
) -> WaitOutcome {
    for _ in 0..delta {
        let summary = scan_predecessors(predecessors);

        if summary.has_aborted_predecessor() {
            return WaitOutcome::Abort;
        }

        if start_committed < summary.get_committed() {
            return WaitOutcome::Change;
        }
    }

    WaitOutcome::NoChange
}

enum WaitOutcome {
    Change,
    NoChange,
    Abort,
}
