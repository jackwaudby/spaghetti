use crate::common::error::NonFatalError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Guard;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct NoConcurrencyControl {
    txn_info: ThreadLocal<RefCell<TransactionInformation>>,
}

impl NoConcurrencyControl {
    thread_local! {
        static EG: RefCell<Option<Guard>> = RefCell::new(None);
    }

    pub fn new(size: usize) -> Self {
        info!("No concurrency control: {} core(s)", size);
        Self {
            txn_info: ThreadLocal::new(),
        }
    }

    pub fn begin(&self) -> TransactionId {
        *self
            .txn_info
            .get_or(|| RefCell::new(TransactionInformation::new()))
            .borrow_mut() = TransactionInformation::new();

        let guard = epoch::pin(); // pin thread
        NoConcurrencyControl::EG.with(|x| x.borrow_mut().replace(guard));

        TransactionId::NoConcurrencyControl
    }

    pub fn read_value<'g>(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        if let TransactionId::NoConcurrencyControl = meta {
            let table: &Table = database.get_table(table_id); // get table
            let rw_table = table.get_rwtable(offset); // get rwtable
            let prv = rw_table.push_front(Access::Read(meta.clone())); // append access
            let lsn = table.get_lsn(offset);

            spin(prv, lsn);

            let guard = &epoch::pin(); // pin thread
            let snapshot = rw_table.iter(guard); // iterator over access history

            for (id, access) in snapshot {
                2 * 2;
            }

            let vals = table
                .get_tuple(column_id, offset)
                .get()
                .get_value()
                .unwrap()
                .get_value(); // read

            lsn.store(prv + 1, Ordering::Release); // update lsn

            self.txn_info.get().unwrap().borrow_mut().add(
                OperationType::Read,
                table_id,
                column_id,
                offset,
                prv,
            ); // record operation

            Ok(vals)
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Write operation.
    pub fn write_value<'g>(
        &self,
        value: &mut Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        if let TransactionId::NoConcurrencyControl = meta {
            let table = database.get_table(table_id);
            let rw_table = table.get_rwtable(offset);
            let lsn = table.get_lsn(offset);
            let prv = rw_table.push_front(Access::Write(meta.clone()));

            spin(prv, lsn);

            let guard = &epoch::pin(); // pin thread
            let snapshot = rw_table.iter(guard); // iterator over access history

            for (id, access) in snapshot {
                2 * 2;
            }

            let tuple = table.get_tuple(column_id, offset).get(); // get tuple
            tuple.set_value(value).unwrap(); // set value
            tuple.commit(); // commit; operations never fail

            lsn.store(prv + 1, Ordering::Release); // update lsn

            self.txn_info.get().unwrap().borrow_mut().add(
                OperationType::Write,
                table_id,
                column_id,
                offset,
                prv,
            ); // record operation

            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit operation.
    pub fn commit<'g>(&self, database: &Database) -> Result<(), NonFatalError> {
        let ops = self.txn_info.get().unwrap().borrow_mut().get(); // get operations

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
                    rwtable.erase(prv); // remove access
                }
                OperationType::Write => {
                    rwtable.erase(prv); // remove access
                }
            }
        }
        NoConcurrencyControl::EG.with(|x| {
            let guard = x.borrow_mut().take();
            drop(guard)
        });
        Ok(())
    }

    /// Abort operation.
    pub fn abort<'g>(&self, database: &Database) -> NonFatalError {
        let ops = self.txn_info.get().unwrap().borrow_mut().get(); // get operations

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
                    rwtable.erase(prv); // remove access
                }
                OperationType::Write => {
                    rwtable.erase(prv); // remove access
                }
            }

            NoConcurrencyControl::EG.with(|x| {
                let guard = x.borrow_mut().take();
                drop(guard)
            });
        }

        NonFatalError::NonSerializable
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
