use crate::common::error::NonFatalError;
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use crossbeam_epoch::Guard;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct NoConcurrencyControl {
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

impl NoConcurrencyControl {
    pub fn new(size: usize) -> Self {
        info!("No concurrency control: {} core(s)", size);
        Self {
            txn_info: ThreadLocal::new(),
        }
    }

    pub fn begin(&self) -> TransactionId {
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new());

        TransactionId::NoConcurrencyControl
    }

    pub fn read_value<'g>(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
        _guard: &'g Guard,
    ) -> Result<Data, NonFatalError> {
        if let TransactionId::NoConcurrencyControl = meta {
            let table: &Table = database.get_table(table_id); // get table
            let rw_table = table.get_rwtable(offset); // get rwtable
            let prv = rw_table.push_front(Access::Read(meta.clone())); // append access
            let lsn = table.get_lsn(offset);

            spin(prv, lsn);

            let vals = table
                .get_tuple(column_id, offset)
                .get()
                .get_value()
                .unwrap()
                .get_value(); // read

            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Read, table_id, column_id, offset, prv); // record operation
            lsn.store(prv + 1, Ordering::Release); // update lsn

            Ok(vals)
        } else {
            panic!("unexpected transaction info");
        }
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
        _guard: &'g Guard,
    ) -> Result<(), NonFatalError> {
        if let TransactionId::NoConcurrencyControl = meta {
            let table = database.get_table(table_id);
            let rw_table = table.get_rwtable(offset);
            let lsn = table.get_lsn(offset);
            let prv = rw_table.push_front(Access::Write(meta.clone()));
            spin(prv, lsn);

            let tuple = table.get_tuple(column_id, offset).get(); // get tuple
            tuple.set_value(value).unwrap(); // set value
            tuple.commit(); // commit; operations never fail

            self.txn_info
                .get()
                .unwrap()
                .borrow_mut()
                .as_mut()
                .unwrap()
                .add(OperationType::Write, table_id, column_id, offset, prv); // record operation

            lsn.store(prv + 1, Ordering::Release); // update lsn

            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit operation.
    pub fn commit<'g>(&self, database: &Database, _guard: &'g Guard) -> Result<(), NonFatalError> {
        let ops = self
            .txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get(); // get operations

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

        Ok(())
    }

    /// Abort operation.
    pub fn abort<'g>(&self, database: &Database, _guard: &'g Guard) -> NonFatalError {
        let ops = self
            .txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get(); // get operations

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
