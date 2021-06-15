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
use tracing::{debug, info};

#[derive(Debug)]
pub struct NoConcurrencyControl {
    txn_info: ThreadLocal<RefCell<TransactionInformation>>,
}

impl NoConcurrencyControl {
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

        TransactionId::NoConcurrencyControl
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
        debug!("read");
        let table: &Table = database.get_table(table_id); // get table
        let rw_table = table.get_rwtable(offset); // get rwtable
        let prv = rw_table.push_front(Access::Read(meta.clone()), guard); // append access
        let lsn = table.get_lsn(offset);

        spin(prv, lsn);

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
        debug!("write");

        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let prv = rw_table.push_front(Access::Write(meta.clone()), guard);

        debug!("write - start spin");
        spin(prv, lsn);
        debug!("write - spin complete");

        let tuple = table.get_tuple(column_id, offset).get(); // get tuple
        tuple.set_value(value, prv, meta.clone()).unwrap(); // set value
        tuple.commit(); // commit; operations never fail

        debug!("write - update lsn");
        lsn.store(prv + 1, Ordering::Release); // update lsn
        debug!("write - updated lsn");

        self.txn_info.get().unwrap().borrow_mut().add(
            OperationType::Write,
            table_id,
            column_id,
            offset,
            prv,
        ); // record operation

        debug!("write - complete");
        Ok(())
    }

    /// Commit operation.
    pub fn commit<'g>(&self, database: &Database, guard: &'g Guard) -> Result<(), NonFatalError> {
        debug!("commit");
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
                    debug!("start erase");
                    rwtable.erase(prv, guard); // remove access
                    debug!("stop erase");
                }
                OperationType::Write => {
                    debug!("start erase");
                    rwtable.erase(prv, guard); // remove access
                    debug!("stop erase");
                }
            }
        }

        Ok(())
    }

    /// Abort operation.
    pub fn abort<'g>(&self, database: &Database, guard: &'g Guard) -> NonFatalError {
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
                    rwtable.erase(prv, guard); // remove access
                }
                OperationType::Write => {
                    rwtable.erase(prv, guard); // remove access
                }
            }
        }

        NonFatalError::NonSerializable
    }
}

fn spin(prv: u64, lsn: &AtomicU64) {
    debug!("prv: {}", prv);
    debug!("lsn: {}", lsn.load(Ordering::Relaxed));
    let mut i = 0;
    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }
    }
}
