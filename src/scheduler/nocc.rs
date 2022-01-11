use crate::common::error::NonFatalError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::storage::access::{Access, TransactionId};
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use std::cell::RefCell;
use std::convert::TryFrom;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct NoConcurrencyControl {
    txn_info: ThreadLocal<RefCell<TransactionInformation>>,
}

impl NoConcurrencyControl {
    /// Create a scheduler with no concurrency control mechanism.
    pub fn new(size: usize) -> Self {
        info!("No concurrency control: {} core(s)", size);

        Self {
            txn_info: ThreadLocal::new(),
        }
    }

    /// Begin a transaction.
    pub fn begin(&self) -> TransactionId {
        *self
            .txn_info
            .get_or(|| RefCell::new(TransactionInformation::new()))
            .borrow_mut() = TransactionInformation::new();

        TransactionId::NoConcurrencyControl
    }

    /// Read a value in a column at some offset.
    pub fn read_value(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        // let table: &Table = database.get_table(table_id);
        // let rw_table = table.get_rwtable(offset);
        // let prv = rw_table.push_front(Access::Read(meta.clone()));
        // let lsn = table.get_lsn(offset);

        // spin(prv, lsn);

        // let tuple = table.get_tuple(column_id, offset).get();
        // let value = tuple.get_value().unwrap().get_value();

        // lsn.store(prv + 1, Ordering::Release);

        // self.txn_info.get().unwrap().borrow_mut().add(
        //     OperationType::Read,
        //     table_id,
        //     column_id,
        //     offset,
        //     prv,
        // );

        // Ok(value)
        unimplemented!()
    }

    /// Write a value in a column at some offset.
    ///
    /// Note, operations effects are committed immediately.
    pub fn write_value(
        &self,
        value: &mut Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let table = database.get_flattable(table_id); // index into a vector
        let row = table.get_row(offset);

        let rw_table = row.get_rwtable();
        let lsn = row.get_lsn(); // index into a vector

        // lsn.load(Ordering::Relaxed) + 1;

        // let prv = rw_table.push_front(Access::Write(meta.clone()));

        // spin(prv, lsn);

        let tuple = row.get_tuple().get(); // index into a vector
        let value = tuple.get_value().unwrap().get_value();
        u64::try_from(value)? * 4; // dummy op

        // tuple.set_value(value).unwrap();

        // tuple.commit();

        // lsn.store(prv + 1, Ordering::Release);

        // self.txn_info.get().unwrap().borrow_mut().add(
        //     OperationType::Write,
        //     table_id,
        //     column_id,
        //     offset,
        //     prv,
        // );

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

    /// Commit a transaction.
    pub fn commit(&self, database: &Database) -> Result<(), NonFatalError> {
        self.tidy_up(database);

        Ok(())
    }

    /// Abort a transaction.
    pub fn abort(&self, database: &Database) -> NonFatalError {
        self.tidy_up(database);

        NonFatalError::NonSerializable
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
