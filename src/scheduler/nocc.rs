use crate::common::error::NonFatalError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::StatsBucket;
use crate::scheduler::ValueId;
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

        let guard = epoch::pin(); // pin thread

        NoConcurrencyControl::EG.with(|x| x.borrow_mut().replace(guard));

        TransactionId::NoConcurrencyControl
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
        NoConcurrencyControl::EG.with(|x| unsafe {
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
        self.tidy_up(database);

        Ok(())
    }

    /// Abort a transaction.
    pub fn abort(&self, _meta: &mut StatsBucket, database: &Database) -> NonFatalError {
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
