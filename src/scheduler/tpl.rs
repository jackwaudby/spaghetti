use std::cell::RefCell;

use thread_local::ThreadLocal;
use tracing::info;

use crate::common::error::NonFatalError;
use crate::common::error::TwoPhaseLockingError;
use crate::common::stats_bucket::StatsBucket;
use crate::common::transaction_information::Operation;
use crate::common::transaction_information::OperationType;
use crate::common::transaction_information::TransactionInformation;
use crate::common::value_id::ValueId;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::Database;

use self::lock_manager::LockManager;

pub mod lock_manager;

pub mod lock_table;

pub mod lock;

#[derive(Debug)]
pub struct TwoPhaseLocking {
    txn_ctr: ThreadLocal<RefCell<u64>>,
    lock_manager: LockManager,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

impl TwoPhaseLocking {
    pub fn new(cores: usize, tables: usize, rows: usize) -> Self {
        info!("Initialise two phase locking with: {} core(s)", cores);
        Self {
            txn_ctr: ThreadLocal::new(),
            lock_manager: LockManager::new(cores, tables, rows),
            txn_info: ThreadLocal::new(),
        }
    }

    fn record(&self, op_type: OperationType, vid: ValueId) {
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();

        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .add(op_type, table_id, column_id, offset, 0);
    }

    fn get_operations(&self) -> Vec<Operation> {
        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .get_clone()
    }

    pub fn begin(&self) -> TransactionId {
        *self.txn_info.get_or(|| RefCell::new(None)).borrow_mut() =
            Some(TransactionInformation::new());
        *self.txn_ctr.get_or(|| RefCell::new(0)).borrow_mut() += 1;
        let local_id = *self.txn_ctr.get_or(|| RefCell::new(0)).borrow();
        let thread_id = thread_id::get() as u64;

        let id = (local_id, thread_id);

        self.lock_manager.start(id);

        TransactionId::TwoPhaseLocking(id)
    }

    pub fn read_value(
        &self,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let table_id = vid.get_table_id();
        let transaction_id = meta.get_transaction_id().extract_pair();
        let offset = vid.get_offset();

        let lock_acquired =
            self.lock_manager
                .lock(transaction_id, table_id as u64, offset as u64, false);

        if !lock_acquired {
            return Err(TwoPhaseLockingError::ReadLockDenied.into());
        }

        let table = database.get_table(table_id);
        let column_id = vid.get_column_id();
        let tuple = table.get_tuple(column_id, offset).get();
        let value = tuple.get_value().unwrap().get_value();

        self.record(OperationType::Read, vid);

        Ok(value)
    }

    pub fn write_value(
        &self,
        value: &mut Data,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let table_id = vid.get_table_id();
        let transaction_id = meta.get_transaction_id().extract_pair();
        let offset = vid.get_offset();

        let acquired_lock =
            self.lock_manager
                .lock(transaction_id, table_id as u64, offset as u64, true);

        if !acquired_lock {
            return Err(TwoPhaseLockingError::WriteLockDenied.into());
        }

        let column_id = vid.get_column_id();
        let table = database.get_table(table_id);

        if let Err(_) = table.get_tuple(column_id, offset).get().set_value(value) {
            panic!(
                "{} attempting to write over uncommitted value on ({},{},{})",
                meta.get_transaction_id(),
                table_id,
                column_id,
                offset,
            ); // Assert: never write to an uncommitted value.
        }

        self.record(OperationType::Write, vid);

        Ok(())
    }

    pub fn commit(&self, meta: &mut StatsBucket, database: &Database) -> Result<(), NonFatalError> {
        let ops = self.get_operations();
        let transaction_id = meta.get_transaction_id().extract_pair();

        for op in &ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                ..
            } = op;

            if let OperationType::Write = op_type {
                let table = database.get_table(*table_id);
                let tuple = table.get_tuple(*column_id, *offset).get();
                tuple.commit();
            }
        }

        // release locks
        for op in &ops {
            let Operation {
                table_id, offset, ..
            } = op;
            self.lock_manager
                .unlock(transaction_id, *table_id as u64, *offset as u64);
        }

        self.lock_manager.end(transaction_id);

        Ok(())
    }

    pub fn abort(&self, meta: &mut StatsBucket, database: &Database) {
        let transaction_id = meta.get_transaction_id().extract_pair();

        let ops = self.get_operations();

        // revert changes
        for op in &ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                ..
            } = op;

            if let OperationType::Write = op_type {
                let table = database.get_table(*table_id);
                let tuple = table.get_tuple(*column_id, *offset).get();
                tuple.revert();
            }
        }

        // release locks
        for op in &ops {
            let Operation {
                table_id, offset, ..
            } = op;
            self.lock_manager
                .unlock(transaction_id, *table_id as u64, *offset as u64);
        }
    }
}
