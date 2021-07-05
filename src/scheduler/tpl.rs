use crate::common::error::NonFatalError;
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::scheduler::tpl::error::TwoPhaseLockingError;
use crate::scheduler::tpl::lock_info::{Lock, LockMode};
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use flurry::HashMap;
use parking_lot::{Condvar, Mutex};
use std::cell::RefCell;
use std::sync::Arc;
use thread_local::ThreadLocal;
use tracing::{debug, info};

pub mod error;

pub mod lock_info;

#[derive(Debug)]
pub struct TwoPhaseLocking {
    id: Arc<Mutex<u64>>,
    lock_table: HashMap<(usize, usize), Lock>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

#[derive(Debug)]
enum LockRequest {
    AlreadyGranted,
    Granted,
    Denied,
    Delay(Arc<(Mutex<bool>, Condvar)>),
}

impl TwoPhaseLocking {
    pub fn new(size: usize) -> Self {
        info!("Initialise two phase locking with: {} core(s)", size);
        Self {
            id: Arc::new(Mutex::new(0)),
            lock_table: HashMap::new(),
            txn_info: ThreadLocal::new(),
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
            .borrow_mut() = Some(TransactionInformation::new());

        let mut lock = self.id.lock();
        let id = *lock;
        *lock += 1;

        debug!("start {}", id);
        TransactionId::TwoPhaseLocking(id)
    }

    pub fn read_value<'g>(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        debug!(
            "read by {} on ({},{},{})",
            timestamp, table_id, column_id, offset
        );

        let request = self.request_lock(table_id, offset, LockMode::Read, *timestamp); // request read lock

        match request {
            LockRequest::AlreadyGranted => {
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                Ok(vals.unwrap().get_value())
            }

            LockRequest::Granted => {
                self.record(OperationType::Read, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                Ok(vals.unwrap().get_value())
            }

            LockRequest::Delay(pair) => {
                let &(ref lock, ref cvar) = &*pair;
                let mut waiting = lock.lock(); // wait flag: false = asleep
                if !*waiting {
                    cvar.wait(&mut waiting);
                }

                self.record(OperationType::Read, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                Ok(vals.unwrap().get_value())
            }
            LockRequest::Denied => {
                self.abort(database, meta);

                Err(TwoPhaseLockingError::WriteLockRequestDenied(format!(
                    "{:?}",
                    (table_id, offset)
                ))
                .into())
            }
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
    ) -> Result<(), NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        debug!(
            "write by {} on ({},{},{})",
            timestamp, table_id, column_id, offset
        );

        let request = self.request_lock(table_id, offset, LockMode::Write, *timestamp); // request read lock

        match request {
            LockRequest::AlreadyGranted => {
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }

            LockRequest::Granted => {
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }

            LockRequest::Delay(pair) => {
                let &(ref lock, ref cvar) = &*pair;
                let mut waiting = lock.lock(); // wait flag: false = asleep
                if !*waiting {
                    cvar.wait(&mut waiting);
                }

                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }
            LockRequest::Denied => {
                self.abort(database, meta);

                Err(TwoPhaseLockingError::WriteLockRequestDenied(format!(
                    "{:?}",
                    (table_id, offset)
                ))
                .into())
            }
        }
    }

    /// Commit operation.
    pub fn commit<'g>(
        &self,
        database: &Database,
        meta: &TransactionId,
    ) -> Result<(), NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        debug!("commit by {}", timestamp);

        let ops = self.get_operations();

        // commit changes
        for op in &ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                ..
            } = op;

            if let OperationType::Write = op_type {
                let table = database.get_table(*table_id); // get table
                let tuple = table.get_tuple(*column_id, *offset).get(); // get tuple
                tuple.commit(); // commit; operations never fail
            }
        }

        // release locks
        for op in ops {
            let Operation {
                table_id, offset, ..
            } = op;
            self.release_lock(table_id, offset, *timestamp).unwrap();
        }

        Ok(())
    }

    /// Abort operation.
    pub fn abort<'g>(&self, database: &Database, meta: &TransactionId) -> NonFatalError {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        debug!("abort by {}", timestamp);
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
                let table = database.get_table(*table_id); // get table
                let tuple = table.get_tuple(*column_id, *offset).get(); // get tuple
                tuple.revert(); // revert; operations never fail
            }
        }

        // release locks
        for op in ops {
            let Operation {
                table_id, offset, ..
            } = op;
            self.release_lock(table_id, offset, *timestamp).unwrap();
        }

        NonFatalError::NonSerializable
    }

    /// Attempt to acquire lock.
    fn request_lock(
        &self,
        table_id: usize,
        offset: usize,
        request_mode: LockMode,
        timestamp: u64,
    ) -> LockRequest {
        debug!(
            "{:?} lock requested on ({},{}) by {}",
            request_mode, table_id, offset, timestamp
        );

        let mref = self.lock_table.pin(); // pin
        let lock_handle = mref.get(&(table_id, offset)); // handle to lock

        let lock = match lock_handle {
            Some(lock) => lock, // locks exist in the table for this record

            // no lock for this record in the lock table
            None => {
                let lock = Lock::create_and_grant(request_mode, timestamp); // create lock and grant to this request

                // attempt to insert into lock table
                match mref.try_insert((table_id, offset), lock) {
                    Ok(_) => return LockRequest::Granted, // success
                    Err(_) => {
                        return LockRequest::Denied; // lock was concurrently created; deny
                    }
                }
            }
        };

        let mut info = lock.lock(); // get exculsive access to lock

        if !info.is_free() {
            match request_mode {
                LockMode::Read => {
                    match info.get_mode() {
                        LockMode::Read => {
                            info.add_granted(LockMode::Read, timestamp);
                            drop(info);
                            return LockRequest::Granted;
                        }

                        LockMode::Write => {
                            // Assumption: transactions are uniquely identifable by their assigned timestamp.
                            if info.get_group_timestamp() == timestamp {
                                drop(info);
                                return LockRequest::AlreadyGranted;
                            }

                            if timestamp > info.get_group_timestamp() {
                                drop(info);
                                return LockRequest::Denied; // wait-die deadlock detection: newer requests are denied
                            }

                            let pair = info.add_waiting(LockMode::Read, timestamp);
                            drop(info);
                            return LockRequest::Delay(pair);
                        }
                    }
                }

                LockMode::Write => {
                    match info.get_mode() {
                        LockMode::Read => {
                            if info.upgrade(timestamp) {
                                drop(info);
                                return LockRequest::AlreadyGranted; // upgraded from read to write lock.
                            }

                            if timestamp > info.get_group_timestamp() {
                                drop(info);
                                return LockRequest::Denied; // wait-die deadlock detection: newer requests are denied
                            }

                            let pair = info.add_waiting(LockMode::Write, timestamp);
                            drop(info);
                            return LockRequest::Delay(pair);
                        }
                        LockMode::Write => {
                            // Assumption: transactions are uniquely identifable by their assigned timestamp.
                            if info.get_group_timestamp() == timestamp {
                                drop(info);
                                return LockRequest::AlreadyGranted;
                            }

                            if timestamp > info.get_group_timestamp() {
                                drop(info);
                                return LockRequest::Denied; // wait-die deadlock detection: newer requests are denied
                            }

                            let pair = info.add_waiting(LockMode::Write, timestamp);
                            drop(info);
                            LockRequest::Delay(pair)
                        }
                    }
                }
            }
        } else {
            // lock is free
            info.add_granted(LockMode::Read, timestamp);
            drop(info);
            return LockRequest::Granted;
        }
    }

    /// Release a lock.
    fn release_lock(
        &self,
        table_id: usize,
        offset: usize,
        timestamp: u64,
    ) -> Result<(), NonFatalError> {
        let mref = self.lock_table.pin();
        let lock_handle = mref.get(&(table_id, offset)).unwrap();

        let mut info = lock_handle.lock();

        // If 1 granted lock and no waiting requests; then reset lock
        if info.num_granted() == 1 && !info.is_waiting() {
            info.reset();
            return Ok(());
        }

        let request = info.remove_granted(timestamp);

        match request.get_mode() {
            LockMode::Write => {
                debug!(
                    "write lock released on ({},{}) by {}",
                    table_id, offset, timestamp
                );
                info.grant_waiting();
            }

            LockMode::Read => {
                debug!(
                    "read lock released on ({},{}) by {}",
                    table_id, offset, timestamp
                );
                if info.num_granted() > 1 {
                    // (i) multiple outstanding read locks
                    // drop request
                    // TODO: need to update lock group timestamp?
                } else {
                    info.grant_waiting();
                }
            }
        }
        Ok(())
    }
}

impl PartialEq for LockRequest {
    fn eq(&self, other: &Self) -> bool {
        use LockRequest::*;
        match (self, other) {
            (&Granted, &Granted) => true,
            (&Denied, &Denied) => true,
            (&Delay(_), &Delay(_)) => true,
            _ => false,
        }
    }
}
