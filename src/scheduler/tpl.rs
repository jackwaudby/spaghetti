use crate::common::error::NonFatalError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::tpl::error::TwoPhaseLockingError;
use crate::scheduler::tpl::lock_info::{Lock, LockMode};
use crate::scheduler::tpl::locks_held::LocksHeld;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use flurry::HashMap;
use parking_lot::{Condvar, Mutex};
use std::cell::{RefCell, RefMut};
use std::fmt;
use std::sync::Arc;
use thread_local::ThreadLocal;
use tracing::{debug, info, instrument, Span};

pub mod locks_held;

pub mod error;

pub mod lock_info;

#[derive(Debug)]
pub struct TwoPhaseLocking {
    id: Arc<Mutex<u64>>,
    lock_table: HashMap<(usize, usize), Lock>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    locks_held: ThreadLocal<RefCell<LocksHeld>>,
}

#[derive(Debug)]
enum LockRequest {
    Upgraded(LockMode),
    AlreadyGranted(LockMode),
    Granted(LockMode),
    Denied(LockMode),
    Delay(Arc<(Mutex<bool>, Condvar)>),
}

impl TwoPhaseLocking {
    pub fn new(size: usize) -> Self {
        info!("Initialise two phase locking with: {} core(s)", size);
        Self {
            id: Arc::new(Mutex::new(0)),
            lock_table: HashMap::new(),
            locks_held: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
        }
    }

    pub fn get_locks(&self) -> RefMut<LocksHeld> {
        self.locks_held.get().unwrap().borrow_mut()
    }

    pub fn add_lock(&self, table_id: usize, offset: usize) {
        let mut locks_held = self.locks_held.get().unwrap().borrow_mut();
        locks_held.add(table_id, offset);
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

    /// Start a new transaction.
    #[instrument(level = "debug", skip(self), fields(id))]
    pub fn begin(&self) -> TransactionId {
        *self
            .txn_info
            .get_or(|| RefCell::new(Some(TransactionInformation::new())))
            .borrow_mut() = Some(TransactionInformation::new());

        *self
            .locks_held
            .get_or(|| RefCell::new(LocksHeld::new()))
            .borrow_mut() = LocksHeld::new();

        let mut lock = self.id.lock();

        let id = *lock;
        *lock += 1;

        Span::current().record("id", &id);
        debug!("begin");

        TransactionId::TwoPhaseLocking(id)
    }

    /// Read a value at `offset` in `column_id` in `table_id`.
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn read_value<'g>(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => {
                Span::current().record("id", &id);
                id
            }
            _ => panic!("unexpected txn id"),
        };

        let request = self.request_lock(table_id, offset, LockMode::Read, *timestamp); // request read lock
        debug!("{}", request);

        match request {
            LockRequest::AlreadyGranted(_) => {
                // No need to record lock or operation
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                Ok(vals.unwrap().get_value())
            }

            LockRequest::Granted(_) => {
                self.add_lock(table_id, offset);
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
                debug!("read lock granted");
                self.add_lock(table_id, offset);
                self.record(OperationType::Read, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                Ok(vals.unwrap().get_value())
            }
            LockRequest::Denied(_) => {
                self.abort(database, meta);

                Err(TwoPhaseLockingError::ReadLockRequestDenied(format!(
                    "{:?}",
                    (table_id, offset)
                ))
                .into())
            }

            LockRequest::Upgraded(_) => unreachable!(),
        }
    }

    /// Write operation.
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn write_value<'g>(
        &self,
        value: &mut Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => {
                Span::current().record("id", &id);
                id
            }
            _ => panic!("unexpected txn id"),
        };

        let request = self.request_lock(table_id, offset, LockMode::Write, *timestamp); // request read lock
        debug!("{}", request);

        match request {
            LockRequest::Upgraded(_) => {
                // Lock already registered from read
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }

            LockRequest::AlreadyGranted(_) => {
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }

            LockRequest::Granted(_) => {
                self.add_lock(table_id, offset);
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

                debug!("write lock granted");

                self.add_lock(table_id, offset); // record operation; prv is redunant here
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }
            LockRequest::Denied(_) => {
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
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn commit<'g>(
        &self,
        database: &Database,
        meta: &TransactionId,
    ) -> Result<(), NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => {
                Span::current().record("id", &id);
                id
            }
            _ => panic!("unexpected txn id"),
        };

        let ops = self.get_operations();
        let mut locks = self.get_locks();

        debug!("operations: {:?}", ops);
        debug!("locks to release: {:?}", locks);

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
        for op in locks.iter() {
            let (table_id, offset) = op.get_id();
            self.release_lock(table_id, offset, *timestamp).unwrap();
        }

        locks.clear();

        debug!("Committed");
        Ok(())
    }

    /// Abort operation.
    #[instrument(level = "debug", skip(self, meta, database), fields(id))]
    pub fn abort<'g>(&self, database: &Database, meta: &TransactionId) -> NonFatalError {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => {
                Span::current().record("id", &id);
                id
            }
            _ => panic!("unexpected txn id"),
        };

        let ops = self.get_operations();
        let mut locks = self.get_locks();

        debug!("operations: {:?}", ops);
        debug!("locks to release: {:?}", locks);

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

            debug!("revert changes on ({},{},{})", table_id, column_id, offset);
        }

        // release locks

        for op in locks.iter() {
            let (table_id, offset) = op.get_id();
            self.release_lock(table_id, offset, *timestamp).unwrap();
        }

        locks.clear();

        debug!("aborted");
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
        let mref = self.lock_table.pin(); // pin
        let lock_handle = mref.get(&(table_id, offset)); // handle to lock

        let lock = match lock_handle {
            Some(lock) => lock, // locks exist in the table for this record

            // no lock for this record in the lock table
            None => {
                let lock = Lock::create_and_grant(request_mode, timestamp); // create lock and grant to this request

                // attempt to insert into lock table
                match mref.try_insert((table_id, offset), lock) {
                    Ok(_) => return LockRequest::Granted(request_mode), // success
                    Err(_) => {
                        return LockRequest::Denied(request_mode); // lock was concurrently created; deny
                    }
                }
            }
        };

        let mut info = lock.lock(); // get exculsive access to lock

        let outcome = if !info.is_free() {
            match request_mode {
                LockMode::Read => {
                    match info.get_mode() {
                        LockMode::Read => {
                            if info.holds_lock(timestamp) {
                                LockRequest::AlreadyGranted(request_mode) // nth read request
                            } else {
                                info.add_granted(LockMode::Read, timestamp); // 1st read request
                                LockRequest::Granted(request_mode)
                            }
                        }

                        LockMode::Write => {
                            // Assumption: transactions are uniquely identifable by their assigned timestamp.
                            if info.get_group_timestamp() == timestamp {
                                LockRequest::AlreadyGranted(request_mode)
                            } else if timestamp > info.get_deadlock_detection_timestamp() {
                                LockRequest::Denied(request_mode) // wait-die deadlock detection: newer requests are denied
                            } else {
                                let pair = info.add_waiting(LockMode::Read, timestamp);
                                LockRequest::Delay(pair)
                            }
                        }
                    }
                }

                LockMode::Write => {
                    match info.get_mode() {
                        LockMode::Read => {
                            if info.holds_lock(timestamp) {
                                // transaction already holds read lock
                                if info.upgrade(timestamp) {
                                    LockRequest::Upgraded(request_mode) // upgraded from read to write lock
                                } else {
                                    LockRequest::Denied(request_mode) // other read locks held
                                }
                            } else if timestamp > info.get_group_timestamp() {
                                LockRequest::Denied(request_mode) // wait-die deadlock detection: newer requests are denied
                            } else {
                                let pair = info.add_waiting(LockMode::Write, timestamp);
                                LockRequest::Delay(pair)
                            }
                        }
                        LockMode::Write => {
                            // Assumption: transactions are uniquely identifable by their assigned timestamp.
                            if info.get_group_timestamp() == timestamp {
                                LockRequest::AlreadyGranted(request_mode)
                            } else if timestamp > info.get_deadlock_detection_timestamp() {
                                LockRequest::Denied(request_mode) // wait-die deadlock detection: newer requests are denied
                            } else {
                                let pair = info.add_waiting(LockMode::Write, timestamp);

                                LockRequest::Delay(pair)
                            }
                        }
                    }
                }
            }
        } else {
            info.add_granted(request_mode, timestamp);
            LockRequest::Granted(request_mode)
        };

        drop(info);
        outcome
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

        if info.num_granted() == 1 && !info.is_waiting() {
            let mode = info.get_mode();
            debug!(
                "{} lock released on ({},{}) by {}",
                mode, table_id, offset, timestamp
            );
            info.reset();

            return Ok(()); // no waiting requests; reset lock
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
                if info.num_granted() == 0 {
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
            (&Granted(_), &Granted(_)) => true,
            (&Denied(_), &Denied(_)) => true,
            (&Delay(_), &Delay(_)) => true,
            _ => false,
        }
    }
}

impl fmt::Display for LockRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LockRequest::AlreadyGranted(lock_mode) => {
                write!(f, "{} already granted", lock_mode)
            }
            LockRequest::Denied(lock_mode) => {
                write!(f, "{} denied", lock_mode)
            }
            LockRequest::Granted(lock_mode) => {
                write!(f, "{} granted", lock_mode)
            }
            LockRequest::Upgraded(lock_mode) => {
                write!(f, " read lock upgraded to {}", lock_mode)
            }
            LockRequest::Delay(_) => write!(f, "delayed"),
        }
    }
}
