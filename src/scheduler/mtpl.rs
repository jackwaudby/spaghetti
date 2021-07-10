use crate::common::error::NonFatalError;
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};
use crate::scheduler::mtpl::error::MixedTwoPhaseLockingError;
use crate::scheduler::mtpl::lock_info::{Lock, LockMode};
use crate::scheduler::mtpl::locks_held::LocksHeld;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;
use crate::workloads::IsolationLevel;

use flurry::HashMap;
use parking_lot::{Condvar, Mutex};
use std::cell::{RefCell, RefMut};
use std::sync::Arc;
use thread_local::ThreadLocal;
use tracing::{debug, info};

pub mod locks_held;

pub mod error;

pub mod lock_info;

#[derive(Debug)]
pub struct MixedTwoPhaseLocking {
    id: Arc<Mutex<u64>>,
    lock_table: HashMap<(usize, usize), Lock>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    locks_held: ThreadLocal<RefCell<LocksHeld>>,
    isolation_level: ThreadLocal<RefCell<IsolationLevel>>,
}

#[derive(Debug)]
enum LockRequest {
    AlreadyGranted,
    Granted,
    Denied,
    Delay(Arc<(Mutex<bool>, Condvar)>),
}

impl MixedTwoPhaseLocking {
    pub fn new(size: usize) -> Self {
        info!("Initialise two phase locking with: {} core(s)", size);
        Self {
            id: Arc::new(Mutex::new(0)),
            lock_table: HashMap::new(),
            locks_held: ThreadLocal::new(),
            txn_info: ThreadLocal::new(),
            isolation_level: ThreadLocal::new(),
        }
    }

    pub fn get_locks(&self) -> RefMut<LocksHeld> {
        self.locks_held.get().unwrap().borrow_mut()
    }

    pub fn get_level(&self) -> IsolationLevel {
        *self.isolation_level.get().unwrap().borrow_mut()
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

    pub fn begin(&self, isolation: IsolationLevel) -> TransactionId {
        *self
            .txn_info
            .get_or(|| RefCell::new(Some(TransactionInformation::new())))
            .borrow_mut() = Some(TransactionInformation::new());

        *self
            .locks_held
            .get_or(|| RefCell::new(LocksHeld::new()))
            .borrow_mut() = LocksHeld::new();

        *self
            .isolation_level
            .get_or(|| RefCell::new(IsolationLevel::Serializable))
            .borrow_mut() = isolation;

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

        match self.get_level() {
            // take no read lock
            IsolationLevel::ReadUncommitted => {
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation
                Ok(vals.unwrap().get_value())
            }

            // take short duration read lock
            IsolationLevel::ReadCommitted => {
                let request = self.request_lock(table_id, offset, LockMode::Read, *timestamp); // request read lock
                match request {
                    LockRequest::AlreadyGranted => {
                        let table: &Table = database.get_table(table_id); // get table
                        let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                        Ok(vals.unwrap().get_value())
                    }

                    LockRequest::Granted => {
                        let table: &Table = database.get_table(table_id); // get table
                        let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation
                        self.release_lock(table_id, offset, *timestamp).unwrap(); // release after reading

                        Ok(vals.unwrap().get_value())
                    }

                    LockRequest::Delay(pair) => {
                        let &(ref lock, ref cvar) = &*pair;
                        let mut waiting = lock.lock(); // wait flag: false = asleep
                        if !*waiting {
                            cvar.wait(&mut waiting);
                        }

                        let table: &Table = database.get_table(table_id); // get table
                        let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation
                        self.release_lock(table_id, offset, *timestamp).unwrap(); // release after reading

                        Ok(vals.unwrap().get_value())
                    }

                    LockRequest::Denied => {
                        self.abort(database, meta);

                        Err(MixedTwoPhaseLockingError::ReadLockRequestDenied(format!(
                            "{:?}",
                            (table_id, offset)
                        ))
                        .into())
                    }
                }
            }

            // take long duration read lock
            IsolationLevel::Serializable => {
                let request = self.request_lock(table_id, offset, LockMode::Read, *timestamp); // request read lock

                match request {
                    LockRequest::AlreadyGranted => {
                        let table: &Table = database.get_table(table_id); // get table
                        let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                        Ok(vals.unwrap().get_value())
                    }

                    LockRequest::Granted => {
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

                        self.add_lock(table_id, offset);
                        self.record(OperationType::Read, table_id, column_id, offset, 0); // record operation; prv is redunant here
                        let table: &Table = database.get_table(table_id); // get table
                        let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                        Ok(vals.unwrap().get_value())
                    }

                    LockRequest::Denied => {
                        self.abort(database, meta);

                        Err(MixedTwoPhaseLockingError::ReadLockRequestDenied(format!(
                            "{:?}",
                            (table_id, offset)
                        ))
                        .into())
                    }
                }
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
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }

            LockRequest::Granted => {
                self.add_lock(table_id, offset); // record operation; prv is redunant here
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

                self.add_lock(table_id, offset); // record operation; prv is redunant here
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }
            LockRequest::Denied => {
                self.abort(database, meta);

                Err(MixedTwoPhaseLockingError::WriteLockRequestDenied(format!(
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
        let mut locks = self.get_locks();

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
        let mut locks = self.get_locks();

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
        for op in locks.iter() {
            let (table_id, offset) = op.get_id();
            self.release_lock(table_id, offset, *timestamp).unwrap();
        }

        locks.clear();

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

        let outcome = if !info.is_free() {
            match request_mode {
                LockMode::Read => {
                    match info.get_mode() {
                        LockMode::Read => {
                            info.add_granted(LockMode::Read, timestamp);
                            LockRequest::Granted
                        }

                        LockMode::Write => {
                            // Assumption: transactions are uniquely identifable by their assigned timestamp.
                            if info.get_group_timestamp() == timestamp {
                                LockRequest::AlreadyGranted
                            } else if timestamp > info.get_group_timestamp() {
                                LockRequest::Denied // wait-die deadlock detection: newer requests are denied
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
                                    LockRequest::AlreadyGranted // upgraded from read to write lock
                                } else {
                                    LockRequest::Denied // other read locks held
                                }
                            } else if timestamp > info.get_group_timestamp() {
                                LockRequest::Denied // wait-die deadlock detection: newer requests are denied
                            } else {
                                let pair = info.add_waiting(LockMode::Write, timestamp);
                                LockRequest::Delay(pair)
                            }
                        }
                        LockMode::Write => {
                            // Assumption: transactions are uniquely identifable by their assigned timestamp.
                            if info.get_group_timestamp() == timestamp {
                                LockRequest::AlreadyGranted
                            } else if timestamp > info.get_group_timestamp() {
                                LockRequest::Denied // wait-die deadlock detection: newer requests are denied
                            } else {
                                let pair = info.add_waiting(LockMode::Write, timestamp);

                                LockRequest::Delay(pair)
                            }
                        }
                    }
                }
            }
        } else {
            info.add_granted(LockMode::Read, timestamp);
            LockRequest::Granted
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
            (&Granted, &Granted) => true,
            (&Denied, &Denied) => true,
            (&Delay(_), &Delay(_)) => true,
            _ => false,
        }
    }
}
