use crate::common::error::NonFatalError;
use crate::scheduler::sgt::transaction_information::{
    Operation, OperationType, TransactionInformation,
};
use crate::scheduler::tpl::error::TwoPhaseLockingError;
use crate::scheduler::tpl::lock_info::LockInfo;
use crate::scheduler::tpl::lock_info::*;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::table::Table;
use crate::storage::Database;

use crossbeam_epoch::Guard;
use flurry::HashMap;
use std::cell::RefCell;
use std::sync::{Arc, Condvar, Mutex};
use thread_local::ThreadLocal;
use tracing::info;

pub mod error;

pub mod lock_info;

#[derive(Debug)]
pub struct TwoPhaseLocking {
    id: Arc<Mutex<u64>>,
    lock_table: HashMap<(usize, usize), LockInfo>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
}

#[derive(Debug)]
enum LockRequest {
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

        let mut lock = self.id.lock().unwrap();
        let id = *lock;
        *lock += 1;

        TransactionId::TwoPhaseLocking(id)
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
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let request = self.request_lock(table_id, offset, LockMode::Read, *timestamp); // request read lock

        match request {
            LockRequest::Granted => {
                self.record(OperationType::Read, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                Ok(vals.unwrap().get_value())
            }

            LockRequest::Delay(pair) => {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                self.record(OperationType::Read, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let vals = table.get_tuple(column_id, offset).get().get_value(); // read operation

                Ok(vals.unwrap().get_value())
            }
            LockRequest::Denied => {
                self.abort(database, guard, meta);

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
        guard: &'g Guard,
    ) -> Result<(), NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let request = self.request_lock(table_id, offset, LockMode::Write, *timestamp); // request read lock

        match request {
            LockRequest::Granted => {
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }

            LockRequest::Delay(pair) => {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                self.record(OperationType::Write, table_id, column_id, offset, 0); // record operation; prv is redunant here
                let table: &Table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.set_value(value).unwrap(); // set value

                Ok(())
            }
            LockRequest::Denied => {
                self.abort(database, guard, meta);

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
        guard: &'g Guard,
        meta: &TransactionId,
    ) -> Result<(), NonFatalError> {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let ops = self.get_operations();

        // commit changes
        for op in ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                ..
            } = op;

            if let OperationType::Write = op_type {
                let table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.commit(); // commit; operations never fail
            }
        }

        // release locks
        for op in ops {
            let Operation {
                table_id, offset, ..
            } = op;
            self.release_lock(table_id, offset, *timestamp);
        }

        Ok(())
    }

    /// Abort operation.
    pub fn abort<'g>(
        &self,
        database: &Database,
        guard: &'g Guard,
        meta: &TransactionId,
    ) -> NonFatalError {
        let timestamp = match meta {
            TransactionId::TwoPhaseLocking(id) => id,
            _ => panic!("unexpected txn id"),
        };

        let ops = self.get_operations();

        // revert changes
        for op in ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                ..
            } = op;

            if let OperationType::Write = op_type {
                let table = database.get_table(table_id); // get table
                let tuple = table.get_tuple(column_id, offset).get(); // get tuple
                tuple.revert(); // revert; operations never fail
            }
        }

        // release locks
        for op in ops {
            let Operation {
                table_id, offset, ..
            } = op;
            self.release_lock(table_id, offset, *timestamp);
        }

        NonFatalError::NonSerializable
    }

    /// Attempt to acquire lock.
    ///
    /// Takes a search key, desired lock mode, and transaction name and timestamp.
    /// If the lock is acquired `Granted` is returned.
    /// If the lock request is refused `Denied` is returned.
    /// If the lock request is delayed `Delayed` is returned.
    fn request_lock(
        &self,
        table_id: usize,
        offset: usize,
        request_mode: LockMode,
        timestamp: u64,
    ) -> LockRequest {
        let mref = self.lock_table.pin();

        let lock_info = mref.get(&(table_id, offset)); // attempt to get lock for a key

        let mut lock_info = match lock_info {
            // lock exists in the table for this record
            Some(lock) => lock,

            // no lock for this record, create a new one
            None => {
                let mut lock_info = LockInfo::new(request_mode, timestamp); // create new lock information
                let entry = Entry::new(request_mode, None, timestamp); // create new request entry
                lock_info.add_entry(entry); // add entry to lock info

                // attempt to insert into lock table
                match mref.try_insert((table_id, offset), lock_info) {
                    // inserted
                    Ok(_) => return LockRequest::Granted,

                    // lock info was concurrently created
                    Err(_) => {
                        return LockRequest::Denied;
                    }
                }
            }
        };

        // if lock is not free then consult the granted lock
        if !lock_info.is_free() {
            // Consult the lock's group mode.
            //
            // If request is a `Read` then:
            // (a) If current lock is a `Read` then grant the lock.
            // (b) If current lock is a `Write` then and apply deadlock detection:
            //    (i) If lock.timestamp > request.timestamp (older) then the request waits and the calling thread waits.
            //    (ii) If lock.timestamp < request.timestamp (younger) the requests die and then the lock is denied.
            //
            // If request is a `Write` then apply deadlock detection as above.
            match request_mode {
                // requesting a read lock
                LockMode::Read => {
                    match lock_info.get_mode() {
                        // current lock is read; grant lock
                        LockMode::Read => {
                            let entry = Entry::new(LockMode::Read, None, timestamp); // create new entry
                            lock_info.add_entry(entry); // add to lock info

                            // if this request has lower timestamp; then update lock's timestamp
                            if timestamp < lock_info.get_timestamp() {
                                lock_info.set_timestamp(timestamp);
                            }

                            lock_info.inc_granted(); // increment locks granted

                            LockRequest::Granted
                        }
                        //  current lock is write; apply deadlock detection then wait
                        LockMode::Write => {
                            // wait-die deadlock detection
                            // if transaction is younger than transaction holding the lock; then deny lock
                            if lock_info.get_timestamp() < timestamp {
                                return LockRequest::Denied;
                            }

                            // only wait if all waiting write requests are older
                            if lock_info.get_iter().any(|e| {
                                e.get_timestamp() > timestamp
                                    && e.get_mode() == LockMode::Write
                                    && e.get_timestamp() != lock_info.get_timestamp()
                            }) {
                                return LockRequest::Denied;
                            }

                            let pair = Arc::new((Mutex::new(false), Condvar::new())); // initialise condvar to sleep the waiting thread
                            let entry =
                                Entry::new(LockMode::Read, Some(Arc::clone(&pair)), timestamp); // create new entry for transaction request list
                            lock_info.add_entry(entry); // add to request list

                            if !lock_info.is_waiting() {
                                lock_info.set_waiting(true); // set waiting transaction(s) flag on lock.
                            }

                            LockRequest::Delay(pair)
                        }
                    }
                }

                // requesting a write lock
                LockMode::Write => {
                    // wait-die deadlock detection
                    // if transaction is younger than transaction holding the lock; then deny lock
                    if lock_info.get_timestamp() < timestamp {
                        return LockRequest::Denied;
                    }

                    // only wait if all waiting write requests are older
                    if lock_info.get_iter().any(|e| {
                        e.get_timestamp() > timestamp
                            && e.get_mode() == LockMode::Write
                            && e.get_timestamp() != lock_info.get_timestamp()
                    }) {
                        return LockRequest::Denied;
                    }

                    let pair = Arc::new((Mutex::new(false), Condvar::new())); // initialise condvar to sleep the waiting thread
                    let entry = Entry::new(LockMode::Write, Some(Arc::clone(&pair)), timestamp); // create new entry for transaction request list
                    lock_info.add_entry(entry); // add to request list

                    if !lock_info.is_waiting() {
                        lock_info.set_waiting(true); // set waiting transaction(s) flag on lock
                    }

                    LockRequest::Delay(pair)
                }
            }
        } else {
            // lock is free

            lock_info.set_mode(request_mode); // set group mode to request mode
            let entry = Entry::new(request_mode, None, timestamp); // create new entry for request
            lock_info.add_entry(entry);
            lock_info.set_timestamp(timestamp); // set lock timestamp
            lock_info.inc_granted(); // increment granted

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
        let lock_info = mref.get(&(table_id, offset)).unwrap();

        // if 1 granted lock and no waiting requests; then reset lock
        if lock_info.get_granted() == 1 && !lock_info.is_waiting() {
            lock_info.reset();
            return Ok(());
        }

        let entry_index = lock_info
            .get_mut_list()
            .iter()
            .position(|e| e.get_timestamp() == timestamp)
            .unwrap(); // find the entry for this lock request

        let entry = lock_info.remove_entry(entry_index); // Remove transactions entry.

        match entry.get_mode() {
            // if record was locked with a Write lock then this is the only transaction with a lock
            // on this record. If so,
            // (i) Grant the next n read lock requests, or,
            // (ii) Grant the next write lock request
            LockMode::Write => {
                let mut read_requests = 0; // calculate waiting read requests

                for e in lock_info.get_iter() {
                    if e.get_mode() == LockMode::Write {
                        break;
                    }
                    read_requests += 1; // TODO: grant all read request with timestamp less than this write request
                }

                // if some reads requests first in queue;
                if read_requests != 0 {
                    // wake up threads and determine new lock timestamp
                    let mut new_lock_timestamp = 0;

                    for e in lock_info.get_iter().take(read_requests) {
                        if e.get_timestamp() > new_lock_timestamp {
                            new_lock_timestamp = e.get_timestamp();
                        }
                        let cond = e.get_cond();
                        let (lock, cvar) = &*cond;
                        let mut started = lock.lock().unwrap(); // wake up thread
                        *started = true;
                        cvar.notify_all();
                    }

                    // set new lock information
                    lock_info.set_timestamp(new_lock_timestamp); // highest read timestamp
                    lock_info.set_mode(LockMode::Read); // group mode
                    lock_info.set_granted(read_requests as u32); // granted n read requests

                    // if all waiters have have been granted then set to false
                    if lock_info.num_waiting() as u32 == lock_info.get_granted() {
                        lock_info.set_waiting(false);
                    }
                } else {
                    // no reads waiting

                    // set new lock information
                    lock_info.set_timestamp(lock_info.get_list()[0].get_timestamp());
                    lock_info.set_mode(LockMode::Write);
                    lock_info.set_granted(1);

                    // if all waiters have have been granted then set to false
                    if lock_info.num_waiting() as u32 == lock_info.get_granted() {
                        lock_info.set_waiting(false);
                    }

                    let cond = lock_info.get_list()[0].get_cond();
                    let (lock, cvar) = &*cond;
                    let mut started = lock.lock().unwrap();
                    *started = true;
                    cvar.notify_all();
                }
            }
            // If record was locked with a read lock then either,
            // (i) there are n other read locks being held, or,
            // (ii) this is the only read lock. In which case check if any write lock requests are waiting
            // Assumption: reads requests are always granted when a read lock is held, thus none wait
            LockMode::Read => {
                if lock_info.get_granted() > 1 {
                    // removal of this lock still leaves the lock in read mode.

                    // update lock information
                    let mut new_lock_timestamp = 0;
                    for e in lock_info.get_iter() {
                        if e.get_timestamp() > new_lock_timestamp {
                            new_lock_timestamp = e.get_timestamp();
                        }
                    }
                    lock_info.set_timestamp(new_lock_timestamp);
                    lock_info.dec_granted(); // decrement locks held.
                } else {
                    // 1 active read lock and some write lock waiting.

                    if lock_info.is_waiting() {
                        let next_write_entry_index = lock_info
                            .get_iter()
                            .position(|e| e.get_mode() == LockMode::Write)
                            .unwrap();

                        lock_info.set_mode(LockMode::Write);
                        lock_info.set_timestamp(
                            lock_info.get_list()[next_write_entry_index].get_timestamp(),
                        );
                        lock_info.inc_granted();
                        if lock_info.num_waiting() as u32 == lock_info.get_granted() {
                            lock_info.set_waiting(false);
                        }

                        let cond = lock_info.get_list()[next_write_entry_index].get_cond();
                        let (lock, cvar) = &*cond;
                        let mut started = lock.lock().unwrap();
                        *started = true;
                        cvar.notify_all();
                    }
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
