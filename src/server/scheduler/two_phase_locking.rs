use crate::common::error::NonFatalError;
use crate::server::scheduler::two_phase_locking::active_transaction::ActiveTransaction;
use crate::server::scheduler::two_phase_locking::error::TwoPhaseLockingError;
use crate::server::scheduler::two_phase_locking::lock_info::{Entry, LockInfo, LockMode};
use crate::server::scheduler::Scheduler;
use crate::server::scheduler::TransactionInfo;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::Row;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use chashmap::CHashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::{fmt, thread};

use tracing::{debug, info};

pub mod error;

pub mod lock_info;

pub mod active_transaction;

/// Represents a 2PL scheduler.
#[derive(Debug)]
pub struct TwoPhaseLocking {
    /// Transaction ID counter.
    id: Arc<Mutex<u64>>,

    /// Map of database records to their lock information.
    lock_table: Arc<CHashMap<PrimaryKey, LockInfo>>,

    /// Map of transaction ids to neccessary runtime information.
    active_transactions: Arc<CHashMap<String, ActiveTransaction>>,

    /// Handle to storage layer.
    data: Arc<Workload>,
}

impl Scheduler for TwoPhaseLocking {
    /// Register a transaction with the scheduler.
    ///
    /// # Aborts
    ///
    /// A transaction with the same name is already registered.
    fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        let th = thread::current();
        let thread_id = th.name().unwrap(); // get thread id
        let counter = Arc::clone(&self.id); // get handle to id generator
        let mut lock = counter.lock().unwrap(); // get lock
        let id = *lock; // get id
        *lock += 1; // increment
        debug!("Thread {}: assigned id {}", thread_id, id);
        drop(lock); // drop lock

        let t = TransactionInfo::new(Some(id.to_string()), Some(id));
        let at = ActiveTransaction::new(&t.get_id().unwrap()); // create active transaction

        // insert into map of active transactions.
        if self
            .active_transactions
            .insert(t.get_id().unwrap(), at)
            .is_some()
        {
            return Err(TwoPhaseLockingError::AlreadyRegistered(t.get_id().unwrap()).into());
        }

        Ok(t)
    }

    /// Attempt to create a row in a table.
    ///
    /// The row is created and stored in the active transactions.
    /// It is then inserted at the point the transaction commits.
    /// It is possible for phantoms to occur in this set up.
    ///
    /// # Aborts
    ///
    /// The table cannot be found.
    /// Column cannot be found in table.
    /// Parsing error.
    /// Row already exists.
    fn create(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let table = self.get_table(table, &meta)?; // get handle to table
        let index_name = self.get_index_name(Arc::clone(&table), &meta)?;
        let index = self.get_index(Arc::clone(&table), &meta)?; // get handle to index

        let lock_info = self.lock_table.get_mut(&key); // get lock for the new record
        match lock_info {
            // lock exists for proposed new record, hence the record already exists
            Some(_) => {
                self.abort(&meta).unwrap();
                return Err(NonFatalError::RowAlreadyExists(
                    format!("{}", key),
                    table.to_string(),
                )); // abort -- record exists in index
            }
            // no lock in the table for this record
            None => {
                let mut lock_info =
                    LockInfo::new(LockMode::Write, meta.get_ts().unwrap(), &index_name); // new lock info
                let entry = Entry::new(
                    meta.get_id().unwrap().to_string(),
                    LockMode::Write,
                    None,
                    meta.get_ts().unwrap(),
                ); // new lock entry
                lock_info.add_entry(entry);
                let res = self.lock_table.insert(key.clone(), lock_info); // attempt to insert into lock tabe
                if let Some(existing_lock) = res {
                    self.lock_table.insert(key.clone(), existing_lock); // lock was concurrently created, back off
                    return Err(NonFatalError::RowAlreadyExists(
                        format!("{}", key),
                        table.to_string(),
                    )); // abort -- record exists in index
                }
            }
        };

        self.active_transactions
            .get_mut(&meta.get_id().unwrap())
            .unwrap()
            .add_lock(key.clone()); // register lock with transaction

        if index.key_exists(key.clone()) {
            return Err(NonFatalError::RowAlreadyExists(
                format!("{}", key),
                table.to_string(),
            )); // abort -- record exists in index
        }

        let mut row = Row::new(Arc::clone(&table), "2pl"); // init row
        row.set_primary_key(key); // set pk

        for (i, column) in columns.iter().enumerate() {
            if let Err(e) = row.init_value(column, &values[i].to_string()) {
                self.abort(&meta).unwrap();
                return Err(e); // abort -- initalise error
            }
        }

        // make rows dirty
        if let Err(e) = row.set_values(columns, values, "2pl", &meta.get_id().unwrap()) {
            self.abort(&meta).unwrap();
            return Err(e);
        }

        self.active_transactions
            .get_mut(&meta.get_id().unwrap())
            .unwrap()
            .add_row_to_insert(index, row); // register insert

        Ok(())
    }

    /// Attempt to read columns in row.
    ///
    /// # Aborts
    ///
    /// The table cannot be found.
    /// There is no primary index on this table.
    /// The index cannot be found.
    /// Row cannot be found.
    /// Column does not exist in the table.
    /// Lock request denied.
    fn read(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        let table = self.get_table(table, &meta)?; // get handle to table
        let index_name = self.get_index_name(Arc::clone(&table), &meta)?;
        let index = self.get_index(Arc::clone(&table), &meta)?; // get handle to index

        let request = self.request_lock(
            &index_name,
            key.clone(),
            LockMode::Read,
            &meta.get_id().unwrap(),
            meta.get_ts().unwrap(),
        ); // request read lock

        match request {
            LockRequest::Granted => {
                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone()); // register lock

                match index.read(key, columns, "2pl", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let vals = res.get_values().unwrap(); // get values
                        Ok(vals)
                    }
                    Err(e) => {
                        self.abort(&meta).unwrap();
                        Err(e) // abort -- ???
                    }
                }
            }

            LockRequest::Delay(pair) => {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }

                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone()); // register lock

                match index.read(key, columns, "2pl", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let vals = res.get_values().unwrap(); // Get values
                        Ok(vals)
                    }
                    Err(e) => {
                        self.abort(&meta).unwrap();
                        Err(e)
                    }
                }
            }

            LockRequest::Denied => {
                self.abort(&meta).unwrap();
                Err(TwoPhaseLockingError::ReadLockRequestDenied(format!("{}", key)).into())
            }
        }
    }

    /// Attempt to update columns in row.
    ///
    /// # Aborts
    ///
    /// The table cannot be found.
    /// There is no primary index on this table.
    /// The index cannot be found.
    /// Row cannot be found.
    /// Column does not exist in the table.
    /// Lock request denied.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: Vec<String>,
        read: bool,
        params: Vec<Data>,
        f: &dyn Fn(
            Vec<String>,
            Option<Vec<Data>>,
            Vec<Data>,
        ) -> Result<(Vec<String>, Vec<String>), NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let table = self.get_table(table, &meta)?; // get handle to table
        let index_name = self.get_index_name(Arc::clone(&table), &meta)?;
        let index = self.get_index(Arc::clone(&table), &meta)?; // get handle to index

        let request = self.request_lock(
            &index_name,
            key.clone(),
            LockMode::Write,
            &meta.get_id().unwrap(),
            meta.get_ts().unwrap(),
        ); // request lock

        match request {
            LockRequest::Granted => {
                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone()); // Register lock.

                if let Err(e) = index.update(
                    key,
                    columns,
                    read,
                    params,
                    f,
                    "2pl",
                    &meta.get_id().unwrap(),
                ) {
                    self.abort(&meta).unwrap();
                    return Err(e);
                }
                Ok(())
            }
            LockRequest::Delay(pair) => {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }

                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone()); // register lock

                if let Err(e) = index.update(
                    key,
                    columns,
                    read,
                    params,
                    f,
                    "2pl",
                    &meta.get_id().unwrap(),
                ) {
                    self.abort(&meta).unwrap();
                    return Err(e);
                }
                Ok(())
            }
            LockRequest::Denied => {
                let err = TwoPhaseLockingError::WriteLockRequestDenied(format!("{}", key));
                self.abort(&meta).unwrap();
                Err(err.into())
            }
        }
    }

    /// Attempt to append `value` to `columns` in row.
    ///
    /// # Aborts
    ///
    /// The table cannot be found.
    /// There is no primary index on this table.
    /// The index cannot be found.
    /// Row cannot be found.
    /// Column does not exist in the table.
    /// Lock request denied.
    fn append(
        &self,
        table: &str,
        key: PrimaryKey,
        column: &str,
        value: &str,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let table = self.get_table(table, &meta)?;
        let index_name = self.get_index_name(Arc::clone(&table), &meta)?;
        let index = self.get_index(Arc::clone(&table), &meta)?;

        let request = self.request_lock(
            &index_name,
            key.clone(),
            LockMode::Write,
            &meta.get_id().unwrap(),
            meta.get_ts().unwrap(),
        ); // request lock

        match request {
            LockRequest::Granted => {
                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone()); // register lock

                if let Err(e) = index.append(key, column, value, "2pl", &meta.get_id().unwrap()) {
                    self.abort(&meta).unwrap();
                    return Err(e);
                }

                Ok(())
            }
            LockRequest::Delay(pair) => {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }

                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone()); // Register lock.
                if let Err(e) = index.append(key, column, value, "2pl", &meta.get_id().unwrap()) {
                    self.abort(&meta).unwrap();
                    return Err(e);
                }
                Ok(())
            }
            LockRequest::Denied => {
                let err = TwoPhaseLockingError::WriteLockRequestDenied(format!("{}", key));
                self.abort(&meta).unwrap();
                Err(err.into())
            }
        }
    }

    /// Attempt to update columns in row.
    ///
    /// # Aborts
    ///
    /// The table cannot be found.
    /// There is no primary index on this table.
    /// The index cannot be found.
    /// Row cannot be found.
    /// Column does not exist in the table.
    /// Lock request denied.
    fn read_and_update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        let table = self.get_table(table, &meta)?;
        let index_name = self.get_index_name(Arc::clone(&table), &meta)?;
        let index = self.get_index(Arc::clone(&table), &meta)?;

        let request = self.request_lock(
            &index_name,
            key.clone(),
            LockMode::Write,
            &meta.get_id().unwrap(),
            meta.get_ts().unwrap(),
        );

        match request {
            LockRequest::Granted => {
                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone());

                match index.read_and_update(key, columns, values, "2pl", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let vals = res.get_values().unwrap(); // Get values.
                        Ok(vals)
                    }
                    Err(e) => {
                        self.abort(&meta).unwrap();
                        Err(e)
                    }
                }
            }
            LockRequest::Delay(pair) => {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }

                // Register lock.
                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone());
                // Execute update.

                match index.read_and_update(key, columns, values, "2pl", &meta.get_id().unwrap()) {
                    Ok(res) => {
                        let vals = res.get_values().unwrap();
                        Ok(vals)
                    }
                    Err(e) => {
                        self.abort(&meta).unwrap();
                        Err(e)
                    }
                }
            }
            LockRequest::Denied => {
                let err = TwoPhaseLockingError::WriteLockRequestDenied(format!("{}", key));
                self.abort(&meta).unwrap();
                Err(err.into())
            }
        }
    }

    /// Attempt to delete row.
    ///
    /// # Aborts
    ///
    /// The table cannot be found.
    /// There is no primary index on this table.
    /// The index cannot be found.
    /// Row cannot be found.
    fn delete(
        &self,
        table: &str,
        key: PrimaryKey,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let table = self.get_table(table, &meta)?;
        let index_name = self.get_index_name(Arc::clone(&table), &meta)?;
        let index = self.get_index(Arc::clone(&table), &meta)?;

        let request = self.request_lock(
            &index_name,
            key.clone(),
            LockMode::Write,
            &meta.get_id().unwrap(),
            meta.get_ts().unwrap(),
        );

        match request {
            LockRequest::Granted => {
                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone());

                if let Err(e) = index.delete(key, "2pl") {
                    self.abort(&meta).unwrap();
                    return Err(e);
                }
                Ok(())
            }
            LockRequest::Delay(pair) => {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }

                self.active_transactions
                    .get_mut(&meta.get_id().unwrap())
                    .unwrap()
                    .add_lock(key.clone());

                if let Err(e) = index.delete(key, "2pl") {
                    self.abort(&meta).unwrap();
                    return Err(e);
                }
                Ok(())
            }
            LockRequest::Denied => {
                let err = TwoPhaseLockingError::WriteLockRequestDenied(format!("{}", key));
                self.abort(&meta).unwrap();
                Err(err.into())
            }
        }
    }

    /// Commit a transaction.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        let rows = self
            .active_transactions
            .get_mut(&meta.get_id().unwrap())
            .unwrap()
            .get_rows_to_insert();

        match rows {
            Some(rows) => {
                debug!("Rows to insert for {}", &meta.get_id().unwrap());
                for row in rows.into_iter() {
                    let (index, row) = row;
                    let key = row.get_primary_key().unwrap();
                    debug!("Key: {}", key.clone());
                    // This insert can never fail.
                    index.insert(key.clone(), row).unwrap();
                }
            }
            None => debug!("No rows to insert for {}", &meta.get_id().unwrap()),
        }

        let at = self
            .active_transactions
            .remove(&meta.get_id().unwrap())
            .unwrap();
        debug!(
            "Removed {} from active transactions",
            &meta.get_id().unwrap()
        );

        debug!("Inserted dirty rows for {}", &meta.get_id().unwrap());
        // (4) Release locks.
        self.release_locks(&meta.get_id().unwrap(), at, true);
        debug!("Release all locks for {}", &meta.get_id().unwrap());

        Ok(())
    }

    /// Abort a transaction.
    fn abort(&self, meta: &TransactionInfo) -> crate::Result<()> {
        debug!("Abort transaction {:?}", meta.get_id().unwrap());
        //  Remove from active transactions.
        let at = self
            .active_transactions
            .remove(&meta.get_id().unwrap())
            .unwrap_or_else(|| {
                panic!(
                    "{} not found in active transaction",
                    &meta.get_id().unwrap()
                )
            });

        // Release locks.
        self.release_locks(&meta.get_id().unwrap(), at, false);

        Ok(())
    }

    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}

impl TwoPhaseLocking {
    /// Creates a new scheduler with an empty lock table.
    pub fn new(workload: Arc<Workload>) -> Self {
        let workers = workload
            .get_internals()
            .get_config()
            .get_int("workers")
            .unwrap() as usize;
        info!("Initialise 2pl with {} workers", workers);

        let lock_table = Arc::new(CHashMap::<PrimaryKey, LockInfo>::new());
        let active_transactions = Arc::new(CHashMap::<String, ActiveTransaction>::new());

        TwoPhaseLocking {
            id: Arc::new(Mutex::new(1)),
            lock_table,
            active_transactions,
            data: workload,
        }
    }

    /// Attempt to acquire lock.
    ///
    /// Takes a search key, desired lock mode, and transaction name and timestamp.
    /// If the lock is acquired `Granted` is returned.
    /// If the lock request is refused `Denied` is returned.
    /// If the lock request is delayed `Delayed` is returned.
    fn request_lock(
        &self,
        index: &str,
        key: PrimaryKey,
        request_mode: LockMode,
        tid: &str,
        tts: u64,
    ) -> LockRequest {
        let lock_info = self.lock_table.get_mut(&key); // attempt to get lock for a key
        let mut lock_info = match lock_info {
            // Lock exists.
            Some(lock) => lock,
            // No lock for this record, create a new one.
            None => {
                let mut lock_info = LockInfo::new(request_mode, tts, index); // Create new lock information.
                let entry = Entry::new(tid.to_string(), request_mode, None, tts); // Create new request entry.
                lock_info.add_entry(entry); // Attempt to insert into lock table

                match self.lock_table.insert(key.clone(), lock_info) {
                    // Error.
                    Some(existing_lock) => {
                        self.lock_table.insert(key, existing_lock); // Lock was concurrently created, back off.
                        return LockRequest::Denied;
                    }
                    // Inserted.
                    None => return LockRequest::Granted,
                }
            }
        };

        // The lock may be in use or not.
        // If in-use, consult the granted lock.
        if lock_info.granted.is_some() {
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
                LockMode::Read => {
                    // Current lock type.
                    match lock_info.group_mode.unwrap() {
                        LockMode::Read => {
                            let entry = Entry::new(tid.to_string(), LockMode::Read, None, tts); // Create new entry.
                            lock_info.add_entry(entry); // Add to holder/request list
                            if tts < lock_info.timestamp.unwrap() {
                                lock_info.timestamp = Some(tts); // Update lock timestamp if this lock request has lower timestamp
                            }
                            let held = lock_info.granted.unwrap(); // Increment locks held
                            lock_info.granted = Some(held + 1);

                            LockRequest::Granted
                        }
                        LockMode::Write => {
                            // Record locked with write lock, read lock can not be granted.
                            // Apply wait-die deadlock detection.

                            if lock_info.timestamp.unwrap() < tts {
                                return LockRequest::Denied;
                            }
                            // Only wait if all waiting write requests are older
                            if lock_info.list.iter().any(|e| {
                                e.timestamp > tts
                                    && e.lock_mode == LockMode::Write
                                    && e.timestamp != lock_info.timestamp.unwrap()
                            }) {
                                return LockRequest::Denied;
                            }

                            // Initialise a `Condvar` that will be used to sleep the thread.
                            let pair = Arc::new((Mutex::new(false), Condvar::new()));
                            // Create new entry for transaction request list.
                            let entry = Entry::new(
                                tid.to_string(),
                                LockMode::Read,
                                Some(Arc::clone(&pair)),
                                tts,
                            );
                            // Add to holder/request list.
                            lock_info.add_entry(entry);
                            // Set waiting transaction(s) flag on lock.
                            if !lock_info.waiting {
                                lock_info.waiting = true;
                            }
                            LockRequest::Delay(pair)
                        }
                    }
                }
                LockMode::Write => {
                    // Apply deadlock detection.

                    if tts > lock_info.timestamp.unwrap() {
                        return LockRequest::Denied;
                    }

                    // Only wait if all other waiting requests are older.
                    if lock_info
                        .list
                        .iter()
                        .any(|e| e.timestamp > tts && e.timestamp != lock_info.timestamp.unwrap())
                    {
                        return LockRequest::Denied;
                    }

                    // Initialise a `Condvar` that will be used to wake up thread.
                    let pair = Arc::new((Mutex::new(false), Condvar::new()));
                    // Create new entry.
                    let entry = Entry::new(
                        tid.to_string(),
                        LockMode::Write,
                        Some(Arc::clone(&pair)),
                        tts,
                    );
                    // Add to holder/request list.
                    lock_info.add_entry(entry);
                    // Set waiting request
                    if !lock_info.waiting {
                        lock_info.waiting = true;
                    }
                    LockRequest::Delay(pair)
                }
            }
        } else {
            // Lock information has been initialised but no locks have been granted.
            // Set group mode to request mode.
            lock_info.group_mode = Some(request_mode);
            // Create new entry for request.
            let entry = Entry::new(tid.to_string(), request_mode, None, tts);
            lock_info.add_entry(entry);
            // Set lock timestamp.
            lock_info.timestamp = Some(tts);
            // Increment granted.
            lock_info.granted = Some(1);
            LockRequest::Granted
        }
    }

    /// Release transaction `tid`s lock on row with `key`.
    ///
    /// Returns `Reclaim` if there are no concurrent locks or waiting lock
    /// requests. Else, returns `Ok`.
    fn release_lock(
        &self,
        key: PrimaryKey,
        tid: &str,
        commit: bool,
    ) -> Result<UnlockRequest, NonFatalError> {
        let th = thread::current(); // get handle to thread
        let th_id = th.name().unwrap(); // get thread id
        let t_id = tid;
        let mut lock_info = self.lock_table.get_mut(&key).ok_or_else(|| {
            NonFatalError::TwoPhaseLocking(TwoPhaseLockingError::LockNotInTable(format!(
                "{}",
                key.clone()
            )))
        })?;

        tracing::debug!(
            "Thread {}: Release lock: {:?} held by {}",
            th_id,
            lock_info,
            t_id
        );

        // Get index for this key's table.
        let index = self
            .data
            .get_internals()
            .get_index(&lock_info.index)
            .unwrap();

        // If write lock then commit or revert changes.
        if let LockMode::Write = lock_info.group_mode.unwrap() {
            if commit {
                index.commit(key.clone(), "2pl", tid).unwrap();
            } else {
                // TODO: could be reverting a row that does not exist if a create failed.
                match index.revert(key.clone(), "2pl", tid) {
                    Ok(_) => {}
                    Err(_) => {}
                }
            }
        }

        // If 1 granted lock and no waiting requests, reset lock and return.
        if lock_info.granted.unwrap() == 1 && !lock_info.waiting {
            lock_info.group_mode = None; // Reset group mode.
            lock_info.list.clear(); // Remove from list.
            lock_info.granted = None; // Set granted to 0.
            lock_info.timestamp = None; // Reset timestamp.
            lock_info.reclaimed = false; // Set reclaimed.

            return Ok(UnlockRequest::Reclaim);
        }

        // Find the entry for this lock request.
        // Assumption: only 1 request per transaction name and the lock request must exist
        // in list.
        let entry_index = lock_info.list.iter().position(|e| e.name == tid).unwrap();
        // Remove transactions entry.
        let entry = lock_info.list.remove(entry_index);

        match entry.lock_mode {
            // If record was locked with a Write lock then this is the only transaction with a lock
            // on this record. If so,
            // (i) Grant the next n read lock requests, or,
            // (ii) Grant the next write lock request
            LockMode::Write => {
                // Calculate waiting read requests.
                let mut read_requests = 0;
                for e in lock_info.list.iter() {
                    if e.lock_mode == LockMode::Write {
                        break;
                    }
                    read_requests += 1;
                }

                // If some reads requests first in queue.
                if read_requests != 0 {
                    // Wake up threads and determine new lock timestamp.
                    // let dt = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0);
                    // let mut new_lock_timestamp = DateTime::<Utc>::from_utc(dt, Utc);
                    let mut new_lock_timestamp = 0;
                    for e in lock_info.list.iter().take(read_requests) {
                        if e.timestamp > new_lock_timestamp {
                            new_lock_timestamp = e.timestamp;
                        }
                        // Clone handle to entry's Condvar.
                        let cond = Arc::clone(e.waiting.as_ref().unwrap());
                        // Destructure.
                        let (lock, cvar) = &*cond;
                        // Wake up thread.
                        let mut started = lock.lock().unwrap();
                        *started = true;
                        cvar.notify_all();
                    }
                    // Set new lock information.
                    // Highest read timestamp.
                    lock_info.timestamp = Some(new_lock_timestamp);
                    // Group mode.
                    lock_info.group_mode = Some(LockMode::Read);
                    // Granted n read requests.
                    lock_info.granted = Some(read_requests as u32);
                    // If all waiters have have been granted then set to false.
                    if lock_info.list.len() as u32 == lock_info.granted.unwrap() {
                        lock_info.waiting = false;
                    }
                } else {
                    // Set new lock information.
                    lock_info.timestamp = Some(lock_info.list[0].timestamp);
                    lock_info.group_mode = Some(LockMode::Write);
                    lock_info.granted = Some(1);
                    if lock_info.list.len() as u32 == lock_info.granted.unwrap() {
                        lock_info.waiting = false;
                    }
                    // Clone handle to entry's Condvar.
                    let cond = Arc::clone(lock_info.list[0].waiting.as_ref().unwrap());
                    // Destructure.
                    let (lock, cvar) = &*cond;
                    // Wake up thread.
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
                if lock_info.granted.unwrap() > 1 {
                    // removal of this lock still leaves the lock in read mode.
                    // update lock information
                    // highest remaining timestamp
                    // let dt = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0);
                    // let mut new_lock_timestamp = DateTime::<Utc>::from_utc(dt, Utc);
                    let mut new_lock_timestamp = 0;
                    for e in lock_info.list.iter() {
                        if e.timestamp > new_lock_timestamp {
                            new_lock_timestamp = e.timestamp;
                        }
                    }
                    lock_info.timestamp = Some(new_lock_timestamp);
                    // decrement locks held.
                    let held = lock_info.granted.unwrap();
                    lock_info.granted = Some(held - 1);
                } else {
                    // 1 active read lock and some write lock waiting.

                    if lock_info.waiting {
                        let next_write_entry_index = lock_info
                            .list
                            .iter()
                            .position(|e| e.lock_mode == LockMode::Write)
                            .unwrap();
                        // Set lock information to new write lock.
                        lock_info.group_mode = Some(LockMode::Write);
                        lock_info.timestamp =
                            Some(lock_info.list[next_write_entry_index].timestamp);
                        lock_info.granted = Some(1);
                        if lock_info.list.len() as u32 == lock_info.granted.unwrap() {
                            lock_info.waiting = false;
                        }
                        // Wake up thread.
                        let cond = Arc::clone(
                            lock_info.list[next_write_entry_index]
                                .waiting
                                .as_ref()
                                .unwrap(),
                        );
                        let (lock, cvar) = &*cond;
                        let mut started = lock.lock().unwrap();
                        *started = true;
                        cvar.notify_all();
                    }
                }
            }
        }
        Ok(UnlockRequest::Ok)
    }

    fn release_locks(&self, tid: &str, at: ActiveTransaction, commit: bool) {
        for lock in at.get_locks_held() {
            match self.release_lock(lock.clone(), tid, commit) {
                Ok(ur) => {
                    if let UnlockRequest::Reclaim = ur {
                        // self.lock_table.remove(lock);
                    }
                }
                Err(_) => {
                    // self.lock_table.remove(lock);
                }
            }
        }
    }
}

#[derive(Debug)]
enum LockRequest {
    Granted,
    Denied,
    Delay(Arc<(Mutex<bool>, Condvar)>),
}

#[derive(Debug, PartialEq)]
enum UnlockRequest {
    Ok,
    Reclaim,
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::workloads::tatp::keys::TatpPrimaryKey;
    use crate::workloads::tatp::loader;
    use crate::workloads::Internal;

    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::convert::TryInto;
    use std::thread;

    lazy_static! {
        // Create a scheduler and data.
        static ref TPL: Arc<TwoPhaseLocking> = {
            // Initialise configuration.
            let mut c = Config::default();
            c.merge(config::File::with_name("./tests/Test-tpl.toml")).unwrap();
            let config = Arc::new(c);

            // Workload with fixed seed.
            let schema = "./schema/tatp_schema.txt".to_string();
            let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
            let seed = config.get_int("seed").unwrap();
            let mut rng = StdRng::seed_from_u64(seed.try_into().unwrap());
            loader::populate_tables(&internals, &mut rng).unwrap();
            let workload = Arc::new(Workload::Tatp(internals));

            // Initialise scheduler.
            Arc::new(TwoPhaseLocking::new(workload))

        };
    }

    // Compare lock request types
    #[test]
    fn tpl_lock_request_type_test() {
        assert_eq!(LockRequest::Granted, LockRequest::Granted);
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        assert_eq!(
            LockRequest::Delay(Arc::clone(&pair)),
            LockRequest::Delay(Arc::clone(&pair))
        );
        assert_eq!(LockRequest::Denied, LockRequest::Denied);
        assert!(LockRequest::Granted != LockRequest::Denied);
    }

    // In this test 3 read locks are requested by Ta, Tb, and Tc, which are then released.
    #[test]
    fn tpl_request_lock_read_test() {
        // Get handle to 2PL scheduler.
        let tpl = Arc::clone(&TPL);

        // Register transactions.
        let ta = tpl.register().unwrap();
        let tb = tpl.register().unwrap();
        let tc = tpl.register().unwrap();

        // Row transactions will contend on.
        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2));

        // Request locks.
        assert_eq!(
            tpl.request_lock(
                index,
                pk.clone(),
                LockMode::Read,
                &ta.get_id().unwrap(),
                ta.get_ts().unwrap(),
            ),
            LockRequest::Granted
        );
        assert_eq!(
            tpl.request_lock(
                index,
                pk.clone(),
                LockMode::Read,
                &tb.get_id().unwrap(),
                tb.get_ts().unwrap(),
            ),
            LockRequest::Granted
        );
        assert_eq!(
            tpl.request_lock(
                index,
                pk.clone(),
                LockMode::Read,
                &tc.get_id().unwrap(),
                tc.get_ts().unwrap(),
            ),
            LockRequest::Granted
        );

        // Check
        {
            let lock = tpl.lock_table.get(&pk.clone()).unwrap();
            assert_eq!(
                lock.group_mode == Some(LockMode::Read)
                    && !lock.waiting
                    && lock.list.len() as u32 == 3
                    && lock.timestamp == Some(3)
                    && lock.granted == Some(3),
                true,
                "{}",
                *lock
            );
        }

        // Release locks.
        assert_eq!(
            tpl.release_lock(pk.clone(), &ta.get_id().unwrap(), true)
                .unwrap(),
            UnlockRequest::Ok
        );
        assert_eq!(
            tpl.release_lock(pk.clone(), &tb.get_id().unwrap(), true)
                .unwrap(),
            UnlockRequest::Ok
        );
        assert_eq!(
            tpl.release_lock(pk.clone(), &tc.get_id().unwrap(), true)
                .unwrap(),
            UnlockRequest::Reclaim
        );
        // Check.
        {
            let lock = tpl.lock_table.get(&pk.clone()).unwrap();
            assert_eq!(
                lock.group_mode == None
                    && !lock.waiting
                    && lock.list.len() as u32 == 0
                    && lock.timestamp == None
                    && lock.granted == None,
                true,
                "{}",
                *lock
            );
        }
        // Remove from active transactions.
        // (tid) Remove from active transactions.
        tpl.active_transactions
            .remove(&ta.get_id().unwrap())
            .unwrap();
        tpl.active_transactions
            .remove(&tb.get_id().unwrap())
            .unwrap();
        tpl.active_transactions
            .remove(&tc.get_id().unwrap())
            .unwrap();
    }

    // In this test a write lock is requested and then released.
    #[test]
    fn tpl_request_lock_write_test() {
        // Get handle to 2PL scheduler.
        let tpl = Arc::clone(&TPL);

        // Create transaction id and timestamp.
        let t = tpl.register().unwrap();

        // Row transaction will access.
        let index = "sub_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1));

        // Lock.
        assert_eq!(
            tpl.request_lock(
                index,
                pk.clone(),
                LockMode::Write,
                &t.get_id().unwrap(),
                t.get_ts().unwrap()
            ),
            LockRequest::Granted
        );
        // Check
        {
            let lock = tpl.lock_table.get(&pk.clone()).unwrap();
            assert_eq!(
                lock.group_mode == Some(LockMode::Write)
                    && !lock.waiting
                    && lock.list.len() as u32 == 1
                    && lock.timestamp == Some(t.get_ts().unwrap())
                    && lock.granted == Some(1)
                    && lock.list[0].lock_mode == LockMode::Write,
                true,
                "{:?}",
                lock
            );
        }

        // Unlock
        tpl.release_lock(pk.clone(), &t.get_id().unwrap(), true)
            .unwrap();
        {
            let lock = tpl.lock_table.get(&pk.clone()).unwrap();
            assert_eq!(
                lock.group_mode == None
                    && !lock.waiting
                    && lock.list.len() as u32 == 0
                    && lock.timestamp == None
                    && lock.granted == None,
                true,
                "{:?}",
                lock
            );
        }
        tpl.active_transactions
            .remove(&t.get_id().unwrap())
            .unwrap();
    }

    // In this test a read lock is taken by Ta, followed by a write lock by Tb, which delays
    // until the read lock is released by Ta.
    #[test]
    fn tpl_read_delay_write_test() {
        // Get handle to scheduler.
        let tpl = Arc::clone(&TPL);
        let tpl_h = Arc::clone(&TPL);

        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(2, 2));

        // Register transactions.
        let ta = tpl.register().unwrap();
        let tb = tpl.register().unwrap();

        let ta_h = ta.clone();
        let tb_h = tb.clone();
        let pk_h = pk.clone();

        // Spawn thread.
        let handle = thread::spawn(move || {
            // Tb gets read lock.
            assert_eq!(
                tpl_h.request_lock(
                    index,
                    pk_h.clone(),
                    LockMode::Read,
                    &tb_h.get_id().unwrap(),
                    tb_h.get_ts().unwrap(),
                ),
                LockRequest::Granted
            );

            // Ta gets read lock.
            debug!("Request Write lock");
            if let LockRequest::Delay(pair) = tpl_h.request_lock(
                index,
                pk_h.clone(),
                LockMode::Write,
                &ta_h.get_id().unwrap(),
                ta_h.get_ts().unwrap(),
            ) {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                // delays
            };
        });

        // Sleep this thread, giving ta time to delay.
        let ms = std::time::Duration::from_secs(2);
        thread::sleep(ms);
        // Release read lock

        assert_eq!(
            tpl.release_lock(pk.clone(), &tb.get_id().unwrap(), true)
                .unwrap(),
            UnlockRequest::Ok
        );
        // Check ta got the lock.
        let lock = tpl.lock_table.get(&pk.clone()).unwrap();
        assert_eq!(
            lock.group_mode == Some(LockMode::Write)
                && !lock.waiting
                && lock.list.len() as u32 == 1
                && lock.timestamp == Some(ta.get_ts().unwrap())
                && lock.granted == Some(1),
            true,
            "{}",
            *lock
        );

        handle.join().unwrap();
        tpl.active_transactions
            .remove(&ta.get_id().unwrap())
            .unwrap();
        tpl.active_transactions
            .remove(&tb.get_id().unwrap())
            .unwrap();
    }

    // In this test a write lock is taken by Ta, followed by a read lock by Tb, which delays
    // until the write lock is released by Ta.
    #[test]
    fn tpl_write_delay_read_test() {
        // Get handle to 2PL scheduler.
        let tpl = Arc::clone(&TPL);

        // Register transactions.
        let ta = tpl.register().unwrap();
        let tb = tpl.register().unwrap();

        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 1));

        let tpl_h = Arc::clone(&TPL);
        let ta_h = ta.clone();
        let tb_h = tb.clone();
        let pk_h = pk.clone();

        let handle = thread::spawn(move || {
            debug!("Request write lock by Tb");
            tpl_h.request_lock(
                index,
                pk_h.clone(),
                LockMode::Write,
                &tb_h.get_id().unwrap(),
                tb_h.get_ts().unwrap(),
            );

            debug!("Request read lock by Ta");
            let res = tpl_h.request_lock(
                index,
                pk_h.clone(),
                LockMode::Read,
                &ta_h.get_id().unwrap(),
                ta_h.get_ts().unwrap(),
            );

            // Assert it has been denied, use dummy pair.
            assert_eq!(
                res,
                LockRequest::Delay(Arc::new((Mutex::new(false), Condvar::new())))
            );
            if let LockRequest::Delay(pair) = res {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                debug!("Read lock granted to Ta");
            };
        });

        // Sleep thread
        let ms = std::time::Duration::from_secs(2);
        thread::sleep(ms);
        debug!("Write lock released by Ta");
        tpl.release_lock(pk.clone(), &tb.get_id().unwrap(), true)
            .unwrap();
        let lock = tpl.lock_table.get(&pk.clone()).unwrap();

        assert_eq!(
            lock.group_mode == Some(LockMode::Read)
                && !lock.waiting
                && lock.list.len() as u32 == 1
                && lock.timestamp == Some(ta.get_ts().unwrap())
                && lock.granted == Some(1),
            true,
            "{}",
            *lock
        );

        handle.join().unwrap();
        tpl.active_transactions
            .remove(&ta.get_id().unwrap())
            .unwrap();
        tpl.active_transactions
            .remove(&tb.get_id().unwrap())
            .unwrap();
    }

    // In this test a write lock is taken by Ta, followed by a write lock by Tb, which delays
    // until the write lock is released by Ta.
    #[test]
    fn tpl_write_delay_write_test() {
        let tpl = Arc::clone(&TPL);
        let ta = tpl.register().unwrap();
        let tb = tpl.register().unwrap();

        let tpl_h = Arc::clone(&TPL);
        let ta_h = ta.clone();
        let tb_h = tb.clone();

        let index = "sub_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(2));
        let pk_h = pk.clone();

        let handle = thread::spawn(move || {
            // Tb
            tpl_h.request_lock(
                index,
                pk_h.clone(),
                LockMode::Write,
                &tb_h.get_id().unwrap(),
                tb_h.get_ts().unwrap(),
            );

            // Ta
            let res = tpl_h.request_lock(
                index,
                pk_h.clone(),
                LockMode::Write,
                &ta_h.get_id().unwrap(),
                ta_h.get_ts().unwrap(),
            );

            // Assert it has been denied, use dummy pair.
            assert_eq!(
                res,
                LockRequest::Delay(Arc::new((Mutex::new(false), Condvar::new())))
            );
            if let LockRequest::Delay(pair) = res {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
            };
        });

        // Sleep thread
        let ms = std::time::Duration::from_secs(2);
        thread::sleep(ms);
        // Tb
        tpl.release_lock(pk.clone(), &tb.get_id().unwrap(), true)
            .unwrap();
        let lock = tpl.lock_table.get(&pk.clone()).unwrap();

        assert_eq!(
            lock.group_mode == Some(LockMode::Write)
                && !lock.waiting
                && lock.list.len() as u32 == 1
                && lock.timestamp == Some(ta.get_ts().unwrap())
                && lock.granted == Some(1),
            true,
            "{}",
            *lock
        );

        handle.join().unwrap();
        tpl.active_transactions
            .remove(&ta.get_id().unwrap())
            .unwrap();
        tpl.active_transactions
            .remove(&tb.get_id().unwrap())
            .unwrap();
    }

    //#[test]
    // fn tpl_denied_lock_test() {
    //     // Init scheduler.
    //     let tpl = Arc::clone(&TPL);
    //     let ta = tpl.register().unwrap();
    //     let tb = tpl.register().unwrap();
    //     let tc = tpl.register().unwrap();

    //     let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(3));
    //     let columns: Vec<&str> = vec!["bit_1"];
    //     let values_a: Vec<&str> = vec!["0"];
    //     let values_b: Vec<&str> = vec!["1"];
    //     // Write by Ta.
    //     assert_eq!(
    //         tpl.update("subscriber", pk.clone(), &columns, &values_a, ta.clone())
    //             .unwrap(),
    //         ()
    //     );

    //     // Write by Tb
    //     assert_eq!(
    //         format!(
    //             "{}",
    //             tpl.update("subscriber", pk.clone(), &columns, &values_b, tb)
    //                 .unwrap_err()
    //         ),
    //         "write lock for Subscriber(3) denied"
    //     );

    //     // Write by Tc
    //     assert_eq!(
    //         format!(
    //             "{}",
    //             tpl.read("subscriber", pk.clone(), &columns, tc)
    //                 .unwrap_err()
    //         ),
    //         "read lock for Subscriber(3) denied"
    //     );

    //     // Commit Ta
    //     assert_eq!(tpl.commit(ta).unwrap(), ());
    // }
}

impl fmt::Display for TwoPhaseLocking {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "2PL")
    }
}
