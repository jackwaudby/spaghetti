use crate::server::scheduler::two_phase_locking::active_transaction::ActiveTransaction;
use crate::server::scheduler::two_phase_locking::error::{
    TwoPhaseLockingError, TwoPhaseLockingErrorKind,
};
use crate::server::scheduler::two_phase_locking::lock_info::{Entry, LockInfo, LockMode};
use crate::server::scheduler::{Aborted, Scheduler};
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::row::Row;
use crate::server::storage::table::Table;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use chashmap::CHashMap;
use chrono::{DateTime, NaiveDate, Utc};
use std::sync::{Arc, Condvar, Mutex};
use tracing::debug;

pub mod error;

pub mod lock_info;

pub mod active_transaction;

/// Represents a 2PL scheduler.
pub struct TwoPhaseLocking {
    /// Map of database records  to their lock information.
    lock_table: Arc<CHashMap<PrimaryKey, LockInfo>>,

    /// Map of transaction ids to neccessary runtime information.
    active_transactions: Arc<CHashMap<String, ActiveTransaction>>,

    /// Handle to storage layer.
    pub data: Arc<Workload>,
}

impl Scheduler for TwoPhaseLocking {
    /// Register a transaction with the scheduler.
    ///
    /// # Aborts
    ///
    /// A transaction with the same name is already registered.
    fn register(&self, tid: &str) -> Result<(), Aborted> {
        debug!("Register {}", tid);
        // Create runtime tracker.
        let at = ActiveTransaction::new(tid);
        // Add to map.
        if let Some(_) = self.active_transactions.insert(tid.to_string(), at) {
            let err = TwoPhaseLockingError::new(
                TwoPhaseLockingErrorKind::AlreadyRegisteredInActiveTransactions,
            );
            return Err(Aborted {
                reason: format!("{}", err),
            });
        }

        Ok(())
    }

    /// Attempt to create a row in a table.
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
        tid: &str,
        _tts: DateTime<Utc>,
    ) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, tid)?;
        // Init row.
        let mut row = Row::new(Arc::clone(&table), "2pl");
        // Set pk.
        row.set_primary_key(key);
        // Init values.
        for (i, column) in columns.iter().enumerate() {
            match row.init_value(column, &values[i].to_string()) {
                Ok(_) => {}
                Err(e) => {
                    self.abort(tid).unwrap();
                    return Err(Aborted {
                        reason: format!("{}", e),
                    });
                }
            }
        }
        // Get Index
        let index = self.get_index(table, tid)?;

        // Set values - Needed to make the row "dirty"
        match row.set_values(columns, values, "2pl", tid) {
            Ok(_) => {}
            Err(e) => {
                self.abort(tid).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        }

        // Register
        self.active_transactions
            .get_mut(tid)
            .unwrap()
            .add_row_to_insert(index, row);

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
        tid: &str,
        tts: DateTime<Utc>,
    ) -> Result<Vec<Data>, Aborted> {
        debug!("{} requesting read lock on {:?}", tid, key);
        // Get table.
        let table = self.get_table(table, tid)?;

        // Get primary index name.
        let index_name = self.get_index_name(Arc::clone(&table), tid)?;

        // Get read lock on row with pk `key`.
        let request = self.request_lock(&index_name, key, LockMode::Read, tid, tts);

        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), tid)?;

        match request {
            LockRequest::Granted => {
                debug!("Read lock for {:?} granted to transaction {:?}", key, tid);
                // Execute read operation.
                let result = index.read(key, columns, "2pl", tid);
                // Register lock.
                self.active_transactions.get_mut(tid).unwrap().add_lock(key);
                match result {
                    Ok(res) => {
                        // Get values.
                        let vals = res.get_values().unwrap();
                        return Ok(vals);
                    }
                    Err(e) => {
                        self.abort(tid).unwrap();
                        return Err(Aborted {
                            reason: format!("{}", e),
                        });
                    }
                }
            }

            LockRequest::Delay(pair) => {
                debug!("Waiting for read lock");
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                debug!("Read lock granted");
                // Register lock.
                self.active_transactions.get_mut(tid).unwrap().add_lock(key);

                // Execute read operation.
                let result = index.read(key, columns, "2pl", tid);

                match result {
                    Ok(res) => {
                        // Get values.
                        let vals = res.get_values().unwrap();
                        return Ok(vals);
                    }
                    Err(e) => {
                        self.abort(tid).unwrap();
                        return Err(Aborted {
                            reason: format!("{}", e),
                        });
                    }
                }
            }

            LockRequest::Denied => {
                debug!("Read lock denied");
                let err = TwoPhaseLockingError::new(TwoPhaseLockingErrorKind::LockRequestDenied);
                self.abort(tid).unwrap();
                return Err(Aborted {
                    reason: format!("{}", err),
                });
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
        columns: &Vec<&str>,
        values: &Vec<&str>,
        tid: &str,
        tts: DateTime<Utc>,
    ) -> Result<(), Aborted> {
        debug!("Transaction {:?} requesting write lock on {:?}", tid, key);

        // Get table.
        let table = self.get_table(table, tid)?;

        // Get primary index name.
        let index_name = self.get_index_name(Arc::clone(&table), tid)?;

        // Request lock.
        let request = self.request_lock(&index_name, key, LockMode::Write, tid, tts);

        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), tid)?;

        match request {
            LockRequest::Granted => {
                debug!("Write lock for {:?} granted to transaction {:?}", key, tid);
                // Register lock.
                self.active_transactions.get_mut(tid).unwrap().add_lock(key);

                // Execute update.
                let result = index.update(key, columns, values, "2pl", tid);
                match result {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        self.abort(tid).unwrap();
                        return Err(Aborted {
                            reason: format!("{}", e),
                        });
                    }
                }
            }
            LockRequest::Delay(pair) => {
                debug!("Waiting for write lock");
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                debug!("Write lock granted");
                // Register lock.
                self.active_transactions.get_mut(tid).unwrap().add_lock(key);
                // Execute update.
                let result = index.update(key, columns, values, "2pl", tid);
                match result {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        self.abort(tid).unwrap();
                        return Err(Aborted {
                            reason: format!("{}", e),
                        });
                    }
                }
            }
            LockRequest::Denied => {
                debug!("Write lock denied");
                let err = TwoPhaseLockingError::new(TwoPhaseLockingErrorKind::LockRequestDenied);
                self.abort(tid).unwrap();
                return Err(Aborted {
                    reason: format!("{}", err),
                });
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
        tid: &str,
        tts: DateTime<Utc>,
    ) -> Result<(), Aborted> {
        // Get table.
        let table = self.get_table(table, tid)?;

        // Get primary index name.
        let index_name = self.get_index_name(Arc::clone(&table), tid)?;

        // Request lock.
        let request = self.request_lock(&index_name, key, LockMode::Write, tid, tts);

        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), tid)?;

        match request {
            LockRequest::Granted => {
                debug!("Write lock for {:?} granted to transaction {:?}", key, tid);

                // Register lock.
                self.active_transactions.get_mut(tid).unwrap().add_lock(key);

                // Execute delete.
                let result = index.delete(key, "2pl");
                match result {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        self.abort(tid).unwrap();
                        return Err(Aborted {
                            reason: format!("{}", e),
                        });
                    }
                }
            }
            LockRequest::Delay(pair) => {
                debug!("Waiting for write lock");
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                debug!("Write lock granted");

                // Register lock.
                self.active_transactions.get_mut(tid).unwrap().add_lock(key);

                // Execute delete.
                let result = index.delete(key, "2pl");
                match result {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        self.abort(tid).unwrap();
                        return Err(Aborted {
                            reason: format!("{}", e),
                        });
                    }
                }
            }
            LockRequest::Denied => {
                debug!("Write lock denied");
                let err = TwoPhaseLockingError::new(TwoPhaseLockingErrorKind::LockRequestDenied);
                self.abort(tid).unwrap();
                return Err(Aborted {
                    reason: format!("{}", err),
                });
            }
        }
    }

    /// Commit a transaction.
    fn commit(&self, tid: &str) -> Result<(), Aborted> {
        debug!("Commit transaction {:?}", tid);
        // (tid) Remove from active transactions.
        let mut at = self.active_transactions.remove(tid).unwrap();

        // (2) Insert dirty rows
        for row in at.get_rows_to_insert().into_iter() {
            let (index, row) = row;
            let key = row.get_primary_key().unwrap();
            index.insert(key, row).unwrap();
        }

        // (4) Release locks.
        self.release_locks(tid, at, true);

        Ok(())
    }
    // TODO
    /// Abort a transaction.
    fn abort(&self, tid: &str) -> crate::Result<()> {
        debug!("Abort transaction {:?}", tid);
        //  Remove from active transactions.
        let at = self.active_transactions.remove(tid).unwrap();

        // Release locks.
        self.release_locks(tid, at, false);

        Ok(())
    }
}

impl TwoPhaseLocking {
    /// Creates a new scheduler with an empty lock table.
    pub fn new(workload: Arc<Workload>) -> TwoPhaseLocking {
        let lock_table = Arc::new(CHashMap::<PrimaryKey, LockInfo>::new());
        let active_transactions = Arc::new(CHashMap::<String, ActiveTransaction>::new());

        TwoPhaseLocking {
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
        tts: DateTime<Utc>,
    ) -> LockRequest {
        // If the lock table does not contain lock information for this record, insert lock information, and grant lock.
        if !self.lock_table.contains_key(&key) {
            // Create new lock information.
            let mut lock_info = LockInfo::new(request_mode, tts, index);
            // Create new request entry.
            let entry = Entry::new(tid.to_string(), request_mode, None, tts);
            lock_info.add_entry(entry);
            // Insert into lock table
            self.lock_table.insert_new(key, lock_info);
            return LockRequest::Granted;
        }

        // Else a lock exists for this record.
        // Retrieve the lock information.
        let mut lock_info = self.lock_table.get_mut(&key).unwrap();

        debug!("{}", lock_info.reclaimed);
        // Check not in process of being reclaimed.
        if lock_info.reclaimed {
            return LockRequest::Denied;
        }

        // The lock may be in use or not.
        // If in-use, consult the granted lock.
        if let Some(_) = lock_info.granted {
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
                            // Create new entry.
                            let entry = Entry::new(tid.to_string(), LockMode::Read, None, tts);
                            // Add to holder/request list.
                            lock_info.add_entry(entry);
                            // Update lock timestamp if this lock request has higher timestamp
                            if tts > lock_info.timestamp.unwrap() {
                                lock_info.timestamp = Some(tts);
                            }
                            // Increment locks held
                            let held = lock_info.granted.unwrap();
                            lock_info.granted = Some(held + 1);
                            // Grant lock.
                            return LockRequest::Granted;
                        }
                        LockMode::Write => {
                            // Record locked with write lock, read lock can not be granted.
                            // Apply wait-die deadlock detection.
                            if lock_info.timestamp.unwrap() < tts {
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
                            return LockRequest::Delay(pair);
                        }
                    }
                }
                LockMode::Write => {
                    // Apply deadlock detection.
                    if tts > lock_info.timestamp.unwrap() {
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
                    return LockRequest::Delay(pair);
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
            return LockRequest::Granted;
        }
    }

    fn release_lock(
        &self,
        key: PrimaryKey,
        tid: &str,
        commit: bool,
    ) -> crate::Result<UnlockRequest> {
        debug!("Release {}'s lock on {}", tid, key);
        let mut lock_info = self.lock_table.get_mut(&key).unwrap();

        // Get index for this key's table.
        let index = self
            .data
            .get_internals()
            .indexes
            .get(&lock_info.index)
            .unwrap();

        // If write lock then commit or revert changes.
        if let LockMode::Write = lock_info.group_mode.unwrap() {
            if commit {
                index.commit(key, "2pl", tid).unwrap();
            } else {
                index.revert(key, "2pl", tid)?;
            }
        }

        // If 1 granted lock and no waiting requests, reset lock and return.
        if lock_info.granted.unwrap() == 1 && !lock_info.waiting {
            debug!("1 lock held and no waiting locks");
            // Reset group mode.
            lock_info.group_mode = None;
            // Remove from list.
            lock_info.list.clear();
            // Set granted to 0.
            lock_info.granted = None;
            // Reset timestamp.
            lock_info.timestamp = None;
            // Set reclaimed.
            lock_info.reclaimed = true;
            debug!("set to default");
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
                debug!("{} read requests waiting on lock", read_requests);
                // If some reads requests first in queue.
                if read_requests != 0 {
                    // Wake up threads and determine new lock timestamp.
                    let dt = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0);
                    let mut new_lock_timestamp = DateTime::<Utc>::from_utc(dt, Utc);
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
                    // Next lock request is a Write request.
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
                    let dt = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0);
                    let mut new_lock_timestamp = DateTime::<Utc>::from_utc(dt, Utc);
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
                    debug!("Waiting Write lock(s): {}", lock_info.waiting);
                    if lock_info.waiting {
                        debug!("Grant a Write lock");
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
            match self.release_lock(*lock, tid, commit) {
                Ok(ur) => {
                    if let UnlockRequest::Reclaim = ur {
                        self.lock_table.remove(lock);
                    }
                }
                Err(_xb) => {
                    self.lock_table.remove(lock);
                }
            }
        }
        debug!("All locks released for {:?}", tid);
    }

    /// Get shared reference to a table.
    fn get_table(&self, table: &str, tid: &str) -> Result<Arc<Table>, Aborted> {
        // Get table.
        let res = self.data.get_internals().get_table(table);
        match res {
            Ok(table) => Ok(table),
            Err(e) => {
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get primary index name on a table.
    fn get_index_name(&self, table: Arc<Table>, tid: &str) -> Result<String, Aborted> {
        let res = table.get_primary_index();
        match res {
            Ok(index_name) => Ok(index_name),
            Err(e) => {
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get shared reference to index for a table.
    fn get_index(&self, table: Arc<Table>, tid: &str) -> Result<Arc<Index>, Aborted> {
        // Get index name.
        let index_name = self.get_index_name(table, tid)?;

        // Get index for this key's table.
        let res = self.data.get_internals().get_index(&index_name);
        match res {
            Ok(index) => Ok(index),
            Err(e) => {
                self.abort(tid).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
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

#[derive(Debug)]
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
    use crate::server::scheduler::Protocol;
    use crate::workloads::tatp::keys::TatpPrimaryKey;
    use crate::workloads::tatp::loader;
    use crate::workloads::Internal;
    use chrono::Duration;
    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::Once;
    use std::thread;
    use std::time;
    use std::time::SystemTime;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    static LOG: Once = Once::new();

    fn logging(on: bool) {
        if on {
            LOG.call_once(|| {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting default subscriber failed");
            });
        }
    }

    lazy_static! {
        static ref WORKLOAD: Arc<Workload> = {
            // Initialise configuration.
            let mut c = Config::default();
            c.merge(config::File::with_name("Test.toml")).unwrap();
            let config = Arc::new(c);
            let internals = Internal::new("tatp_schema.txt", config).unwrap();
            let mut rng = StdRng::seed_from_u64(42);
            loader::populate_tables(&internals, &mut rng).unwrap();
            let workload = Arc::new(Workload::Tatp(internals));
            workload
        };
    }

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

    #[test]
    fn tpl_register_test() {
        logging(false);
        // Initialise scheduler.
        let protocol = Arc::new(Protocol::new(Arc::clone(&WORKLOAD)).unwrap());
        // Register transaction.
        assert_eq!(protocol.scheduler.register("some_id").unwrap(), ());
        assert_eq!(
            format!("{}", protocol.scheduler.register("some_id").unwrap_err()),
            "Aborted: transaction already registered in active transaction table"
        );
    }

    // In this test 3 read locks are requested by Ta, Tb, and Tc, which are then released.
    #[test]
    fn tpl_request_lock_read_test() {
        logging(false);

        // Initialise scheduler
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));
        let protocol1 = protocol.clone();

        let sys_time = SystemTime::now();

        // Ta
        let ta_ts: DateTime<Utc> = sys_time.into();
        let ta_id = ta_ts.to_string();
        // Tb (younger)
        let tb_ts: DateTime<Utc> = ta_ts - Duration::seconds(2);
        let tb_id = tb_ts.to_string();
        // Tc (older)
        let tc_ts: DateTime<Utc> = ta_ts + Duration::seconds(2);
        let tc_id = tc_ts.to_string();

        // Register transactions
        protocol.register(&ta_id).unwrap();
        protocol.register(&tb_id).unwrap();
        protocol.register(&tc_id).unwrap();

        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2));

        // Request locks.
        assert_eq!(
            protocol.request_lock(index, pk, LockMode::Read, &ta_id, ta_ts),
            LockRequest::Granted
        );
        assert_eq!(
            protocol.request_lock(index, pk, LockMode::Read, &tb_id, tb_ts),
            LockRequest::Granted
        );
        assert_eq!(
            protocol.request_lock(index, pk, LockMode::Read, &tc_id, tc_ts),
            LockRequest::Granted
        );
        // Check
        {
            let lock = protocol1.lock_table.get(&pk).unwrap();
            assert_eq!(
                lock.group_mode == Some(LockMode::Read)
                    && !lock.waiting
                    && lock.list.len() as u32 == 3
                    && lock.timestamp == Some(tc_ts)
                    && lock.granted == Some(3),
                true,
                "{}",
                *lock
            );
        }

        // Release locks.
        protocol.release_lock(pk, &ta_id, true).unwrap();
        protocol.release_lock(pk, &tb_id, true).unwrap();
        protocol.release_lock(pk, &tc_id, true).unwrap();
        // Check
        {
            let lock = protocol1.lock_table.get(&pk).unwrap();
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
    }

    // In this test a write lock is requested and then released.
    #[test]
    fn tpl_request_lock_write_test() {
        logging(false);

        // Initialise protocol
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));
        let protocol1 = protocol.clone();

        // Create transaction id and timestamp.
        let sys_time = SystemTime::now();
        let datetime: DateTime<Utc> = sys_time.into();
        let t_id = datetime.to_string();
        let t_ts = datetime;

        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2));
        // Lock
        protocol.register(&t_id).unwrap();
        let req = protocol.request_lock(index, pk, LockMode::Write, &t_id, t_ts);
        assert_eq!(req, LockRequest::Granted);

        {
            let lock = protocol1.lock_table.get(&pk).unwrap();
            assert_eq!(
                lock.group_mode == Some(LockMode::Write)
                    && !lock.waiting
                    && lock.list.len() as u32 == 1
                    && lock.timestamp == Some(t_ts)
                    && lock.granted == Some(1)
                    && lock.list[0].lock_mode == LockMode::Write,
                true,
                "{:?}",
                lock
            );
        }

        // Unlock
        protocol.release_lock(pk, &t_id, true).unwrap();
        {
            let lock = protocol1.lock_table.get(&pk).unwrap();
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
    }

    // In this test a read lock is taken by Ta, followed by a write lock by Tb, which delays
    // until the read lock is released by Ta.
    #[test]
    fn tpl_read_delay_write_test() {
        logging(false);
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));
        let protocol1 = protocol.clone();

        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2));

        // Create transaction id and timestamp.
        let sys_time_1 = SystemTime::now();
        let datetime_1: DateTime<Utc> = sys_time_1.into();
        let t_id_1 = datetime_1.to_string();
        let t_id_11 = t_id_1.clone();
        let t_ts_1 = datetime_1;

        let datetime_2: DateTime<Utc> = datetime_1 - Duration::seconds(2);
        let t_id_2 = datetime_2.to_string();
        let t_ts_2 = datetime_2;

        protocol.register(&t_id_1).unwrap();
        protocol.register(&t_id_2).unwrap();

        let _handle = thread::spawn(move || {
            debug!("Request Read lock");
            protocol1.request_lock(index, pk, LockMode::Read, &t_id_11, t_ts_1);
            debug!("Request Write lock");
            if let LockRequest::Delay(pair) =
                protocol1.request_lock(index, pk, LockMode::Write, &t_id_2, t_ts_2)
            {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                debug!("Write lock granted");
            };
        });

        let ms = time::Duration::from_secs(2);
        thread::sleep(ms);
        protocol.release_lock(pk, &t_id_1, true).unwrap();
        let lock = protocol.lock_table.get(&pk).unwrap();
        assert_eq!(
            lock.group_mode == Some(LockMode::Write)
                && !lock.waiting
                && lock.list.len() as u32 == 1
                && lock.timestamp == Some(t_ts_2)
                && lock.granted == Some(1),
            true,
            "{}",
            *lock
        );
    }

    // In this test a write lock is taken by Ta, followed by a read lock by Tb, which delays
    // until the write lock is released by Ta.
    #[test]
    fn tpl_write_delay_read_test() {
        logging(false);
        // Scheduler.
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));
        // Handle for thread.
        let protocol_t = protocol.clone();

        // Transaction A
        let sys_time = SystemTime::now();
        let ta_ts: DateTime<Utc> = sys_time.into();
        let ta_id = ta_ts.to_string();
        let ta_id_t = ta_id.clone();

        // Transaction B
        // Timestamp is smaller/younger so it waits for lock.
        let tb_ts: DateTime<Utc> = ta_ts - Duration::seconds(2);
        let tb_id = tb_ts.to_string();

        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2));

        // Register with scheduler
        protocol.register(&ta_id).unwrap();
        protocol.register(&tb_id).unwrap();

        let _handle = thread::spawn(move || {
            debug!("Request write lock by Ta");
            protocol_t.request_lock(index, pk, LockMode::Write, &ta_id_t, ta_ts);
            debug!("Request read lock by Tb");
            let res = protocol_t.request_lock(index, pk, LockMode::Read, &tb_id, tb_ts);
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
                debug!("Read lock granted to Tb");
            };
        });

        // Sleep thread
        let ms = time::Duration::from_secs(2);
        thread::sleep(ms);
        debug!("Write lock released by Ta");
        protocol.release_lock(pk, &ta_id, true).unwrap();
        let lock = protocol.lock_table.get(&pk).unwrap();

        assert_eq!(
            lock.group_mode == Some(LockMode::Read)
                && !lock.waiting
                && lock.list.len() as u32 == 1
                && lock.timestamp == Some(tb_ts)
                && lock.granted == Some(1),
            true,
            "{}",
            *lock
        );
    }

    // In this test a write lock is taken by Ta, followed by a write lock by Tb, which delays
    // until the write lock is released by Ta.
    #[test]
    fn tpl_write_delay_write_test() {
        logging(false);
        // Scheduler.
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));
        // Handle for thread.
        let protocol_t = protocol.clone();

        // Transaction A
        let sys_time = SystemTime::now();
        let ta_ts: DateTime<Utc> = sys_time.into();
        let ta_id = ta_ts.to_string();
        let ta_id_t = ta_id.clone();

        // Transaction B
        // Timestamp is smaller/younger so it waits for lock.
        let tb_ts: DateTime<Utc> = ta_ts - Duration::seconds(2);
        let tb_id = tb_ts.to_string();

        let index = "access_idx";
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2));

        // Register with scheduler
        protocol.register(&ta_id).unwrap();
        protocol.register(&tb_id).unwrap();

        let _handle = thread::spawn(move || {
            debug!("Request write lock by Ta");
            protocol_t.request_lock(index, pk, LockMode::Write, &ta_id_t, ta_ts);
            debug!("Request write lock by Tb");
            let res = protocol_t.request_lock(index, pk, LockMode::Write, &tb_id, tb_ts);
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
                debug!("Read lock granted to Tb");
            };
        });

        // Sleep thread
        let ms = time::Duration::from_secs(2);
        thread::sleep(ms);
        debug!("Write lock released by Ta");
        protocol.release_lock(pk, &ta_id, true).unwrap();
        let lock = protocol.lock_table.get(&pk).unwrap();

        assert_eq!(
            lock.group_mode == Some(LockMode::Write)
                && !lock.waiting
                && lock.list.len() as u32 == 1
                && lock.timestamp == Some(tb_ts)
                && lock.granted == Some(1),
            true,
            "{}",
            *lock
        );
    }

    #[test]
    fn tpl_denied_lock_test() {
        logging(false);
        // Init scheduler.
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));

        // Create transaction ids and timestamps.
        let sys_time = SystemTime::now();
        let datetime: DateTime<Utc> = sys_time.into();
        let t_id = datetime.to_string();

        // Read with higher/newer ts.
        let datetime_r: DateTime<Utc> = datetime + Duration::seconds(2);
        let t_id_r = datetime_r.to_string();

        // Write with higher/newer ts.
        let datetime_w: DateTime<Utc> = datetime + Duration::seconds(5);
        let t_id_w = datetime_w.to_string();

        // Register transactions.
        assert_eq!(protocol.register(&t_id).unwrap(), ());
        assert_eq!(protocol.register(&t_id_r).unwrap(), ());
        assert_eq!(protocol.register(&t_id_w).unwrap(), ());

        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1));
        let columns: Vec<&str> = vec!["bit_1"];
        let values_a: Vec<&str> = vec!["0"];
        let values_b: Vec<&str> = vec!["1"];
        // Write by Ta.
        assert_eq!(
            protocol
                .update("subscriber", pk, &columns, &values_a, &t_id, datetime)
                .unwrap(),
            ()
        );

        // Write by Tb
        assert_eq!(
            format!(
                "{}",
                protocol
                    .update("subscriber", pk, &columns, &values_b, &t_id_w, datetime_w)
                    .unwrap_err()
            ),
            "Aborted: lock request denied"
        );

        // Write by Tc
        assert_eq!(
            format!(
                "{}",
                protocol
                    .read("subscriber", pk, &columns, &t_id_r, datetime_r)
                    .unwrap_err()
            ),
            "Aborted: lock request denied"
        );

        // Commit Ta
        assert_eq!(protocol.commit(&t_id).unwrap(), ());

        assert_eq!(protocol.lock_table.is_empty(), true);
    }
}
