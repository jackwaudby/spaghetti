use crate::server::scheduler::two_phase_locking::error::{
    TwoPhaseLockingError, TwoPhaseLockingErrorKind,
};
use crate::server::scheduler::Scheduler;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::Row;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;
use crate::Result;

use chashmap::CHashMap;
use chrono::{DateTime, NaiveDate, Utc};
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use tracing::debug;

pub mod error;

/// Represents an operation scheduler.
#[derive(Debug)]
pub struct TwoPhaseLocking {
    /// Map of databse record ids to their lock information.
    lock_table: Arc<CHashMap<String, LockInfo>>,
    /// Map of active transactions to the locks they hold.
    active_transactions: Arc<CHashMap<String, Vec<String>>>,
    /// Handle to storage layer.
    pub data: Arc<Workload>,
}

impl Scheduler for TwoPhaseLocking {
    /// Register a transaction with the scheduler.
    ///
    /// # Errors
    ///
    /// If a transaction with the same name is already registered returns
    /// `TransactionAlreadyRegistered`.
    fn register(&self, transaction_name: &str) -> Result<()> {
        debug!("Register transaction {:?} with scheduler", transaction_name);
        match self
            .active_transactions
            .insert(transaction_name.to_string(), Vec::new())
        {
            Some(_) => Err(Box::new(TwoPhaseLockingError::new(
                TwoPhaseLockingErrorKind::AlreadyRegisteredInActiveTransactions,
            ))),
            None => Ok(()),
        }
    }

    /// Attempt to read a `Row`.
    ///
    /// If the read operation fails, an error is returned.
    fn read(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<Vec<Data>> {
        debug!(
            "Transaction {:?} requesting read lock on {:?}",
            transaction_name, key
        );
        let req = self.request_lock(
            &key.to_string(),
            LockMode::Read,
            transaction_name,
            transaction_ts,
        );
        match req {
            LockRequest::Granted => {
                debug!(
                    "Read lock for {:?} granted to transaction {:?}",
                    key, transaction_name
                );
                let index = self.data.get_internals().indexes.get(index).unwrap();
                let vals = match index.index_read(key, columns) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("Abort transaction {:?}: {:?}", transaction_name, e);
                        self.release_locks(transaction_name);
                        self.cleanup(transaction_name);
                        return Err(e);
                    }
                };
                Ok(vals)
            }
            LockRequest::Delay(pair) => {
                debug!("Waiting for read lock");
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                debug!("Read lock granted");
                let index = self.data.get_internals().indexes.get(index).unwrap();
                let vals = match index.index_read(key, columns) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("Abort transaction {:?}: {:?}", transaction_name, e);
                        self.release_locks(transaction_name);
                        self.cleanup(transaction_name);
                        return Err(e);
                    }
                };

                Ok(vals)
            }
            LockRequest::Denied => {
                debug!("Read lock denied");
                self.cleanup(transaction_name);
                Err(Box::new(TwoPhaseLockingError::new(
                    TwoPhaseLockingErrorKind::LockRequestDenied,
                )))
            }
        }
    }
    fn commit(&self, transaction_name: &str) {
        debug!("Commit transaction {:?}", transaction_name);
        self.release_locks(transaction_name);
        self.cleanup(transaction_name);
    }

    fn insert(
        &self,
        table: &str,
        pk: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        transaction_name: &str,
    ) -> Result<()> {
        // Initialise empty row.
        let table = self.data.get_internals().get_table(table)?;
        let mut row = Row::new(Arc::clone(&table));
        // Set PK.
        row.set_primary_key(pk);
        // Set values.
        for (i, column) in columns.iter().enumerate() {
            row.set_value(column, &values[i].to_string())?;
        }
        // Get index.
        let index = table.get_primary_index()?;
        let index = self.data.get_internals().indexes.get(&index[..]).unwrap();
        // Attempt to insert row.
        match index.index_insert(pk, row) {
            Ok(_) => Ok(()),
            Err(e) => {
                self.cleanup(transaction_name);
                Err(e)
            }
        }
    }

    fn delete(&self, index: &str, pk: PrimaryKey, transaction_name: &str) -> Result<()> {
        // Get index.
        let index = self.data.get_internals().indexes.get(index).unwrap();
        // Attempt to remove row.
        match index.index_remove(pk) {
            Ok(_) => Ok(()),
            Err(e) => {
                self.cleanup(transaction_name);
                Err(e)
            }
        }
    }

    fn write(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<()> {
        debug!(
            "Transaction {:?} requesting write lock on {:?}",
            transaction_name, key
        );
        let req = self.request_lock(
            &key.to_string(),
            LockMode::Write,
            transaction_name,
            transaction_ts,
        );

        match req {
            LockRequest::Granted => {
                debug!(
                    "Write lock for {:?} granted to transaction {:?}",
                    key, transaction_name
                );
                let index = self.data.get_internals().indexes.get(index).unwrap();
                let vals = match index.index_write(key, columns, values) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("Abort transaction {:?}: {:?}", transaction_name, e);
                        self.release_locks(transaction_name);
                        self.cleanup(transaction_name);
                        return Err(e);
                    }
                };

                Ok(vals)
            }
            LockRequest::Delay(pair) => {
                debug!("Waiting for write lock");
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
                debug!("Write lock granted");
                let index = self.data.get_internals().indexes.get(index).unwrap();
                let vals = match index.index_write(key, columns, values) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("Abort transaction {:?}: {:?}", transaction_name, e);
                        self.release_locks(transaction_name);
                        self.cleanup(transaction_name);
                        return Err(e);
                    }
                };

                Ok(vals)
            }
            LockRequest::Denied => {
                debug!("Write lock denied");
                self.cleanup(transaction_name);
                Err(Box::new(TwoPhaseLockingError::new(
                    TwoPhaseLockingErrorKind::LockRequestDenied,
                )))
            }
        }
    }

    // fn get_internals(&self) -> &Internal {
    //     self.data.get_internals()
    // }
}

impl TwoPhaseLocking {
    /// Creates a new scheduler with an empty lock table.
    pub fn new(workload: Arc<Workload>) -> TwoPhaseLocking {
        let lock_table = Arc::new(CHashMap::<String, LockInfo>::new());
        let active_transactions = Arc::new(CHashMap::<String, Vec<String>>::new());

        TwoPhaseLocking {
            lock_table,
            active_transactions,
            data: workload,
        }
    }

    /// Register lock with a transaction.
    fn register_lock(&self, transaction_name: &str, key: &str) -> Result<()> {
        // Attempt to get mutable value for transaction.
        let locks_held = self.active_transactions.get_mut(transaction_name);

        match locks_held {
            Some(mut wg) => {
                wg.push(key.to_string());
                debug!(
                    "Register lock for {:?} with transaction {:?}",
                    key, transaction_name
                );
                Ok(())
            }
            None => Err(Box::new(TwoPhaseLockingError::new(
                TwoPhaseLockingErrorKind::NotRegisteredInActiveTransactions,
            ))),
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
        key: &str,
        request_mode: LockMode,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> LockRequest {
        // If the lock table does not contain lock information for this record, insert lock information, and grant lock.
        if !self.lock_table.contains_key(key) {
            // Create new lock information.
            let mut lock_info = LockInfo::new(request_mode, transaction_ts);
            // Create new request entry.
            let entry = Entry::new(
                transaction_name.to_string(),
                request_mode,
                None,
                transaction_ts,
            );
            lock_info.add_entry(entry);
            // Insert into lock table
            self.lock_table.insert_new(key.to_string(), lock_info);
            self.register_lock(transaction_name, key).unwrap();
            return LockRequest::Granted;
        }

        // Else a lock exists for this record.
        // Retrieve the lock information.
        let mut lock_info = self.lock_table.get_mut(key).unwrap();
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
                            let entry = Entry::new(
                                transaction_name.to_string(),
                                LockMode::Read,
                                None,
                                transaction_ts,
                            );
                            // Add to holder/request list.
                            lock_info.add_entry(entry);
                            // Update lock timestamp if this lock request has higher timestamp
                            if transaction_ts > lock_info.timestamp.unwrap() {
                                lock_info.timestamp = Some(transaction_ts);
                            }
                            // Increment locks held
                            let held = lock_info.granted.unwrap();
                            lock_info.granted = Some(held + 1);
                            // Grant lock.
                            self.register_lock(transaction_name, key).unwrap();
                            return LockRequest::Granted;
                        }
                        LockMode::Write => {
                            // Record locked with write lock, read lock can not be granted.
                            // Apply wait-die deadlock detection.
                            if lock_info.timestamp.unwrap() < transaction_ts {
                                return LockRequest::Denied;
                            }
                            // Initialise a `Condvar` that will be used to sleep the thread.
                            let pair = Arc::new((Mutex::new(false), Condvar::new()));
                            // Create new entry for transaction request list.
                            let entry = Entry::new(
                                transaction_name.to_string(),
                                LockMode::Read,
                                Some(Arc::clone(&pair)),
                                transaction_ts,
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
                    if transaction_ts > lock_info.timestamp.unwrap() {
                        return LockRequest::Denied;
                    }
                    // Initialise a `Condvar` that will be used to wake up thread.
                    let pair = Arc::new((Mutex::new(false), Condvar::new()));
                    // Create new entry.
                    let entry = Entry::new(
                        transaction_name.to_string(),
                        LockMode::Write,
                        Some(Arc::clone(&pair)),
                        transaction_ts,
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
            let entry = Entry::new(
                transaction_name.to_string(),
                request_mode,
                None,
                transaction_ts,
            );
            lock_info.add_entry(entry);
            // Set lock timestamp.
            lock_info.timestamp = Some(transaction_ts);
            // Increment granted.
            lock_info.granted = Some(1);
            self.register_lock(transaction_name, key).unwrap();
            return LockRequest::Granted;
        }
    }

    // TODO: If no other transactions concurrently holding the lock and none are waiting delete lock information.
    // TODO: This could be implemented by assigning the key to a clean up thread which periodically purges the unused locks.

    fn release_lock(&self, key: &str, transaction_name: &str) {
        debug!("Release {}'s lock on {}", transaction_name, key);
        let mut lock_info = self.lock_table.get_mut(key).unwrap();

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
            debug!("set to default");
            return;
        }

        // Find the entry for this lock request.
        // Assumption: only 1 request per transaction name and the lock request must exist
        // in list.
        let entry_index = lock_info
            .list
            .iter()
            .position(|e| e.name == transaction_name)
            .unwrap();
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
                        // Register lock with transaction
                        self.register_lock(&e.name, key).unwrap();
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
                    // Register lock with transaction
                    self.register_lock(&lock_info.list[0].name, key).unwrap();
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
                        // Register lock with transaction
                        self.register_lock(&lock_info.list[next_write_entry_index].name, key)
                            .unwrap();
                    }
                }
            }
        }
    }

    fn release_locks(&self, transaction_name: &str) {
        debug!("Release locks for {:?}", transaction_name);
        let held_locks = self.active_transactions.get(transaction_name).unwrap();
        debug!("Locks held: {:?}", held_locks);
        for lock in held_locks.iter() {
            debug!("Release lock: {:?}", lock);
            self.release_lock(lock, transaction_name);
        }
    }

    fn cleanup(&self, transaction_name: &str) {
        debug!("Clean up {:?}", transaction_name);
        self.active_transactions.remove(transaction_name);
    }
}

#[derive(Debug)]
enum LockRequest {
    Granted,
    Denied,
    Delay(Arc<(Mutex<bool>, Condvar)>),
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

/// Represents the locking information for a given database record.
#[derive(Debug)]
struct LockInfo {
    /// Overall lock state.
    /// Prevents ransactions needing to traverse all `Entry`s to determine lock status.
    group_mode: Option<LockMode>,
    /// Whether transactions are waiting for the lock.
    waiting: bool,
    /// List of transactions that have acquired the lock or are waiting for it.
    list: Vec<Entry>,
    /// Latest timestamp of transactions that hold lock, used for deadlock detection.
    timestamp: Option<DateTime<Utc>>,
    /// Number of locks concurrently granted for this record.
    granted: Option<u32>,
}

impl LockInfo {
    /// Create new locking information container.
    ///
    /// Takes the initial group mode and initial timestamp.
    fn new(group_mode: LockMode, timestamp: DateTime<Utc>) -> LockInfo {
        LockInfo {
            group_mode: Some(group_mode),
            waiting: false,
            list: Vec::new(),
            timestamp: Some(timestamp),
            granted: Some(1),
        }
    }

    /// Add an `Entry` to the lock information.
    fn add_entry(&mut self, entry: Entry) {
        self.list.push(entry);
    }
}

impl fmt::Display for LockInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\n Group mode: {:?}\n Waiting transactions: {}\n Requests: {:?}\n timestamp: {:?}\n granted: {:?}",
            self.group_mode, self.waiting,self.list,self.timestamp, self.granted
        )
    }
}
/// Represents an entry in the list of transactions that have requested a lock.
///
/// These transactions can either be waiting for the lock or be holding it.
/// For requests that are waiting for the lock the calling thread is blocked using a `Condvar`.
/// When a transaction releases the lock it notifies the thread to resume execution.
#[derive(Debug)]
struct Entry {
    /// Transaction name.
    name: String,
    /// Lock request type.
    lock_mode: LockMode,
    /// Waiting for the lock or holding it.
    waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
    // TODO: Pointer to transaction's other `Entry`s for unlocking.
    // previous_entry: &Entry,
    /// Transaction timestamp.
    timestamp: DateTime<Utc>,
}

impl Entry {
    // Create new `Entry`.
    fn new(
        name: String,
        lock_mode: LockMode,
        waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
        timestamp: DateTime<Utc>,
    ) -> Entry {
        Entry {
            name,
            lock_mode,
            waiting,
            timestamp,
        }
    }
}

/// Represents the different lock modes.
#[derive(PartialEq, Debug, Clone, Copy)]
enum LockMode {
    Read,
    Write,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::scheduler::Protocol;
    use chrono::Duration;
    use config::Config;
    use lazy_static::lazy_static;
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
            // Initalise workload.
            let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());
            workload
        };
    }

    #[test]
    fn lock_request_type_test() {
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
    fn register_test() {
        logging(false);
        // Initialise scheduler.
        let protocol = Arc::new(Protocol::new(Arc::clone(&WORKLOAD)).unwrap());
        // Register transaction.
        assert_eq!(protocol.scheduler.register("some_id").unwrap(), ());
        assert_eq!(
            protocol
                .scheduler
                .register("some_id")
                .unwrap_err()
                .downcast::<TwoPhaseLockingError>()
                .unwrap(),
            Box::new(TwoPhaseLockingError::new(
                TwoPhaseLockingErrorKind::AlreadyRegisteredInActiveTransactions
            ))
        );
    }

    // In this test 3 read locks are requested by Ta, Tb, and Tc, which are then released.
    #[test]
    fn request_lock_read_test() {
        logging(true);

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

        // Request locks.
        assert_eq!(
            protocol.request_lock("table_1_row_12", LockMode::Read, &ta_id, ta_ts),
            LockRequest::Granted
        );
        assert_eq!(
            protocol.request_lock("table_1_row_12", LockMode::Read, &tb_id, tb_ts),
            LockRequest::Granted
        );
        assert_eq!(
            protocol.request_lock("table_1_row_12", LockMode::Read, &tc_id, tc_ts),
            LockRequest::Granted
        );
        // Check
        {
            let lock = protocol1.lock_table.get("table_1_row_12").unwrap();
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
        protocol.release_lock("table_1_row_12", &ta_id);
        protocol.release_lock("table_1_row_12", &tb_id);
        protocol.release_lock("table_1_row_12", &tc_id);
        // Check
        {
            let lock = protocol1.lock_table.get("table_1_row_12").unwrap();
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
    fn request_lock_write_test() {
        logging(false);

        // Initialise protocol
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));
        let protocol1 = protocol.clone();

        // Create transaction id and timestamp.
        let sys_time = SystemTime::now();
        let datetime: DateTime<Utc> = sys_time.into();
        let t_id = datetime.to_string();
        let t_ts = datetime;

        // Lock
        protocol.register(&t_id).unwrap();
        let req = protocol.request_lock("table_1_row_12", LockMode::Write, &t_id, t_ts);
        assert_eq!(req, LockRequest::Granted);

        {
            let lock = protocol1.lock_table.get("table_1_row_12").unwrap();
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
        protocol.release_lock("table_1_row_12", &t_id);
        {
            let lock = protocol1.lock_table.get("table_1_row_12").unwrap();
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
    fn lock_table_test() {
        logging(false);
        let protocol = Arc::new(TwoPhaseLocking::new(Arc::clone(&WORKLOAD)));
        let protocol1 = protocol.clone();

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
            protocol1.request_lock("table_1_row_12", LockMode::Read, &t_id_11, t_ts_1);
            debug!("Request Write lock");
            if let LockRequest::Delay(pair) =
                protocol1.request_lock("table_1_row_12", LockMode::Write, &t_id_2, t_ts_2)
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
        protocol.release_lock("table_1_row_12", &t_id_1);
        let lock = protocol.lock_table.get("table_1_row_12").unwrap();
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
    fn delay_read_request_test() {
        logging(true);
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

        // Register with scheduler
        protocol.register(&ta_id).unwrap();
        protocol.register(&tb_id).unwrap();

        let _handle = thread::spawn(move || {
            debug!("Request write lock by Ta");
            protocol_t.request_lock("table_1_row_12", LockMode::Write, &ta_id_t, ta_ts);
            debug!("Request read lock by Tb");
            let res = protocol_t.request_lock("table_1_row_12", LockMode::Read, &tb_id, tb_ts);
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
        protocol.release_lock("table_1_row_12", &ta_id);
        let lock = protocol.lock_table.get("table_1_row_12").unwrap();

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

    #[test]
    fn denied_lock_test() {
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
        protocol.register(&t_id).unwrap();
        protocol.register(&t_id_r).unwrap();
        protocol.register(&t_id_w).unwrap();

        // Take first write lock.
        debug!("Request write lock");
        protocol.request_lock("table_1_row_12", LockMode::Write, &t_id, datetime);

        // Attempt to get read and write locks.
        debug!("Request Read lock");
        assert_eq!(
            protocol.request_lock("table_1_row_12", LockMode::Read, &t_id_r, datetime_r),
            LockRequest::Denied
        );
        debug!("Request another write lock");
        assert_eq!(
            protocol.request_lock("table_1_row_12", LockMode::Write, &t_id_w, datetime_w),
            LockRequest::Denied
        );
        // Release initial write lock.
        debug!("Release write lock");
        protocol.release_lock("table_1_row_12", &t_id);
    }
}
