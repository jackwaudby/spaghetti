use chashmap::CHashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use tracing::debug;

struct Scheduler {
    lock_table: Arc<CHashMap<String, LockInfo>>,
}

impl Scheduler {
    /// Initialise scheduler with empty lock table.
    fn new() -> Scheduler {
        let lock_table = Arc::new(CHashMap::<String, LockInfo>::new());
        Scheduler { lock_table }
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
        transaction_ts: u32,
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
            return LockRequest::Granted;
        }

        // Else a lock exists for this record.
        // Retrieve the lock information.
        let mut lock_info = self.lock_table.get_mut(key).unwrap();
        // The lock may be in use or not.
        // If in-use, consult the granted lock.
        if let Some(n) = lock_info.granted {
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
            return LockRequest::Granted;
        }
    }

    // TODO: If no other transactions concurrently holding the lock and none are waiting delete lock information.
    // TODO: This could be implemented by assigning the key to a clean up thread which periodically purges the unused locks.

    fn release_lock(&self, key: &str, transaction_name: &str) {
        debug!("Retrieve lock information for {}", key);
        let mut lock_info = self.lock_table.get_mut(key).unwrap();
        debug!("Releasing {:?} lock", lock_info.group_mode.unwrap());
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

        // If record was locked with a Write lock then this is the only transaction with a lock
        // on this record.
        // Then grant the next n Read lock requests
        // Or grant the next Write lock request
        //
        // If record was locked with a Read lock then the next waiting entry must be a
        // Write request as Reads requested when a Read lock is held are always granted.
        match entry.lock_mode {
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
                    lock_info.timestamp = Some(new_lock_timestamp);
                    lock_info.group_mode = Some(LockMode::Read);
                    let held = lock_info.granted.unwrap();
                    lock_info.granted = Some(held + read_requests as u32);
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
            LockMode::Read => {
                // There could be n Read locks active on this record.
                // Entries in lock information are not sorted, thus an active Read lock could
                // be at a higher position in the list than a waiting Write lock.
                // Traverse entries, if this Read lock is the last active Read lock, then
                // grant the next waiting Write lock.
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
                    lock_info.timestamp = Some(lock_info.list[next_write_entry_index].timestamp);
                    lock_info.granted = Some(1);
                    if lock_info.list.len() as u32 == lock_info.granted.unwrap() {
                        lock_info.waiting = false;
                    }
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
                } else {
                    panic!("Next waiting request is a Read");
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
    // The overall lock state, so transactions need not to traverse the list of entries to determine the lock status.
    group_mode: Option<LockMode>,
    // Whether transactions are waiting for the lock.
    waiting: bool,
    // List of transactions that have acquired the lock or are waiting for it.
    list: Vec<Entry>,
    // Latest timestamp of transactions that hold lock, used for deadlock detection.
    timestamp: Option<u32>,
    // Number of locks concurrently granted for this record.
    granted: Option<u32>,
}

impl LockInfo {
    /// Create new locking information container.
    ///
    /// Takes the initial group mode and initial timestamp.
    fn new(group_mode: LockMode, timestamp: u32) -> LockInfo {
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

/// Represents an entry in the list of transactions that have request the lock.
///
/// These transactions can either be waiting for the lock or be holding it.
/// For requests that are waiting for the lock the calling thread is blocked using a `Condvar`.
/// When a transaction releases the lock it notifies the thread to resume execution.
#[derive(Debug)]
struct Entry {
    // Transaction name.
    name: String,
    // Lock request type.
    lock_mode: LockMode,
    // Waiting for the lock or holding it.
    waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
    // TODO: Pointer to transaction's other `Entry`s for unlocking.
    // previous_entry: &Entry,
    timestamp: u32,
}

impl Entry {
    // Create new `Entry`.
    fn new(
        name: String,
        lock_mode: LockMode,
        waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
        timestamp: u32,
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

/// An `Abort` error is returned when a read or write scheuler operation fails for some reason.
#[derive(Debug, PartialEq)]
struct Abort;

impl fmt::Display for Abort {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let err_msg = "Request denied, abort transaction.";
        write!(f, "{}", err_msg)
    }
}

impl Error for Abort {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time};
    use tracing::{info, Level};
    use tracing_subscriber::FmtSubscriber;

    fn logging() {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    }

    #[test]
    fn lock_unlock_read() {
        logging();

        // Initialise scheduler
        let scheduler = Arc::new(Scheduler::new());
        let scheduler1 = scheduler.clone();
        // Lock
        let req = scheduler.request_lock("table_1_row_12", LockMode::Read, "txn_1", 2);
        assert_eq!(req, LockRequest::Granted);

        {
            let lock = scheduler1.lock_table.get("table_1_row_12").unwrap();
            assert_eq!(
                lock.group_mode == Some(LockMode::Read)
                    && !lock.waiting
                    && lock.list.len() as u32 == 1
                    && lock.timestamp == Some(2)
                    && lock.granted == Some(1),
                true,
                "{:?}",
                lock
            );
        }

        // Unlock
        scheduler.release_lock("table_1_row_12", "txn_1");
        {
            let lock = scheduler1.lock_table.get("table_1_row_12").unwrap();
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

    #[test]
    fn lock_unlock_write() {
        logging();

        // Initialise scheduler
        let scheduler = Arc::new(Scheduler::new());
        let scheduler1 = scheduler.clone();
        // Lock
        let req = scheduler.request_lock("table_1_row_12", LockMode::Write, "txn_1", 2);
        assert_eq!(req, LockRequest::Granted);

        {
            let lock = scheduler1.lock_table.get("table_1_row_12").unwrap();
            assert_eq!(
                lock.group_mode == Some(LockMode::Write)
                    && !lock.waiting
                    && lock.list.len() as u32 == 1
                    && lock.timestamp == Some(2)
                    && lock.granted == Some(1)
                    && lock.list[0].lock_mode == LockMode::Write,
                true,
                "{:?}",
                lock
            );
        }

        // Unlock
        scheduler.release_lock("table_1_row_12", "txn_1");
        {
            let lock = scheduler1.lock_table.get("table_1_row_12").unwrap();
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

    #[test]
    fn lock_table_test() {
        logging();
        let scheduler = Arc::new(Scheduler::new());
        let scheduler1 = scheduler.clone();
        let handle = thread::spawn(move || {
            debug!("Request Read lock");
            scheduler1.request_lock("table_1_row_12", LockMode::Read, "txn_1", 2);
            debug!("Request Write lock");
            if let LockRequest::Delay(pair) =
                scheduler1.request_lock("table_1_row_12", LockMode::Write, "txn_2", 1)
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
        scheduler.release_lock("table_1_row_12", "txn_1");
        let lock = scheduler.lock_table.get("table_1_row_12").unwrap();
        assert_eq!(
            lock.group_mode == Some(LockMode::Write)
                && !lock.waiting
                && lock.list.len() as u32 == 1
                && lock.timestamp == Some(1)
                && lock.granted == Some(1),
            true,
            "{:?}",
            *lock
        );
    }
}
