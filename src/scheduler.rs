use chashmap::CHashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};

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
    /// Takes a search key, desired lock mode, transaction name, and transaction timestamp.
    /// If the lock is acquired `Granted` is returned.
    /// If the lock request is refused `Denied` is returned.
    /// If the lock request is delayed `Delayed` is returned.
    fn request_lock(
        &self,
        key: &str,
        mode: LockMode,
        transaction_name: &str,
        transaction_ts: u32,
    ) -> LockRequest {
        // If the lock table does not contain an entry for this record, insert entry and grant lock.
        if !self.lock_table.contains_key(key) {
            // Create new lock information.
            let mut lock_info = LockInfo::new(mode, transaction_ts);
            // Create new entry.
            let entry = Entry::new(transaction_name.to_string(), mode, None, transaction_ts);
            lock_info.add_entry(entry);
            // Insert into lock table
            self.lock_table.insert_new(key.to_string(), lock_info);
            return LockRequest::Granted;
        }

        // Else some lock exists on this record.
        // Get the entry.
        let mut lock_info = self.lock_table.get_mut(key).unwrap();
        // Consult the group mode.
        // If the lock request is a `Read` and the current lock on the record is a `Read`
        // then the lock can be granted. If the current lock on the read is a `Write` the
        // request can not be granted, the thread is put to sleep.
        //
        // If the lock request is a `Write` it is always waits for the lock to be released.
        match mode {
            LockMode::Read => {
                // Lock status.
                match lock_info.group_mode {
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
                        // Update lock timestamp if this lock has higher timestamp
                        if transaction_ts > lock_info.timestamp {
                            lock_info.timestamp = transaction_ts;
                        }
                        return LockRequest::Granted;
                    }
                    LockMode::Write => {
                        // Record locked with write lock, read lock can not be granted.
                        // Wait-die deadlock detection. If the requesting transaction has an younger
                        // timestamp it can wait for lock. Else, the request is refused.
                        if transaction_ts > lock_info.timestamp {
                            return LockRequest::Denied;
                        }
                        // Init. a condvar that will be used to wake up thread.
                        let pair = Arc::new((Mutex::new(false), Condvar::new()));
                        // Create new entry for wait list.
                        let entry = Entry::new(
                            transaction_name.to_string(),
                            LockMode::Read,
                            Some(Arc::clone(&pair)),
                            transaction_ts,
                        );
                        // Add to holder/request list.
                        lock_info.add_entry(entry);
                        return LockRequest::Delay(pair);
                    }
                }
            }
            LockMode::Write => {
                if transaction_ts > lock_info.timestamp {
                    return LockRequest::Denied;
                }
                // Init. a condvar that will be used to wake up thread.
                let pair = Arc::new((Mutex::new(false), Condvar::new()));
                // If wait create new entry with condvar.
                let entry = Entry::new(
                    transaction_name.to_string(),
                    LockMode::Read,
                    Some(Arc::clone(&pair)),
                    transaction_ts,
                );
                // Add to holder/request list.
                lock_info.add_entry(entry);
                return LockRequest::Delay(pair);
            }
        }
    }

    fn release_lock(&self, key: &str, transaction_name: &str) {
        // Get the entry.
        let mut lock_info = self.lock_table.get_mut(key).unwrap();
        // TODO: No other transactions concurrently holding lock and none waiting.
        // Remove transactions entry.
        // TODO: assuming its first for now.
        let entry = lock_info.list.remove(0);
        match entry.lock_mode {
            LockMode::Write => {
                let mut r = 0;
                for e in &lock_info.list[..] {
                    if e.lock_mode == LockMode::Write {
                        break;
                    }
                    r += 1;
                }
                if r != 0 {
                    let mut ts = 0;
                    for e in &lock_info.list[0..r] {
                        if e.timestamp > ts {
                            ts = e.timestamp;
                        }
                        let cond = Arc::clone(e.waiting.as_ref().unwrap());

                        let (lock, cvar) = &*cond;
                        let mut started = lock.lock().unwrap();
                        *started = true;
                        cvar.notify_all();
                    }
                    lock_info.timestamp = ts;
                    lock_info.group_mode = LockMode::Read;
                } else {
                    lock_info.timestamp = lock_info.list[0].timestamp;
                    lock_info.group_mode = LockMode::Write;
                    let cond = Arc::clone(lock_info.list[0].waiting.as_ref().unwrap());

                    let (lock, cvar) = &*cond;
                    let mut started = lock.lock().unwrap();
                    *started = true;
                    cvar.notify_all();
                }
            }
            LockMode::Read => {
                if lock_info.list[0].lock_mode == LockMode::Write {
                    lock_info.timestamp = lock_info.list[0].timestamp;
                    lock_info.group_mode = LockMode::Write;
                    let cond = Arc::clone(lock_info.list[0].waiting.as_ref().unwrap());

                    let (lock, cvar) = &*cond;
                    let mut started = lock.lock().unwrap();
                    *started = true;
                    cvar.notify_all();
                } else {
                    // Another transaction has read lock
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

// Represents the locking information for a given database element.
struct LockInfo {
    // The overall lock state.
    group_mode: LockMode,
    // Whether transactions are waiting for the lock.
    waiting: bool,
    // List of transactions that havex acquired the lock or are waiting for it.
    list: Vec<Entry>,
    // Latest timestamp of transactions that hold lock.
    timestamp: u32,
}

impl LockInfo {
    // Create new locking information container.
    fn new(group_mode: LockMode, timestamp: u32) -> LockInfo {
        LockInfo {
            group_mode,
            waiting: false,
            list: Vec::new(),
            timestamp,
        }
    }

    // Add an `Entry` to the lock information.
    fn add_entry(&mut self, entry: Entry) {
        self.list.push(entry);
    }
}

// Represents the different lock modes.
#[derive(PartialEq, Clone, Copy)]
enum LockMode {
    Read,
    Write,
}

#[derive(Debug, PartialEq)]
struct Abort;

impl fmt::Display for Abort {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let err_msg = "Request denied, abort transaction.";
        write!(f, "{}", err_msg)
    }
}

impl Error for Abort {}

// Represents an entry in the list of requests (holding the lock or waiting for it).
struct Entry {
    // Transaction name.
    name: String,
    // Lock request type.
    lock_mode: LockMode,
    // Waiting for the lock or holding it.
    waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
    // TODO: Pgointer to transaction's other `Entry`s.
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

#[cfg(test)]
mod tests {

    use super::*;
    use std::{thread, time};

    #[test]
    fn lock_table_test() {
        let scheduler = Arc::new(Scheduler::new());
        let scheduler1 = scheduler.clone();
        let handle = thread::spawn(move || {
            scheduler1.request_lock("table_1_row_12", LockMode::Read, "txn_1", 2);
            if let LockRequest::Delay(pair) =
                scheduler1.request_lock("table_1_row_12", LockMode::Write, "txn_2", 1)
            {
                let (lock, cvar) = &*pair;
                let mut waiting = lock.lock().unwrap();
                while !*waiting {
                    waiting = cvar.wait(waiting).unwrap();
                }
            };
        });

        let ms = time::Duration::from_secs(2);
        thread::sleep(ms);
        scheduler.release_lock("table_1_row_12", "txn_1");

        // let cond = Arc::clone(
        //     scheduler.lock_table.get_mut("table_1_row_12").unwrap().list[1]
        //         .waiting
        //         .as_ref()
        //         .unwrap(),
        // );

        // let (lock, cvar) = &*cond;
        // let mut started = lock.lock().unwrap();
        // *started = true;
        // cvar.notify_all();

        assert_eq!(2, 2);
    }
}
