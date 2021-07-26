use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum LockMode {
    Read,
    Write,
}

#[derive(Debug)]
pub struct Lock(Mutex<Information>);

#[derive(Debug)]
pub struct Information {
    group_mode: Option<LockMode>,
    group_timestamp: Option<u64>,
    waiting: Vec<Request>,
    granted: Vec<Request>,
}

// Assumption: transactions only make a single request per record, hence, timestamps are unique.
#[derive(Debug)]
pub struct Request {
    lock_mode: LockMode,
    waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
    timestamp: u64,
}

impl Lock {
    pub fn create_and_grant(group_mode: LockMode, timestamp: u64) -> Self {
        let information = Information::create_and_grant(group_mode, timestamp);

        Lock(Mutex::new(information))
    }

    pub fn lock(&self) -> MutexGuard<Information> {
        self.0.lock()
    }
}

impl Information {
    pub fn create_and_grant(lock_mode: LockMode, timestamp: u64) -> Self {
        let mut information = Information {
            group_mode: Some(lock_mode),
            group_timestamp: Some(timestamp),
            waiting: Vec::new(),
            granted: Vec::new(),
        };

        information.add_granted(lock_mode, timestamp);
        information
    }

    /// Add a request to the list of granted requests.
    ///
    /// Information maintains a list of requests sorted from lowest to highest timestamp.
    /// Information's group mode reflects the highest timestamp.
    pub fn add_granted(&mut self, lock_mode: LockMode, timestamp: u64) {
        let request = Request::new(lock_mode, None, timestamp);

        self.granted.push(request);
        self.granted.sort();

        if self.group_timestamp.is_some() {
            if timestamp > self.get_group_timestamp() {
                self.group_timestamp = Some(timestamp);
            }
        } else {
            self.group_timestamp = Some(timestamp);
            self.group_mode = Some(lock_mode);
        }
    }

    /// Add a request to the list of waiting requests.
    ///
    /// Information maintains a list of requests sorted from lowest to highest timestamp.
    pub fn add_waiting(
        &mut self,
        lock_mode: LockMode,
        timestamp: u64,
    ) -> Arc<(Mutex<bool>, Condvar)> {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let res = Arc::clone(&pair);
        let request = Request::new(lock_mode, Some(pair), timestamp);
        self.waiting.push(request);
        self.waiting.sort();
        res
    }

    /// Attempt to upgrade a read lock to a write lock.
    pub fn upgrade(&mut self, timestamp: u64) -> bool {
        if self.num_granted() == 1 && self.get_group_timestamp() == timestamp {
            self.set_mode(LockMode::Write);
            return true;
        }
        false
    }

    /// Returns true if the transaction holds a lock
    pub fn holds_lock(&self, timestamp: u64) -> bool {
        self.granted
            .iter()
            .find(|e| e.get_timestamp() == timestamp)
            .is_some()
    }

    /// Returns true is no transaction holds the lock.
    pub fn is_free(&self) -> bool {
        self.group_mode.is_none()
    }

    pub fn set_mode(&mut self, mode: LockMode) {
        self.group_mode = Some(mode);
    }

    pub fn get_mode(&self) -> LockMode {
        self.group_mode.unwrap().clone()
    }

    pub fn is_waiting(&self) -> bool {
        !self.waiting.is_empty()
    }

    pub fn num_waiting(&self) -> usize {
        self.waiting.len()
    }

    pub fn num_granted(&self) -> usize {
        self.granted.len()
    }

    pub fn get_group_timestamp(&mut self) -> u64 {
        self.group_timestamp.unwrap()
    }

    pub fn reset(&mut self) {
        self.group_mode = None; // reset group mode
        self.group_timestamp = None; // reset timestamp
        self.waiting.clear(); // remove from list
        self.granted.clear(); // remove from list
    }

    /// Remove request with `timestamp` from list of granted requests.
    pub fn remove_granted(&mut self, timestamp: u64) -> Request {
        let index = self
            .granted
            .iter()
            .position(|e| e.get_timestamp() == timestamp);

        match index {
            Some(index) => {
                let removed = self.granted.remove(index);
                // TODO: update group timestamp
                let size = self.granted.len();
                if size > 0 {
                    self.group_timestamp = Some(self.granted[size - 1].get_timestamp());
                }
                removed
            }
            None => panic!("request with timestamp {} not found: {:?}", timestamp, self),
        }
    }

    /// Grant either: (i) next n read lock requests, or, (ii) the next write lock request.
    /// Assumption 1: waiting requests are sorted by timestamp.
    /// Assumption 2: at least 1 waiting request.
    pub fn grant_waiting(&mut self) {
        let write_pos = self
            .waiting
            .iter()
            .position(|request| request.get_mode() == LockMode::Write); // position of first waiting write request

        match write_pos {
            // At least waiting write request;
            Some(write_index) => {
                if write_index == 0 {
                    // (i) first waiting request is a write request
                    let write_request = self.waiting.remove(write_index); // remove from waiting list
                    self.group_timestamp = Some(write_request.get_timestamp()); // update group timestamp
                    self.set_mode(LockMode::Write); // update group mode
                    let cond = write_request.get_cond(); // wake up thread
                    let (lock, cvar) = &*cond;
                    let mut started = lock.lock();
                    *started = true;
                    cvar.notify_all();
                    self.granted.push(write_request); // add to granted
                } else {
                    // (ii) there are n read requests before first write request
                    self.set_mode(LockMode::Read); // update group mode
                    let removed: Vec<_> = self.waiting.drain(0..write_index).collect();
                    for read_request in removed.into_iter() {
                        self.group_timestamp = Some(read_request.get_timestamp()); // update group timestamp
                        let cond = read_request.get_cond(); // wake up thread
                        let (lock, cvar) = &*cond;
                        let mut started = lock.lock();
                        *started = true;
                        cvar.notify_all();
                        self.granted.push(read_request); // add to granted
                    }
                }
            }
            // Grant waiting read requests;
            None => {
                self.set_mode(LockMode::Read); // update group mode
                while let Some(read_request) = self.waiting.pop() {
                    self.group_timestamp = Some(read_request.get_timestamp()); // update group timestamp
                    let cond = read_request.get_cond(); // wake up thread
                    let (lock, cvar) = &*cond;
                    let mut started = lock.lock();
                    *started = true;
                    cvar.notify_all();
                    self.granted.push(read_request); // add to granted
                }
            }
        }
    }
}

impl Request {
    pub fn new(
        lock_mode: LockMode,
        waiting: Option<Arc<(Mutex<bool>, Condvar)>>,
        timestamp: u64,
    ) -> Self {
        Self {
            lock_mode,
            waiting,
            timestamp,
        }
    }

    pub fn get_cond(&self) -> Arc<(Mutex<bool>, Condvar)> {
        Arc::clone(self.waiting.as_ref().unwrap())
    }

    pub fn get_mode(&self) -> LockMode {
        self.lock_mode.clone()
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Ord for Request {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for Request {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl Eq for Request {}

impl fmt::Display for LockMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LockMode::Read => {
                write!(f, "read lock")
            }
            LockMode::Write => {
                write!(f, " write lock")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tpl_information_write_waiting() {
        let mut info = Information::create_and_grant(LockMode::Read, 3);

        assert_eq!(info.get_mode(), LockMode::Read);
        assert_eq!(info.get_group_timestamp(), 3);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 0);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), false);

        info.add_granted(LockMode::Read, 4);

        assert_eq!(info.upgrade(4), false);

        assert_eq!(info.get_mode(), LockMode::Read);
        assert_eq!(info.get_group_timestamp(), 4);
        assert_eq!(info.num_granted(), 2);
        assert_eq!(info.num_waiting(), 0);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), false);

        info.add_waiting(LockMode::Write, 1);

        assert_eq!(info.get_mode(), LockMode::Read);
        assert_eq!(info.get_group_timestamp(), 4);
        assert_eq!(info.num_granted(), 2);
        assert_eq!(info.num_waiting(), 1);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), true);

        info.remove_granted(4);

        assert_eq!(info.get_mode(), LockMode::Read);
        assert_eq!(info.get_group_timestamp(), 3);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 1);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), true);

        assert_eq!(info.upgrade(3), true);

        assert_eq!(info.get_mode(), LockMode::Write);
        assert_eq!(info.get_group_timestamp(), 3);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 1);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), true);

        info.remove_granted(3);
        info.grant_waiting();

        assert_eq!(info.get_mode(), LockMode::Write);
        assert_eq!(info.get_group_timestamp(), 1);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 0);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), false);

        info.reset();

        assert_eq!(info.num_granted(), 0);
        assert_eq!(info.num_waiting(), 0);
        assert_eq!(info.is_free(), true);
        assert_eq!(info.is_waiting(), false);
    }

    #[test]
    fn test_tpl_information_read_waiting() {
        let mut info = Information::create_and_grant(LockMode::Write, 10);

        assert_eq!(info.get_mode(), LockMode::Write);
        assert_eq!(info.get_group_timestamp(), 10);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 0);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), false);

        info.add_waiting(LockMode::Read, 2);
        info.add_waiting(LockMode::Read, 5);
        info.add_waiting(LockMode::Read, 3);
        info.add_waiting(LockMode::Write, 8);
        info.add_waiting(LockMode::Read, 9);

        assert_eq!(info.get_mode(), LockMode::Write);
        assert_eq!(info.get_group_timestamp(), 10);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 5);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), true);

        info.remove_granted(10);
        info.grant_waiting();

        assert_eq!(info.get_mode(), LockMode::Read);
        assert_eq!(info.get_group_timestamp(), 5);
        assert_eq!(info.num_granted(), 3);
        assert_eq!(info.num_waiting(), 2);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), true);

        info.remove_granted(5);
        assert_eq!(info.upgrade(2), false);
        info.remove_granted(2);

        assert_eq!(info.get_mode(), LockMode::Read);
        assert_eq!(info.get_group_timestamp(), 3);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 2);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), true);

        info.remove_granted(3);
        info.grant_waiting();

        assert_eq!(info.get_mode(), LockMode::Write);
        assert_eq!(info.get_group_timestamp(), 8);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 1);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), true);

        info.remove_granted(8);
        info.grant_waiting();

        assert_eq!(info.get_mode(), LockMode::Read);
        assert_eq!(info.get_group_timestamp(), 9);
        assert_eq!(info.num_granted(), 1);
        assert_eq!(info.num_waiting(), 0);
        assert_eq!(info.is_free(), false);
        assert_eq!(info.is_waiting(), false);

        info.reset();

        assert_eq!(info.num_granted(), 0);
        assert_eq!(info.num_waiting(), 0);
        assert_eq!(info.is_free(), true);
        assert_eq!(info.is_waiting(), false);
    }
}
