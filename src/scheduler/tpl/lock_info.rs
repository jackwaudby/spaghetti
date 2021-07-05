use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cmp::Ordering;
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

        debug_assert!(!information.is_free());
        debug_assert!(!information.is_waiting());
        debug_assert_eq!(information.num_granted(), 1);
        debug_assert_eq!(information.num_waiting(), 0);

        information
    }

    pub fn add_granted(&mut self, lock_mode: LockMode, timestamp: u64) {
        let request = Request::new(lock_mode, None, timestamp);
        self.granted.push(request);
        self.granted.sort(); // timestamp (lowest/oldest request to highest/youngest request)

        if timestamp < self.get_group_timestamp() {
            self.group_timestamp = Some(timestamp); // if this granted request has lower timestamp; then update lock's group timestamp
        }
    }

    pub fn add_waiting(
        &mut self,

        lock_mode: LockMode,
        timestamp: u64,
    ) -> Arc<(Mutex<bool>, Condvar)> {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let res = Arc::clone(&pair);
        let request = Request::new(lock_mode, Some(pair), timestamp);
        self.waiting.push(request);
        self.waiting.sort(); // timestamp (lowest/oldest request to highest/youngest request)
        res
    }

    pub fn owned_by(&mut self) -> u64 {
        self.get_group_timestamp()
    }

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

    pub fn remove_granted(&mut self, timestamp: u64) -> Request {
        let index = self
            .granted
            .iter()
            .position(|e| e.get_timestamp() == timestamp)
            .unwrap();

        self.granted.remove(index)
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
