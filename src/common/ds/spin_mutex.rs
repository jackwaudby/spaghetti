use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct Mutex {
    locked: AtomicBool,
}

pub struct MutexGuard<'a> {
    mutex: &'a Mutex,
}

impl Mutex {
    pub fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_> {
        loop {
            if !self
                .locked
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                break MutexGuard { mutex: self };
            }
        }
    }
}

impl<'a> Drop for MutexGuard<'a> {
    fn drop(&mut self) {
        self.mutex.locked.store(false, Ordering::Release);
    }
}
