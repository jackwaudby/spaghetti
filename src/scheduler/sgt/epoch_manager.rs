use crate::scheduler::sgt::node::ArcNode;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::debug;

#[derive(Debug)]
pub struct EpochManager {
    /// Global epoch counter.
    global_ctr: AtomicU64,

    /// Counter of EpochGuard that have the same view of the global counter.
    same_epoch_ctr: Vec<AtomicU64>,

    /// EpochGuard counter.
    guard_ctr: AtomicU64,

    /// Global used transactions.
    global_used: Mutex<Vec<ArcNode>>,
}

#[derive(Debug)]
pub struct EpochGuard {
    /// Reference to the EpochManager.
    em: Arc<EpochManager>,

    /// Coutner of active transactions registered with this EpochGuard.
    active_ctr: u64,

    /// Local view of the global epoch counter.
    local_ctr: u64,

    /// Runs when attempting to increment to the next global epoch
    runs: u64,

    /// List of used ArcNodes.
    used: Option<Vec<ArcNode>>,
}

impl EpochManager {
    /// Create a new EpochManager.
    pub fn new(threads: u64) -> Self {
        let mut same_epoch_ctr: Vec<AtomicU64> = Vec::new();
        for _ in 0..6 {
            same_epoch_ctr.push(AtomicU64::new(0));
        }

        EpochManager {
            global_ctr: AtomicU64::new(0),
            same_epoch_ctr,
            guard_ctr: AtomicU64::new(threads),
            global_used: Mutex::new(Vec::new()),
        }
    }

    fn try_increment_ctr(&self) -> bool {
        let old = self.global_ctr.load(Ordering::SeqCst);
        let this_epoch_ctr = self.same_epoch_ctr[(old % 6) as usize].load(Ordering::SeqCst);
        if this_epoch_ctr >= self.guard_ctr.load(Ordering::SeqCst) {
            self.global_ctr
                .compare_exchange(old, old + 1, Ordering::SeqCst, Ordering::SeqCst);

            let global_ctr = self.global_ctr.load(Ordering::SeqCst);

            if global_ctr > 1 {
                let n_bucket = (global_ctr + 2) % 6;
                let next_epoch = self.same_epoch_ctr[n_bucket as usize].load(Ordering::SeqCst);
                assert!(next_epoch == 0, "Em: {:?}; Bucket: {}", self, n_bucket);

                let b_bucket = (global_ctr - 2) % 6;
                let b_epoch = self.same_epoch_ctr[b_bucket as usize].load(Ordering::SeqCst);
                assert!(b_epoch == 0, "Em: {:?}; Bucket: {}", self, b_bucket);
            }
            return true;
        }
        false
    }
}

impl EpochGuard {
    /// Create a new instance of an EpochGuard.
    pub fn new(em: Arc<EpochManager>) -> Self {
        em.same_epoch_ctr[0].fetch_add(1, Ordering::SeqCst);

        EpochGuard {
            em,
            active_ctr: 0,
            local_ctr: 0,
            runs: 0,
            used: Some(Vec::new()),
        }
    }

    pub fn pin(&mut self) -> bool {
        debug!("pin");
        let mut res = false;
        if self.active_ctr > 0 {
            self.active_ctr += 1;
            return res;
        }
        self.active_ctr = 1;
        self.runs += 1;

        if self.runs >= self.em.guard_ctr.load(Ordering::SeqCst) {
            res = self.em.try_increment_ctr();
            self.runs = 0;
            if self.local_ctr != self.em.global_ctr.load(Ordering::SeqCst) {
                self.em.same_epoch_ctr[(self.local_ctr % 6) as usize]
                    .fetch_sub(1, Ordering::SeqCst);
                self.local_ctr = self.em.global_ctr.load(Ordering::SeqCst);
                self.em.same_epoch_ctr[(self.local_ctr % 6) as usize]
                    .fetch_add(1, Ordering::SeqCst);
            }
        } else if self.runs > 100 {
            panic!("too many runs");
        }

        res
    }

    /// Unpin a transaction from the EpochGuard.
    ///
    /// Decrements the active counter.
    pub fn unpin(&mut self) {
        debug!("unpin");
        self.active_ctr -= 1;
    }

    pub fn add(&mut self, node: ArcNode) {
        // TODO: add to txn info holder
        debug!("drop: {}", Arc::as_ptr(&node) as usize);
        self.used.as_mut().unwrap().push(node);

        self.cleanup();
    }

    fn cleanup(&self) {
        // TODO: remove stuff from next bucket
    }
}

impl Drop for EpochGuard {
    fn drop(&mut self) {
        let mut u = String::new();
        let n = self.used.as_ref().unwrap().len();

        if n > 0 {
            u.push_str("[");

            for node in &self.used.as_ref().unwrap()[0..n - 1] {
                u.push_str(&format!("{}", Arc::as_ptr(&node) as usize));
                u.push_str(", ");
            }

            let node = &self.used.as_ref().unwrap()[n - 1].clone();
            u.push_str(&format!("{}]", Arc::as_ptr(&node) as usize));
        } else {
            u.push_str("[]");
        }

        debug!("{:?}", u);

        self.em
            .global_used
            .lock()
            .unwrap()
            .extend(self.used.take().unwrap());
    }
}
