use crate::scheduler::sgt::node::ArcNode;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct EpochManager {
    global_ctr: AtomicU64,
    same_epoch_ctr: Vec<AtomicU64>,
    guard_ctr: AtomicU64,
}

#[derive(Debug)]
pub struct EpochGuard {
    em: Arc<EpochManager>,
    active_ctr: u64,
    local_ctr: u64,
    runs: u64,
}

impl EpochManager {
    pub fn new(threads: u64) -> Self {
        let mut same_epoch_ctr: Vec<AtomicU64> = Vec::new();
        for _ in 0..6 {
            same_epoch_ctr.push(AtomicU64::new(0));
        }

        EpochManager {
            global_ctr: AtomicU64::new(0),
            same_epoch_ctr,
            guard_ctr: AtomicU64::new(threads),
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
    pub fn new(em: Arc<EpochManager>) -> Self {
        em.same_epoch_ctr[0].fetch_add(1, Ordering::SeqCst);

        EpochGuard {
            em,
            active_ctr: 0,
            local_ctr: 0,
            runs: 0,
        }
    }

    pub fn pin(&mut self) -> bool {
        let mut res = false;
        if self.active_ctr > 0 {
            self.active_ctr += 1;
            return res;
        }
        self.active_ctr = 1;
        self.runs += 1;

        //  println!("eg: {}; run: {}", self.id, self.runs);
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
        } else {
            //       println!("eg: {}; same epoch ctr < instance ctr ", self.id);
        }
        res
    }

    pub fn unpin(&mut self) {
        self.active_ctr -= 1;
    }

    pub fn add(&self, node: ArcNode) {
        // TODO: add to txn info holder
        drop(node);
        self.cleanup();
    }

    fn cleanup(&self) {
        // TODO: remove stuff from next bucket
    }
}
