use crate::scheduler::owh::thread_state::ThreadState;

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use tracing::debug;

/// Garbage Collector.
#[derive(Debug)]
pub struct GarbageCollector {
    pub thread: Option<thread::JoinHandle<()>>,
}

// channel between hit list scheduler and garbage collection thread
impl GarbageCollector {
    pub fn new(
        shared: Arc<Vec<Arc<ThreadState>>>,
        receiver: mpsc::Receiver<()>,
        sleep: u64,
        threads: usize,
    ) -> GarbageCollector {
        let builder = thread::Builder::new().name("garbage_collector".to_string()); // thread name
        let thread = builder
            .spawn(move || {
                debug!("Starting garbage collector");

                let mut alpha = vec![];

                for _ in 0..threads {
                    alpha.push(None);
                }
                loop {
                    // attempt to receive shutdown notification without blocking
                    if let Ok(()) = receiver.try_recv() {
                        break; // exit loop
                    }

                    thread::sleep(Duration::from_millis(sleep * 1000)); // sleep garbage collector

                    for (i, thread) in shared.iter().enumerate() {
                        thread.get_epoch_tracker().new_epoch(); // increment epoch per thread
                        alpha[i] = Some(thread.get_epoch_tracker().update_alpha());
                    }

                    let min = alpha.iter().min().unwrap();

                    for thread in shared.iter() {
                        let to_remove = thread
                            .get_epoch_tracker()
                            .get_transactions_to_garbage_collect(min.unwrap());
                        thread.remove_transactions(to_remove);
                    }
                }
            })
            .unwrap();

        GarbageCollector {
            thread: Some(thread),
        }
    }
}
