use crate::server::scheduler::opt_hit_list::terminated_list::ThreadState;

use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::Duration;
use tracing::{debug, info};

/// Garbage Collector.
#[derive(Debug)]
pub struct GarbageCollector {
    pub thread: Option<thread::JoinHandle<()>>,
}

// channel between hit list scheduler and garbage collection thread
impl GarbageCollector {
    pub fn new(
        shared: Arc<Vec<Arc<RwLock<ThreadState>>>>,
        receiver: mpsc::Receiver<()>,
        sleep: u64,
    ) -> GarbageCollector {
        let builder = thread::Builder::new().name("garbage_collector".to_string().into()); // thread name
        let thread = builder
            .spawn(move || {
                debug!("Starting garbage collector");
                let handle = thread::current();

                let mut alpha = vec![];
                // TODO
                for i in 0..10 {
                    alpha.push(None);
                }
                loop {
                    // attempt to receive shutdown notification without blocking
                    if let Ok(()) = receiver.try_recv() {
                        break; // exit loop
                    }
                    thread::sleep(Duration::from_millis(sleep)); // sleep garbage collector

                    // pass 1: increment epochs
                    for thread in shared.iter() {
                        thread.read().unwrap().get_epoch_tracker().new_epoch();
                    }

                    // pass 2: compute alphs
                    for (i, thread) in shared.iter().enumerate() {
                        alpha[i] = Some(thread.read().unwrap().get_epoch_tracker().update_alpha());
                    }

                    // pass 3: remove all < min alpha
                    let min = alpha.iter().min().unwrap();

                    for thread in shared.iter() {
                        let to_remove = thread
                            .read()
                            .unwrap()
                            .get_epoch_tracker()
                            .get_transactions_to_garbage_collect(min.unwrap());

                        for id in to_remove {
                            thread.read().unwrap().remove_transaction(id);
                        }
                    }
                }
            })
            .unwrap();

        GarbageCollector {
            thread: Some(thread),
        }
    }
}
