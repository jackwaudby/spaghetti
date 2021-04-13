use crate::server::scheduler::opt_hit_list::thread_state::ThreadState;

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
        let builder = thread::Builder::new().name("garbage_collector".to_string().into()); // thread name
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

                    // pass 1: increment epochs
                    for thread in shared.iter() {
                        thread.get_epoch_tracker().new_epoch();
                    }

                    for (i, thread) in shared.iter().enumerate() {
                        debug!("Thread {}: {}", i, thread);
                    }

                    // pass 2: compute alphs
                    for (i, thread) in shared.iter().enumerate() {
                        alpha[i] = Some(thread.get_epoch_tracker().update_alpha());
                    }

                    // pass 3: remove all < min alpha
                    let min = alpha.iter().min().unwrap();
                    debug!("Alpha: {:?}", alpha);

                    for thread in shared.iter() {
                        let mut to_remove = thread
                            .get_epoch_tracker()
                            .get_transactions_to_garbage_collect(min.unwrap());
                        to_remove.sort();
                        debug!("To remove: {:?}", to_remove);

                        for id in to_remove {
                            debug!("Remove: {:?}", id);
                            thread.remove_transaction(id);
                            debug!("Remove: {:?}", id);
                        }
                        debug!("All removed");
                    }
                }
            })
            .unwrap();

        GarbageCollector {
            thread: Some(thread),
        }
    }
}
