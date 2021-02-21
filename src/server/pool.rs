//! A custom threadpool implementation based on the example in the Rust book.
use crate::workloads::Workload;

use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use tracing::{debug, info};

#[derive(Debug)]
pub struct ThreadPool {
    /// Worker threads.
    workers: Vec<Worker>,

    /// Channel to worker threads for passing `Message`s.
    sender: mpsc::Sender<Message>,
}

impl ThreadPool {
    /// Create a new `ThreadPool`.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(workload: Arc<Workload>) -> ThreadPool {
        match workload
            .get_internals()
            .config
            .get_str("protocol")
            .unwrap()
            .as_str()
        {
            "2pl" => {
                // Get thread pool size.
                let size = workload.get_internals().config.get_int("threads").unwrap();

                // Must have at least 1 thread in the pool.
                assert!(size > 0);

                // Job queue, threadpool keeps sending end.
                let (sender, receiver) = mpsc::channel();

                // Wrap receiver in Mutex and Arc so can be shared across worker threads.
                let receiver = Arc::new(Mutex::new(receiver));

                // Pre-allocate vec to hold workers.
                let mut workers = Vec::with_capacity(size as usize);

                // Create `size` threads.
                for id in 0..size {
                    workers.push(Worker::new(id as usize, None, Arc::clone(&receiver)));
                }
                ThreadPool { workers, sender }
            }
            "sgt" => {
                // Retrieve the IDs of all active CPU cores.
                let core_ids = core_affinity::get_core_ids().unwrap();

                info!("Detected {} cores", core_ids.len());

                // Job queue, threadpool keeps sending end.
                let (sender, receiver) = mpsc::channel();

                // Wrap receiver in Mutex and Arc so can be shared across worker threads.
                let receiver = Arc::new(Mutex::new(receiver));

                // Pre-allocate vec to hold workers.
                let mut workers = Vec::with_capacity(core_ids.len());

                // Create `size` threads.
                for (id, core_id) in core_ids.into_iter().enumerate() {
                    info!("Initialise worker {}", id);
                    workers.push(Worker::new(
                        id as usize,
                        Some(core_id),
                        Arc::clone(&receiver),
                    ));
                }
                ThreadPool { workers, sender }
            }
            _ => panic!("incorrect protocol"),
        }
    }

    /// Execute new job.
    ///
    /// Accepts a generic type that implements FnOnce(), Send and 'static.
    /// Boxes the input and sends to workers.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // New job instance which holds the closure to pass to the thread.
        let job = Box::new(f);
        // Send to worker threads down the channel.
        self.sender.send(Message::NewJob(job)).unwrap();
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Send a terminate message for each worker.
        // Happens after request queue drained into threadpool job queue, so no requests
        // should be missed.
        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        debug!("Terminating workers.");

        for worker in &mut self.workers {
            debug!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }

            debug!("Shutdown worker {}", worker.id);
        }

        debug!("Workers shutdown.");
    }
}

/// Represents the type of jobs worker can receive.
enum Message {
    NewJob(Job),
    Terminate,
}

/// Represents a worker in the threadpool.
///
/// Wrapper around a thread with an id.
#[derive(Debug)]
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(
        id: usize,
        core_id: Option<core_affinity::CoreId>,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    ) -> Worker {
        // Set thread name to id.
        let builder = thread::Builder::new().name(id.to_string().into());

        let thread = builder
            .spawn(move || {
                if let Some(core_id) = core_id {
                    // Pin this thread to a single CPU core.
                    core_affinity::set_for_current(core_id);
                }
                loop {
                    // Get message from job queue.
                    let message = receiver.lock().unwrap().recv().unwrap();
                    // Execute job.
                    match message {
                        Message::NewJob(job) => {
                            debug!("Worker {} got a job; executing.", id);
                            job.call_box();
                        }
                        Message::Terminate => {
                            debug!("Worker {} was told to terminate.", id);
                            break;
                        }
                    }
                }
            })
            .unwrap();

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

/// Type alias for a trait object that holds the type of closure that `execute' receives.
type Job = Box<dyn FnBox + Send + 'static>;
