use crate::common::error::FatalError;
use crate::workloads::Workload;
use crate::Result;

use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use tracing::{debug, info};

/// A thread pool used to execute transactions in parallel.
///
/// Spawns a specified number of worker threads and shuts down if any worker threads
/// panic.
#[derive(Debug)]
pub struct ThreadPool {
    /// Worker threads.
    workers: Vec<Worker>,

    /// Channel to worker threads for passing `Message`s.
    sender: mpsc::Sender<Message>,

    /// Receiver to listen for panicked threads.
    panic_receiver: mpsc::Receiver<()>,

    is_shutdown: bool,
}

/// Used to notify pool a worker has panicked.
struct Sentinel {
    active: bool,
    panic_notify: mpsc::Sender<()>,
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
            .get_config()
            .get_str("protocol")
            .unwrap()
            .as_str()
        {
            "2pl" | "hit" => {
                // Get thread pool size.
                let size = workload
                    .get_internals()
                    .get_config()
                    .get_int("workers")
                    .unwrap();

                // Must have at least 1 thread in the pool.
                assert!(size > 0);

                // Job queue, threadpool keeps sending end.
                let (sender, receiver) = mpsc::channel();

                let (panic_sender, panic_receiver) = mpsc::channel();

                // Wrap receiver in Mutex and Arc so can be shared across worker threads.
                let receiver = Arc::new(Mutex::new(receiver));

                // Pre-allocate vec to hold workers.
                let mut workers = Vec::with_capacity(size as usize);

                // Create `size` threads.
                for id in 0..size {
                    workers.push(Worker::new(
                        id as usize,
                        None,
                        Arc::clone(&receiver),
                        panic_sender.clone(),
                    ));
                }
                ThreadPool {
                    workers,
                    sender,
                    is_shutdown: false,
                    panic_receiver,
                }
            }
            "sgt" => {
                // Retrieve the IDs of all active CPU cores.
                let core_ids = core_affinity::get_core_ids().unwrap();

                info!("Detected {} cores", core_ids.len());

                // Job queue, threadpool keeps sending end.
                let (sender, receiver) = mpsc::channel();
                let (panic_sender, panic_receiver) = mpsc::channel();

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
                        panic_sender.clone(),
                    ));
                }
                ThreadPool {
                    workers,
                    sender,
                    is_shutdown: false,
                    panic_receiver,
                }
            }
            _ => panic!("incorrect protocol"),
        }
    }

    pub fn listen_for_panic(&mut self) {
        if let Ok(()) = self.panic_receiver.try_recv() {
            debug!("A thread has panicked");
            self.is_shutdown = true;
        }
    }

    /// Execute new job.
    ///
    /// Accepts a generic type that implements FnOnce(), Send and 'static.
    /// Boxes the input and sends to workers.
    pub fn execute<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        // If panicked then stop.
        if self.is_shutdown {
            debug!("Unable to execute request");
            return Err(Box::new(FatalError::ThreadPoolClosed));
        }

        // New job instance which holds the closure to pass to the thread.
        let job = Box::new(f);
        // Send to worker threads down the channel.
        self.sender.send(Message::NewJob(job)).unwrap();
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }
}

impl Sentinel {
    /// Create sentinel.
    fn new(panic_notify: mpsc::Sender<()>) -> Sentinel {
        Sentinel {
            active: true,
            panic_notify,
        }
    }

    /// Cancel and destroy this sentinel.
    fn cancel(mut self) {
        self.active = false;
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
                match thread.join() {
                    Ok(res) => debug!("worker {}: {:?}", worker.id, res),
                    Err(e) => {
                        if let Some(e) = e.downcast_ref::<&'static str>() {
                            debug!("Got an error: {}", e);
                        } else {
                            debug!("Unable to downcast error {:?}", e);
                        }
                    }
                }
            }

            debug!("Shutdown worker {}", worker.id);
        }

        debug!("Workers shutdown.");
    }
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        if self.active {
            if thread::panicking() {
                debug!("Send panic notification");
                self.panic_notify.send(()).unwrap();
            }
        }
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
    thread: Option<thread::JoinHandle<Result<()>>>,
}

impl Worker {
    fn new(
        id: usize,
        core_id: Option<core_affinity::CoreId>,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
        panic_receiver: mpsc::Sender<()>,
    ) -> Worker {
        // Set thread name to id.
        let builder = thread::Builder::new().name(id.to_string().into());

        let thread = builder
            .spawn(move || -> Result<()> {
                let sentinel = Sentinel::new(panic_receiver);

                if let Some(core_id) = core_id {
                    // Pin this thread to a single CPU core.
                    core_affinity::set_for_current(core_id);
                }
                loop {
                    // debug!("Worker {} waiting for job.", id);
                    // Get message from job queue.
                    let message = receiver.lock().unwrap().recv().unwrap();
                    // Execute job.
                    match message {
                        Message::NewJob(job) => {
                            // debug!("Worker {} got a job; executing.", id);
                            // If closure causes error then panic.
                            if let Err(e) = job.call_box() {
                                debug!("Panicked - drops sentinal");
                                panic!("{}", e);
                            }
                        }
                        Message::Terminate => {
                            debug!("Worker {} was told to terminate.", id);
                            break;
                        }
                    }
                }
                sentinel.cancel();
                Ok(())
            })
            .unwrap();

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

trait FnBox {
    fn call_box(self: Box<Self>) -> Result<()>;
}

impl<F: FnOnce() -> Result<()>> FnBox for F {
    fn call_box(self: Box<F>) -> Result<()> {
        (*self)()
    }
}

/// Type alias for a trait object that holds the type of closure that `execute' receives.
type Job = Box<dyn FnBox + Send + 'static>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workloads::tatp::loader;
    use crate::workloads::Internal;
    use crate::workloads::Workload;
    use config::Config;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::convert::TryInto;
    use std::sync::Arc;
    use std::sync::Once;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    static LOG: Once = Once::new();

    fn logging(on: bool) {
        if on {
            LOG.call_once(|| {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting default subscriber failed");
            });
        }
    }

    #[test]
    fn pool() {
        logging(false);
        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("Test-tpl.toml")).unwrap();
        let config = Arc::new(c);

        // Workload with fixed seed.
        let schema = config.get_str("schema").unwrap();
        let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
        let seed = config.get_int("seed").unwrap();
        let mut rng = StdRng::seed_from_u64(seed.try_into().unwrap());
        loader::populate_tables(&internals, &mut rng).unwrap();
        let workload = Arc::new(Workload::Tatp(internals));

        let mut pool = ThreadPool::new(workload);

        // Panic thread.
        assert_eq!(pool.execute(move || panic!("ARGHHH")).unwrap(), ());

        // Delay until shutdown.
        while !pool.is_shutdown {
            pool.listen_for_panic();
        }

        // Check pool is closed.
        assert_eq!(
            format!("{}", pool.execute(move || { Ok(()) }).unwrap_err()),
            "thread pool is closed"
        );
    }
}
