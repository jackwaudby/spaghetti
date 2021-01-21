use crate::scheduler::Scheduler;
use crate::workloads::tatp;
use config::Config;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use tracing::{debug, info};

#[derive(Debug)]
pub struct TransactionManager {
    /// Thread pool.
    pub pool: ThreadPool,

    /// Work queue.
    queue: ArrayQueue<tatp::TatpTransaction>,

    /// Worker listener.
    work_listener_rx: std::sync::mpsc::Receiver<tatp::TatpTransaction>,

    /// Listen for shutdown notifications from handlers
    pub listener_handler_rx: std::sync::mpsc::Receiver<()>,

    /// Notify main thread of shutdown
    _notify_main_tx: NotifyMainThread,
}

#[derive(Debug)]
struct NotifyMainThread {
    sender: tokio::sync::mpsc::UnboundedSender<()>,
}

impl Drop for NotifyMainThread {
    fn drop(&mut self) {
        info!("Transaction manager sending shutdown notification to main");
    }
}

impl TransactionManager {
    /// Create transaction manager.
    pub fn new(
        size: usize,
        work_listener_rx: std::sync::mpsc::Receiver<tatp::TatpTransaction>,
        listener_handler_rx: std::sync::mpsc::Receiver<()>,
        _notify_main_tx: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> TransactionManager {
        let pool = ThreadPool::new(size);
        let nmt = NotifyMainThread {
            sender: _notify_main_tx,
        };
        let queue = ArrayQueue::<tatp::TatpTransaction>::new(100);
        TransactionManager {
            pool,
            queue,
            work_listener_rx,
            listener_handler_rx,
            _notify_main_tx: nmt,
        }
    }

    pub fn run(&mut self, scheduler: Arc<Scheduler>, config: Arc<Config>) {
        loop {
            // Check if shutdown.
            match self.listener_handler_rx.try_recv() {
                Ok(_) => panic!("Should not receive message on this channel"),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    info!("Transaction manager received shutdown notification from handlers");
                    break;
                }
            }

            // Fill job queue.
            if let Ok(job) = self.work_listener_rx.try_recv() {
                info!("Got job {:?}", job);
                self.queue.push(job);
            };

            // Pop job from work queue.
            match self.queue.pop() {
                Some(job) => {
                    info!("Do job");
                    // Pass to thread pool.
                    let s = Arc::clone(&scheduler);
                    self.pool.execute(move || {
                        info!("Execute {:?}", job);
                        match job {
                            tatp::TatpTransaction::GetSubscriberData(payload) => {
                                // Register with scheduler.
                                s.register("txn1").unwrap();
                                let v = s.read(&payload.s_id.to_string(), "txn1", 1).unwrap();
                                s.commit("txn1");
                                info!("{:?}", v);
                                // TODO: Send to response queue.
                            }
                            _ => unimplemented!(),
                        }
                    });
                }
                None => {
                    // Another channel between
                    continue;
                }
            }
        }
    }
}

// Thread pool containing `Worker`s.
#[derive(Debug)]
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: std::sync::mpsc::Sender<Message>,
}

impl ThreadPool {
    /// Create a new `ThreadPool`.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(size: usize) -> ThreadPool {
        // Must have at least 1 thread in the pool.
        assert!(size > 0);

        // Job queue, thread pool keeps sending end.
        let (sender, receiver) = std::sync::mpsc::channel();

        // Wrap receiver so can be shared across worker threads.
        let receiver = Arc::new(Mutex::new(receiver));

        // Pre-allocate vec to hold workers.
        let mut workers = Vec::with_capacity(size);

        // Create`size` threads.
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }
        ThreadPool { workers, sender }
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
        // Send to worker threads.
        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        info!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        info!("Shutting down all workers.");

        for worker in &mut self.workers {
            debug!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }

        info!("Workers shutdown.");
    }
}

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
    fn new(id: usize, receiver: Arc<Mutex<std::sync::mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    info!("Worker {} got a job; executing.", id);

                    job.call_box();
                }
                Message::Terminate => {
                    debug!("Worker {} was told to terminate.", id);

                    break;
                }
            }
        });

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
