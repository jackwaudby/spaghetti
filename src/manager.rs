use crate::handler::{Request, Response};
use crate::pool::ThreadPool;
use crate::scheduler::Scheduler;
use crate::workloads::tatp;

use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::thread;
use tracing::info;

#[derive(Debug)]
pub struct TransactionManager {
    /// Thread pool.
    pub pool: ThreadPool,

    /// Work queue.
    queue: ArrayQueue<Request>,

    /// Worker listener.
    work_rx: std::sync::mpsc::Receiver<Request>,

    /// Listen for shutdown notifications from handlers
    pub shutdown_rx: std::sync::mpsc::Receiver<()>,

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
        work_rx: std::sync::mpsc::Receiver<Request>,
        shutdown_rx: std::sync::mpsc::Receiver<()>,
        _notify_main_tx: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> TransactionManager {
        let pool = ThreadPool::new(size);
        let nmt = NotifyMainThread {
            sender: _notify_main_tx,
        };
        let queue = ArrayQueue::<Request>::new(100);
        TransactionManager {
            pool,
            queue,
            work_rx,
            shutdown_rx,
            _notify_main_tx: nmt,
        }
    }

    pub fn run(&mut self, scheduler: Arc<Scheduler>) {
        loop {
            // Check if shutdown.
            match self.shutdown_rx.try_recv() {
                Ok(_) => panic!("Should not receive message on this channel"),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    info!("Transaction manager received shutdown notification from handlers");
                    break;
                }
            }

            // Fill job queue.
            if let Ok(job) = self.work_rx.try_recv() {
                info!("Got job {:?}", job);
                self.queue.push(job).unwrap();
            };

            // Pop job from work queue.
            match self.queue.pop() {
                Some(job) => {
                    info!("Do job");
                    // Pass to thread pool.
                    let s = Arc::clone(&scheduler);
                    self.pool.execute(move || {
                        info!("Execute {:?}", job);
                        match job.transaction {
                            tatp::TatpTransaction::GetSubscriberData(payload) => {
                                // Register with scheduler.
                                s.register("txn1").unwrap();
                                let v = s.read(&payload.s_id.to_string(), "txn1", 1).unwrap();
                                s.commit("txn1");
                                info!("{:?}", v);
                                let resp = Response { payload: v };
                                // TODO: Send to response queue.
                                job.response_sender.send(resp);
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

pub fn run(mut tm: TransactionManager, s: Arc<Scheduler>) {
    thread::spawn(move || {
        info!("Start transaction manager");
        tm.run(s);
    });
}
