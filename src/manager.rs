use crate::pool::ThreadPool;
use crate::scheduler::Scheduler;
use crate::workloads::tatp;

use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use tracing::info;

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

    pub fn run(&mut self, scheduler: Arc<Scheduler>) {
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
