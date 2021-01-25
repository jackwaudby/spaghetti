use crate::common::message::Response;
use crate::server::handler::Request;
use crate::server::pool::ThreadPool;
use crate::server::scheduler::Scheduler;
use crate::workloads::tatp;

use std::sync::Arc;
use std::thread;
use tracing::info;

#[derive(Debug)]
pub struct TransactionManager {
    /// Thread pool.
    pub pool: ThreadPool,

    /// Worker listener.
    work_rx: std::sync::mpsc::Receiver<Request>,

    /// Listen for shutdown notifications from handlers
    pub shutdown_rx: std::sync::mpsc::Receiver<()>,

    /// Notify `WriteHandler`s of shutdown.
    _notify_wh_tx: NotifyWriteHandlers,
}

// Graceful shutdown.
// `ReadHandler`s and the `TransactionManager` are linked by a std mpsc channel.
// Each spawned `ReadHandler` gets a `Sender` which is dropped when it receives a
// shutdown notification.

#[derive(Debug)]
struct NotifyWriteHandlers {
    sender: tokio::sync::broadcast::Sender<()>,
}

impl Drop for NotifyWriteHandlers {
    fn drop(&mut self) {
        info!("Transaction manager broadcasting shutdown notification to write handlers.");
    }
}

impl TransactionManager {
    /// Create transaction manager.
    pub fn new(
        size: usize,
        work_rx: std::sync::mpsc::Receiver<Request>,
        shutdown_rx: std::sync::mpsc::Receiver<()>,
        _notify_wh_tx: tokio::sync::broadcast::Sender<()>,
    ) -> TransactionManager {
        let pool = ThreadPool::new(size);
        let nwhs = NotifyWriteHandlers {
            sender: _notify_wh_tx,
        };

        TransactionManager {
            pool,
            work_rx,
            shutdown_rx,
            _notify_wh_tx: nwhs,
        }
    }

    pub fn run(&mut self, scheduler: Arc<Scheduler>) {
        loop {
            // Check if shutdown initiated.
            // All `ReadHandler`s must have closed, dropping their `Sender` handles.
            if let Err(std::sync::mpsc::TryRecvError::Disconnected) = self.shutdown_rx.try_recv() {
                info!("Transaction manager received shutdown notification from all read handlers");
                // Drain remainder of `Request`s sent from `ReadHandler`s.
                while let Ok(request) = self.work_rx.recv() {
                    info!("Pass request to thread pool");
                    // Get handle to scheduler.
                    let s = Arc::clone(&scheduler);
                    self.pool.execute(move || {
                        info!("Execute request: {:?}", request);
                        match request.transaction {
                            tatp::TatpTransaction::GetSubscriberData(payload) => {
                                // Register with scheduler.
                                s.register("txn1").unwrap();
                                // Stored procedure.
                                let v = s.read(&payload.s_id.to_string(), "txn1", 1).unwrap();
                                // Commit transaction.
                                s.commit("txn1");
                                // Package response.
                                let resp = Response { payload: v };
                                // Send to corresponding `WriteHandler`.
                                request.response_sender.send(resp).unwrap();
                            }
                            _ => unimplemented!(),
                        }
                    });
                }

                assert!(self.work_rx.try_iter().next().is_none());
                info!("Request queue empty");
                break;
            } else {
                // Normal operation.
                if let Ok(request) = self.work_rx.try_recv() {
                    info!("Transaction manager received {:?}", request.transaction);
                    info!("Submit to workers.");
                    let s = Arc::clone(&scheduler);
                    self.pool.execute(move || {
                        match request.transaction {
                            tatp::TatpTransaction::GetSubscriberData(payload) => {
                                // Register with scheduler.
                                s.register("txn1").unwrap();
                                // Stored procedure.
                                let v = s.read(&payload.s_id.to_string(), "txn1", 1).unwrap();
                                // Commit transaction.
                                s.commit("txn1");
                                // Package response.
                                let resp = Response { payload: v };
                                info!("Send response {:?} to write handler", resp);
                                request.response_sender.send(resp).unwrap();
                            }
                            _ => unimplemented!(),
                        }
                    });
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
    // Transaction Manager dropped here.
    // First threadpool is dropped which cleans itself up after finishing request.
    // Send message to each Write Handler says no more requests/
}
