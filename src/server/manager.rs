use crate::common::message::{Message, Request, Response, Transaction};
use crate::server::pool::ThreadPool;
use crate::server::scheduler::Protocol;
use crate::workloads::tatp;
use crate::workloads::tatp::profiles::TatpTransaction;
use crate::workloads::Workload;

use std::sync::Arc;
use std::thread;
use tracing::debug;

/// Transaction manager owns a thread pool containing workers.
pub struct TransactionManager {
    /// Thread pool.
    pub pool: ThreadPool,

    /// Scheduler.
    scheduler: Arc<Protocol>,

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
        debug!("Transaction manager broadcasting shutdown notification to write handlers.");
    }
}

impl TransactionManager {
    /// Create a new `TransactionManager`.
    pub fn new(
        workload: Arc<Workload>,
        work_rx: std::sync::mpsc::Receiver<Request>,
        shutdown_rx: std::sync::mpsc::Receiver<()>,
        _notify_wh_tx: tokio::sync::broadcast::Sender<()>,
    ) -> TransactionManager {
        // Create thread pool.
        let pool = ThreadPool::new(Arc::clone(&workload));
        // Create scheduler.
        let scheduler = Arc::new(Protocol::new(Arc::clone(&workload), pool.size()).unwrap());
        // Shutdown notification.
        let nwhs = NotifyWriteHandlers {
            sender: _notify_wh_tx,
        };

        TransactionManager {
            pool,
            scheduler,
            work_rx,
            shutdown_rx,
            _notify_wh_tx: nwhs,
        }
    }

    pub fn get_pool(&self) -> &ThreadPool {
        &self.pool
    }

    pub fn get_scheduler(&self) -> Arc<Protocol> {
        Arc::clone(&self.scheduler)
    }

    pub fn run(&mut self) {
        loop {
            // Check if shutdown initiated.
            // All `ReadHandler`s must have closed, dropping their `Sender` handles.
            if let Err(std::sync::mpsc::TryRecvError::Disconnected) = self.shutdown_rx.try_recv() {
                debug!("Transaction manager received shutdown notification from all read handlers");
                // Drain remainder of `Request`s sent from `ReadHandler`s.
                while let Ok(request) = self.work_rx.recv() {
                    debug!("Pass request to thread pool");
                    // Get handle to scheduler.
                    let scheduler = Arc::clone(&self.scheduler);

                    // Send job to thread pool.
                    self.pool.execute(move || {
                        debug!("Execute request: {:?}", request);
                        // Client's # request.
                        let request_no = request.request_no;
                        // Execute trasaction.
                        let res = match request.transaction {
                            Transaction::Tatp(transaction) => match transaction {
                                TatpTransaction::GetSubscriberData(params) => {
                                    tatp::procedures::get_subscriber_data(params, scheduler)
                                }
                                TatpTransaction::GetNewDestination(params) => {
                                    tatp::procedures::get_new_destination(params, scheduler)
                                }
                                TatpTransaction::GetAccessData(params) => {
                                    tatp::procedures::get_access_data(params, scheduler)
                                }
                                TatpTransaction::UpdateSubscriberData(params) => {
                                    tatp::procedures::update_subscriber_data(params, scheduler)
                                }
                                TatpTransaction::UpdateLocationData(params) => {
                                    tatp::procedures::update_location(params, scheduler)
                                }
                                TatpTransaction::InsertCallForwarding(params) => {
                                    tatp::procedures::insert_call_forwarding(params, scheduler)
                                }
                                TatpTransaction::DeleteCallForwarding(params) => {
                                    tatp::procedures::delete_call_forwarding(params, scheduler)
                                }
                            },
                            _ => unimplemented!(),
                        };
                        // Package response.
                        let resp = match res {
                            Ok(res) => Response::Committed { value: Some(res) },
                            Err(e) => Response::Aborted {
                                err: format!("err=\"{}\"", e.source().unwrap()),
                            },
                        };
                        // Send to corresponding `WriteHandler`.
                        request
                            .response_sender
                            .send(Message::Response { request_no, resp })
                            .unwrap();
                    });
                }

                assert!(self.work_rx.try_iter().next().is_none());
                debug!("Request queue empty");
                break;
            } else {
                // Normal operation.
                if let Ok(request) = self.work_rx.try_recv() {
                    debug!("Pass request to thread pool");
                    // Get handle to scheduler.
                    let scheduler = Arc::clone(&self.scheduler);
                    // Assign transaction id and timestamp.
                    let request_no = request.request_no;

                    self.pool.execute(move || {
                        // Execute trasaction.
                        let res = match request.transaction {
                            Transaction::Tatp(transaction) => match transaction {
                                TatpTransaction::GetSubscriberData(params) => {
                                    tatp::procedures::get_subscriber_data(params, scheduler)
                                }
                                TatpTransaction::GetNewDestination(params) => {
                                    tatp::procedures::get_new_destination(params, scheduler)
                                }
                                TatpTransaction::GetAccessData(params) => {
                                    tatp::procedures::get_access_data(params, scheduler)
                                }
                                TatpTransaction::UpdateSubscriberData(params) => {
                                    tatp::procedures::update_subscriber_data(params, scheduler)
                                }
                                TatpTransaction::UpdateLocationData(params) => {
                                    tatp::procedures::update_location(params, scheduler)
                                }
                                TatpTransaction::InsertCallForwarding(params) => {
                                    tatp::procedures::insert_call_forwarding(params, scheduler)
                                }
                                TatpTransaction::DeleteCallForwarding(params) => {
                                    tatp::procedures::delete_call_forwarding(params, scheduler)
                                }
                            },
                            _ => unimplemented!(),
                        };
                        // Package response.
                        let resp = match res {
                            Ok(res) => Response::Committed { value: Some(res) },
                            Err(e) => Response::Aborted {
                                err: format!("{{err=\"{:?}\"}}", e.source()),
                            },
                        };

                        // Send to corresponding `WriteHandler`.
                        request
                            .response_sender
                            .send(Message::Response { request_no, resp })
                            .unwrap();
                    });
                }
            }
        }
    }
}

pub fn run(mut tm: TransactionManager) {
    thread::spawn(move || {
        debug!("Start transaction manager");
        tm.run();
    });

    // Transaction Manager dropped here.
    // First threadpool is dropped which cleans itself up after finishing request.
    // Send message to each Write Handler says no more requests/
}
