use crate::common::message::{Message, Request, Response, Transaction};
use crate::server::pool::ThreadPool;
use crate::server::scheduler::Scheduler;
use crate::workloads::tatp::{self, TatpTransaction};

use chrono::offset::Utc;
use chrono::DateTime;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;
use tracing::debug;

/// Transaction manager owns a thread pool containing workers.
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
        debug!("Transaction manager broadcasting shutdown notification to write handlers.");
    }
}

impl TransactionManager {
    /// Create a new `TransactionManager`.
    pub fn new(
        size: usize,
        work_rx: std::sync::mpsc::Receiver<Request>,
        shutdown_rx: std::sync::mpsc::Receiver<()>,
        _notify_wh_tx: tokio::sync::broadcast::Sender<()>,
    ) -> TransactionManager {
        // Create thread pool.
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
                debug!("Transaction manager received shutdown notification from all read handlers");
                // Drain remainder of `Request`s sent from `ReadHandler`s.
                while let Ok(mut request) = self.work_rx.recv() {
                    debug!("Pass request to thread pool");
                    // Get handle to scheduler.
                    let scheduler = Arc::clone(&scheduler);
                    // Assign transaction id and timestamp.
                    let sys_time = SystemTime::now();
                    let datetime: DateTime<Utc> = sys_time.into();
                    request.id = Some(datetime.to_string());
                    request.timestamp = Some(datetime);

                    // Send job to thread pool.
                    self.pool.execute(move || {
                        debug!("Execute request: {:?}", request);
                        match request.transaction {
                            Transaction::Tatp(transaction) => match transaction {
                                TatpTransaction::GetSubscriberData(params) => {
                                    // Subscriber ID.
                                    let s_id = &params.s_id.to_string();
                                    // Transaction ID.
                                    let t_id = &request.id.unwrap();
                                    // Transaction timestamp.
                                    let t_ts = request.timestamp.unwrap();
                                    // Stored procedure.
                                    let res =
                                        tatp::get_subscriber_data(s_id, t_id, t_ts, scheduler);
                                    // Package response.
                                    let resp = match res {
                                        Ok(res) => Response::Committed { value: Some(res) },
                                        Err(e) => Response::Aborted {
                                            err: format!("  Caused by: {}", e.source().unwrap()),
                                        },
                                    };
                                    // Send to corresponding `WriteHandler`.
                                    request
                                        .response_sender
                                        .send(Message::Response(resp))
                                        .unwrap();
                                }
                                _ => unimplemented!(),
                            },
                            _ => unimplemented!(),
                        }
                    });
                }

                assert!(self.work_rx.try_iter().next().is_none());
                debug!("Request queue empty");
                break;
            } else {
                // Normal operation.
                if let Ok(request) = self.work_rx.try_recv() {
                    debug!("Transaction manager received {:?}", request.transaction);
                    debug!("Submit to workers.");
                    let s = Arc::clone(&scheduler);
                    self.pool.execute(move || {
                        debug!("Execute request: {:?}", request);
                        match request.transaction {
                            Transaction::Tatp(transaction) => match transaction {
                                TatpTransaction::GetSubscriberData(params) => {
                                    // Subscriber ID.
                                    let s_id = &params.s_id.to_string();
                                    // Transaction ID.
                                    let t_id = &request.id.unwrap();
                                    // Transaction timestamp.
                                    let t_ts = request.timestamp.unwrap();
                                    // Stored procedure.
                                    let res = tatp::get_subscriber_data(s_id, t_id, t_ts, s);
                                    // Package response.
                                    let resp = match res {
                                        Ok(res) => Response::Committed { value: Some(res) },
                                        Err(e) => Response::Aborted {
                                            err: format!("  Caused by: {}", e.source().unwrap()),
                                        },
                                    };
                                    // Send to corresponding `WriteHandler`.
                                    request
                                        .response_sender
                                        .send(Message::Response(resp))
                                        .unwrap();
                                }
                                _ => unimplemented!(),
                            },
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
        debug!("Start transaction manager");
        tm.run(s);
    });
    // Transaction Manager dropped here.
    // First threadpool is dropped which cleans itself up after finishing request.
    // Send message to each Write Handler says no more requests/
}
