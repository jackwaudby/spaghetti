use crate::common::message::{Message, Request, Response, Transaction};
use crate::server::pool::ThreadPool;
use crate::server::scheduler::Protocol;
use crate::workloads::tatp;
use crate::workloads::tatp::profiles::TatpTransaction;
use crate::workloads::Workload;
use crate::Result;

use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tracing::{debug, error};

/// Transaction manager owns a thread pool containing workers.
///
/// # Shutdown
///
/// Graceful shutdown:
/// Occurs when all channels from the listener and read handlers are dropped.
///
/// Internal errors:
/// Thread pool may panic and close early.
pub struct TransactionManager {
    /// State
    pub state: State,

    /// Thread pool.
    pub pool: ThreadPool,

    /// Handle to scheduler.
    scheduler: Arc<Protocol>,

    /// Channel to receive requests from read handler.
    work_rx: std::sync::mpsc::Receiver<Request>,

    /// Listen for shutdown.
    pub shutdown_rx: std::sync::mpsc::Receiver<()>,

    /// Channel to notify write handler
    _notify_wh_tx: tokio::sync::broadcast::Sender<State>,
}

#[derive(Debug, Copy, Clone)]
pub enum State {
    Active,
    GracefullyShutdown,
    ThreadPoolPanicked,
}

impl TransactionManager {
    /// Create a new `TransactionManager`.
    pub fn new(
        workload: Arc<Workload>,
        work_rx: std::sync::mpsc::Receiver<Request>,
        shutdown_rx: std::sync::mpsc::Receiver<()>,
        _notify_wh_tx: tokio::sync::broadcast::Sender<State>,
    ) -> TransactionManager {
        // Create thread pool.
        let pool = ThreadPool::new(Arc::clone(&workload));
        // Create scheduler.
        let scheduler = Arc::new(Protocol::new(Arc::clone(&workload), pool.size()).unwrap());
        TransactionManager {
            state: State::Active,
            pool,
            scheduler,
            work_rx,
            shutdown_rx,
            _notify_wh_tx,
        }
    }

    /// Get shared reference to thread pool.
    pub fn get_pool(&self) -> &ThreadPool {
        &self.pool
    }

    /// Get shared reference to scheduler.
    pub fn get_scheduler(&self) -> Arc<Protocol> {
        Arc::clone(&self.scheduler)
    }

    /// Run transaction manager.
    pub fn run(&mut self) -> Result<()> {
        // Closure passed to thread pool.
        let execute_request = |request: Request, scheduler| -> Result<()> {
            // Client's # request.
            let request_no = request.request_no;
            // Start timer
            let start = Instant::now();
            // Execute trasaction.
            let res = match request.transaction {
                Transaction::Tatp(transaction) => match transaction {
                    TatpTransaction::GetSubscriberData(params) => {
                        debug!("GetSubscriberData");
                        tatp::procedures::get_subscriber_data(params, scheduler)
                    }
                    TatpTransaction::GetNewDestination(params) => {
                        debug!("GetNewDestination");
                        tatp::procedures::get_new_destination(params, scheduler)
                    }
                    TatpTransaction::GetAccessData(params) => {
                        debug!("GetAccessData");
                        tatp::procedures::get_access_data(params, scheduler)
                    }
                    TatpTransaction::UpdateSubscriberData(params) => {
                        debug!("UpdateSubscriberData");
                        tatp::procedures::update_subscriber_data(params, scheduler)
                    }
                    TatpTransaction::UpdateLocationData(params) => {
                        debug!("UpdateLocationData");
                        tatp::procedures::update_location(params, scheduler)
                    }
                    TatpTransaction::InsertCallForwarding(params) => {
                        debug!("InsertCallForwarding");
                        tatp::procedures::insert_call_forwarding(params, scheduler)
                    }
                    TatpTransaction::DeleteCallForwarding(params) => {
                        debug!("DeleteCallForwarding");
                        tatp::procedures::delete_call_forwarding(params, scheduler)
                    }
                },
                _ => unimplemented!(),
            };
            let latency = Some(start.elapsed());
            // Package response.
            let resp = match res {
                Ok(value) => Response::Committed { value: Some(value) },
                // TODO: match based on abort type and workload
                Err(reason) => Response::Aborted { reason },
            };
            // Send to corresponding `WriteHandler`.
            request
                .response_sender
                .send(Message::Response {
                    request_no,
                    resp,
                    latency,
                })
                .unwrap();
            Ok(())
        };

        loop {
            // Check if shutdown initiated.
            // All `ReadHandler`s must have closed, dropping their `Sender` handles.
            if let Err(std::sync::mpsc::TryRecvError::Disconnected) = self.shutdown_rx.try_recv() {
                debug!("Manager received shutdown from all read handlers");
                // Drain remainder of `Request`s sent from `ReadHandler`s.
                while let Ok(request) = self.work_rx.recv() {
                    // Get handle to scheduler.
                    let scheduler = Arc::clone(&self.scheduler);

                    self.pool.listen_for_panic();
                    // Send job to thread pool.
                    self.pool
                        .execute(move || execute_request(request, scheduler))?;
                    debug!("Manager sent request to pool");
                }

                assert!(self.work_rx.try_iter().next().is_none());
                debug!("Request queue empty");
                break;
            } else {
                // Normal operation.
                if let Ok(request) = self.work_rx.try_recv() {
                    // Get handle to scheduler.
                    let scheduler = Arc::clone(&self.scheduler);

                    self.pool.listen_for_panic();
                    // Send job to thread pool.
                    self.pool
                        .execute(move || execute_request(request, scheduler))?;
                    debug!("Manager sent request to pool");
                }
            }
        }
        Ok(())
    }
}

/// Start thread to run the transaction manager.
pub fn run(mut tm: TransactionManager) {
    thread::spawn(move || {
        debug!("Start transaction manager");
        if let Err(err) = tm.run() {
            error!("{}", err);
            if let Err(err) = tm._notify_wh_tx.send(State::ThreadPoolPanicked) {
                error!("{}", err);
            }
        } else {
            if let Err(err) = tm._notify_wh_tx.send(State::GracefullyShutdown) {
                error!("{}", err);
            }
        }
    });

    // Transaction Manager dropped here.
    // First threadpool is dropped which cleans itself up after finishing request.
    // Send message to each Write Handler says no more requests/
}
