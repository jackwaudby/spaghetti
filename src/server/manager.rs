use crate::common::message::{InternalRequest, InternalResponse, Outcome, Parameters, Transaction};
use crate::server::pool::ThreadPool;
use crate::server::scheduler::Protocol;
use crate::workloads::tatp;
use crate::workloads::tatp::profiles::TatpTransactionProfile;
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
    work_rx: std::sync::mpsc::Receiver<InternalRequest>,

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
        work_rx: std::sync::mpsc::Receiver<InternalRequest>,
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
        let execute_request = |request: InternalRequest, scheduler| -> Result<()> {
            // Destructure internal request.
            let InternalRequest {
                request_no,
                transaction,
                parameters,
                response_sender,
            } = request;

            // Start timer
            let start = Instant::now();
            // Execute trasaction.
            let handle = thread::current();
            let res = match transaction {
                Transaction::Tatp(_) => {
                    if let Parameters::Tatp(params) = parameters {
                        match params {
                            TatpTransactionProfile::GetSubscriberData(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                tatp::procedures::get_subscriber_data(params, scheduler)
                            }
                            TatpTransactionProfile::GetNewDestination(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                tatp::procedures::get_new_destination(params, scheduler)
                            }
                            TatpTransactionProfile::GetAccessData(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                tatp::procedures::get_access_data(params, scheduler)
                            }
                            TatpTransactionProfile::UpdateSubscriberData(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                tatp::procedures::update_subscriber_data(params, scheduler)
                            }
                            TatpTransactionProfile::UpdateLocationData(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                tatp::procedures::update_location(params, scheduler)
                            }
                            TatpTransactionProfile::InsertCallForwarding(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                tatp::procedures::insert_call_forwarding(params, scheduler)
                            }
                            TatpTransactionProfile::DeleteCallForwarding(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                tatp::procedures::delete_call_forwarding(params, scheduler)
                            }
                        }
                    } else {
                        panic!("transaction type and parameters do not match");
                    }
                }
                Transaction::Tpcc(_) => unimplemented!(),
            };

            // Stop timer.
            let latency = Some(start.elapsed());
            // Create outcome.
            let outcome = match res {
                Ok(value) => Outcome::Committed { value: Some(value) },
                Err(reason) => Outcome::Aborted { reason },
            };
            // Create internal response.
            let response = InternalResponse {
                request_no,
                transaction,
                outcome,
                latency,
            };
            // Send to corresponding `WriteHandler`.
            response_sender.send(response).unwrap();
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
                    // debug!("Manager sent request to pool");
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
