use crate::common::message::{InternalResponse, Outcome, Parameters, Transaction};
use crate::embedded::generator::InternalRequest;
use crate::embedded::pool::ThreadPool;
use crate::server::scheduler::Protocol;
use crate::workloads::acid;
use crate::workloads::acid::paramgen::AcidTransactionProfile;
use crate::workloads::smallbank;
use crate::workloads::smallbank::paramgen::SmallBankTransactionProfile;
use crate::workloads::tatp;
use crate::workloads::tatp::paramgen::TatpTransactionProfile;
use crate::workloads::Workload;
use crate::Result;

use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tracing::{debug, info};

/// Transaction manager owns a thread pool containing workers.
///
/// # Shutdown
///
/// Graceful shutdown:
/// Occurs when the generator finishes.
///
/// Internal errors:
/// Thread pool may panic and close early.
pub struct TransactionManager {
    /// Thread pool.
    pool: ThreadPool,

    /// Handle to scheduler.
    scheduler: Arc<Protocol>,

    /// Channel to receive requests from generator.
    request_rx: Receiver<InternalRequest>,
}

impl TransactionManager {
    /// Create a new `TransactionManager`.
    pub fn new(
        workload: Arc<Workload>,
        request_rx: Receiver<InternalRequest>,
        next_tx: SyncSender<()>,
    ) -> TransactionManager {
        let pool = ThreadPool::new(Arc::clone(&workload), next_tx);
        let scheduler = Arc::new(Protocol::new(Arc::clone(&workload), pool.size()).unwrap());

        TransactionManager {
            pool,
            scheduler,
            request_rx,
        }
    }

    /// Run transaction manager.
    pub fn run(&mut self) {
        // Closure passed to thread pool.
        let execute_request = |request: InternalRequest, scheduler: Arc<Protocol>| -> Result<()> {
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
                Transaction::SmallBank(_) => {
                    if let Parameters::SmallBank(params) = parameters {
                        match params {
                            SmallBankTransactionProfile::Amalgamate(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                smallbank::procedures::amalgmate(params, scheduler)
                            }
                            SmallBankTransactionProfile::Balance(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                smallbank::procedures::balance(params, scheduler)
                            }
                            SmallBankTransactionProfile::DepositChecking(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                smallbank::procedures::deposit_checking(params, scheduler)
                            }
                            SmallBankTransactionProfile::SendPayment(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                smallbank::procedures::send_payment(params, scheduler)
                            }
                            SmallBankTransactionProfile::TransactSaving(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                smallbank::procedures::transact_savings(params, scheduler)
                            }
                            SmallBankTransactionProfile::WriteCheck(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                smallbank::procedures::write_check(params, scheduler)
                            }
                        }
                    } else {
                        panic!("transaction type and parameters do not match");
                    }
                }
                Transaction::Tpcc(_) => unimplemented!(),
                Transaction::Acid(_) => {
                    if let Parameters::Acid(params) = parameters {
                        match params {
                            AcidTransactionProfile::G0Write(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::g0_write(params, scheduler)
                            }
                            AcidTransactionProfile::G0Read(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::g0_read(params, scheduler)
                            }
                            AcidTransactionProfile::G1aRead(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::g1a_read(params, scheduler)
                            }
                            AcidTransactionProfile::G1aWrite(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::g1a_write(params, scheduler)
                            }
                            AcidTransactionProfile::G1cReadWrite(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::g1c_read_write(params, scheduler)
                            }
                            AcidTransactionProfile::ImpRead(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::imp_read(params, scheduler)
                            }
                            AcidTransactionProfile::ImpWrite(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::imp_write(params, scheduler)
                            }
                            AcidTransactionProfile::OtvRead(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::otv_read(params, scheduler)
                            }
                            AcidTransactionProfile::OtvWrite(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::otv_write(params, scheduler)
                            }
                            AcidTransactionProfile::LostUpdateRead(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::lu_read(params, scheduler)
                            }
                            AcidTransactionProfile::LostUpdateWrite(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::lu_write(params, scheduler)
                            }
                            AcidTransactionProfile::G2itemRead(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::g2_item_read(params, scheduler)
                            }
                            AcidTransactionProfile::G2itemWrite(params) => {
                                debug!("Thread {}: {:?}", handle.name().unwrap(), params);
                                acid::procedures::g2_item_write(params, scheduler)
                            }
                        }
                    } else {
                        panic!("transaction type and parameters do not match");
                    }
                }
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

            // Send to logger.
            response_sender.send(response).unwrap();
            Ok(())
        };

        while let Ok(request) = self.request_rx.recv() {
            let scheduler = Arc::clone(&self.scheduler);
            self.pool.listen_for_panic();
            self.pool
                .execute(move || execute_request(request, scheduler))
                .unwrap();
        }
    }
}

/// Run transaction manager on a thread.
pub fn run(mut tm: TransactionManager) {
    thread::spawn(move || {
        info!("Start transaction manager");
        tm.run();
        info!("Transaction manager closing...");
    });
}
