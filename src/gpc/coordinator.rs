use crate::common::message::{InternalResponse, Message, Outcome, Parameters, Transaction};
use crate::common::statistics::LocalStatistics;
use crate::gpc::generator;
use crate::server::scheduler::Protocol;
use crate::workloads::acid;
use crate::workloads::acid::paramgen::AcidTransactionProfile;
use crate::workloads::smallbank;
use crate::workloads::smallbank::paramgen::SmallBankTransactionProfile;
use crate::workloads::tatp;
use crate::workloads::tatp::paramgen::TatpTransactionProfile;

use config::Config;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(
        id: usize,
        core_id: Option<core_affinity::CoreId>,
        config: Arc<Config>,
        scheduler: Arc<Protocol>,
        tx: mpsc::Sender<LocalStatistics>,
    ) -> Worker {
        let mut generator = generator::get_transaction_generator(Arc::clone(&config)); // initialise transaction generator
        let timeout = config.get_int("timeout").unwrap() as u64;
        let p = config.get_str("protocol").unwrap();
        let w = config.get_str("workload").unwrap();
        let max_transactions = config.get_int("transactions").unwrap() as u32;

        let mut stats = LocalStatistics::new(id as u32, &w, &p);

        let builder = thread::Builder::new().name(id.to_string().into());

        let thread = builder
            .spawn(move || {
                if let Some(core_id) = core_id {
                    core_affinity::set_for_current(core_id); // pin thread to cpu core
                }

                let mut sent = 0; // txns sent

                let st = Instant::now();
                let runtime = Duration::new(timeout * 60, 0);
                let et = st + runtime; // timeout
                loop {
                    if sent == max_transactions {
                        tracing::debug!("All transactions sent: {} = {}", sent, max_transactions);
                        break;
                    } else if Instant::now() > et {
                        tracing::debug!("Timeout reached: {} minute(s)", timeout);
                        break;
                    } else {
                        let txn = generator.get_next(); // get txn
                        let ir = execute(txn, Arc::clone(&scheduler)); // execute txn
                        let InternalResponse {
                            transaction,
                            outcome,
                            latency,
                            ..
                        } = ir;
                        stats.record(transaction, outcome.clone(), latency); // record txn
                        sent += 1;
                    }
                }

                tx.send(stats).unwrap();
                tracing::debug!("Worker {} finished", id);
            })
            .unwrap();

        Worker {
            id,

            thread: Some(thread),
        }
    }
}

/// Start execution with `cores` workers.
pub fn run(
    cores: usize,
    scheduler: Arc<Protocol>,
    config: Arc<Config>,
    tx: mpsc::Sender<LocalStatistics>,
) {
    let mut workers = Vec::with_capacity(cores); // worker per core
    let core_ids = core_affinity::get_core_ids().unwrap(); // error -- no cores found

    for (id, core_id) in core_ids[..cores].into_iter().enumerate() {
        workers.push(Worker::new(
            id as usize,
            Some(*core_id),
            Arc::clone(&config),
            Arc::clone(&scheduler),
            tx.clone(),
        ));
    }

    for mut worker in workers {
        if let Some(thread) = worker.thread.take() {
            thread.join().unwrap();
        }
    }

    drop(tx);
}

/// Execute a transaction.
fn execute(txn: Message, scheduler: Arc<Protocol>) -> InternalResponse {
    if let Message::Request {
        request_no,
        transaction,
        parameters,
    } = txn
    {
        let start = Instant::now(); // start timer
        let res = match transaction {
            Transaction::Tatp(_) => {
                if let Parameters::Tatp(params) = parameters {
                    match params {
                        TatpTransactionProfile::GetSubscriberData(params) => {
                            tatp::procedures::get_subscriber_data(params, scheduler)
                        }

                        TatpTransactionProfile::GetNewDestination(params) => {
                            tatp::procedures::get_new_destination(params, scheduler)
                        }
                        TatpTransactionProfile::GetAccessData(params) => {
                            tatp::procedures::get_access_data(params, scheduler)
                        }
                        TatpTransactionProfile::UpdateSubscriberData(params) => {
                            tatp::procedures::update_subscriber_data(params, scheduler)
                        }
                        TatpTransactionProfile::UpdateLocationData(params) => {
                            tatp::procedures::update_location(params, scheduler)
                        }
                        TatpTransactionProfile::InsertCallForwarding(params) => {
                            tatp::procedures::insert_call_forwarding(params, scheduler)
                        }
                        TatpTransactionProfile::DeleteCallForwarding(params) => {
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
                            smallbank::procedures::amalgmate(params, scheduler)
                        }
                        SmallBankTransactionProfile::Balance(params) => {
                            smallbank::procedures::balance(params, scheduler)
                        }
                        SmallBankTransactionProfile::DepositChecking(params) => {
                            smallbank::procedures::deposit_checking(params, scheduler)
                        }
                        SmallBankTransactionProfile::SendPayment(params) => {
                            smallbank::procedures::send_payment(params, scheduler)
                        }
                        SmallBankTransactionProfile::TransactSaving(params) => {
                            smallbank::procedures::transact_savings(params, scheduler)
                        }
                        SmallBankTransactionProfile::WriteCheck(params) => {
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
                            acid::procedures::g0_write(params, scheduler)
                        }
                        AcidTransactionProfile::G0Read(params) => {
                            acid::procedures::g0_read(params, scheduler)
                        }
                        AcidTransactionProfile::G1aRead(params) => {
                            acid::procedures::g1a_read(params, scheduler)
                        }
                        AcidTransactionProfile::G1aWrite(params) => {
                            acid::procedures::g1a_write(params, scheduler)
                        }
                        AcidTransactionProfile::G1cReadWrite(params) => {
                            acid::procedures::g1c_read_write(params, scheduler)
                        }
                        AcidTransactionProfile::ImpRead(params) => {
                            acid::procedures::imp_read(params, scheduler)
                        }
                        AcidTransactionProfile::ImpWrite(params) => {
                            acid::procedures::imp_write(params, scheduler)
                        }
                        AcidTransactionProfile::OtvRead(params) => {
                            acid::procedures::otv_read(params, scheduler)
                        }
                        AcidTransactionProfile::OtvWrite(params) => {
                            acid::procedures::otv_write(params, scheduler)
                        }
                        AcidTransactionProfile::LostUpdateRead(params) => {
                            acid::procedures::lu_read(params, scheduler)
                        }
                        AcidTransactionProfile::LostUpdateWrite(params) => {
                            acid::procedures::lu_write(params, scheduler)
                        }
                        AcidTransactionProfile::G2itemRead(params) => {
                            acid::procedures::g2_item_read(params, scheduler)
                        }
                        AcidTransactionProfile::G2itemWrite(params) => {
                            acid::procedures::g2_item_write(params, scheduler)
                        }
                    }
                } else {
                    panic!("transaction type and parameters do not match");
                }
            }
        };
        let latency = Some(start.elapsed()); // stop timer
        let outcome = match res {
            Ok(value) => Outcome::Committed { value: Some(value) },
            Err(reason) => Outcome::Aborted { reason },
        };

        let response = InternalResponse {
            request_no,
            transaction,
            outcome,
            latency,
        };

        response
    } else {
        panic!("expected message request");
    }
}
