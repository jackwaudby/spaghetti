use crate::common::error::NonFatalError;
use crate::common::message::{InternalResponse, Message, Outcome, Parameters, Transaction};
use crate::common::parameter_generation::ParameterGenerator;
use crate::common::statistics::LocalStatistics;
use crate::common::wait_manager::WaitManager;
use crate::gpc::threads::{Recon, Worker};

use crate::scheduler::Scheduler;
use crate::workloads::smallbank;
use crate::workloads::smallbank::paramgen::{SmallBankGenerator, SmallBankTransactionProfile};
use crate::workloads::Database;

use crate::gpc::helper;
use crate::gpc::threads::log_result;

use config::Config;
use crossbeam_utils::thread;
use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

/// Initialise configuration.
pub fn init_config(file: &str) -> Config {
    tracing::info!("initialise configuration using {}", file);
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file)).unwrap();

    settings
}

/// Initialise result logging, creating the required directory
pub fn create_results_dir(config: &Config) {
    let log_results = config.get_bool("log_results").unwrap();

    if log_results {
        let workload = config.get_str("workload").unwrap();
        let protocol = config.get_str("protocol").unwrap();

        let dir;
        if workload.as_str() == "acid" {
            let anomaly = config.get_str("anomaly").unwrap();
            dir = format!("./log/{}/{}/{}/", workload, protocol, anomaly);
        } else {
            dir = format!("./log/{}/{}/", workload, protocol);
        }

        if Path::new(&dir).exists() {
            fs::remove_dir_all(&dir).unwrap(); // if exists remove dir
        }

        fs::create_dir_all(&dir).unwrap(); // create directory
    }
}

/// Set logging level.
pub fn set_log_level(config: &Config) {
    let level = match config.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

/// Initialise database.
pub fn init_database(config: &Config) -> Database {
    Database::new(config).unwrap()
}

/// Initialise the scheduler.
pub fn init_scheduler(config: &Config) -> Scheduler {
    Scheduler::new(config).unwrap()
}

pub fn run(
    config: &Config,
    scheduler: &Scheduler,
    database: &Database,
    tx: mpsc::Sender<LocalStatistics>,
) {
    let id = 1;
    let timeout = config.get_int("timeout").unwrap() as u64;
    let p = config.get_str("protocol").unwrap();
    let w = config.get_str("workload").unwrap();
    let max_transactions = config.get_int("transactions").unwrap() as u32;
    let record = config.get_bool("record").unwrap();
    let log_results = config.get_bool("log_results").unwrap();
    let mut stats = LocalStatistics::new(id as u32, &w, &p);

    let mut generator = helper::get_transaction_generator(config); // initialise transaction generator

    let mut wm = WaitManager::new();

    // create results file -- dir created by this point
    let mut fh;
    if log_results {
        let workload = config.get_str("workload").unwrap();
        let protocol = config.get_str("protocol").unwrap();

        let dir;
        let file;
        if workload.as_str() == "acid" {
            let anomaly = config.get_str("anomaly").unwrap();
            dir = format!("./log/{}/{}/{}/", workload, protocol, anomaly);
            file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, id);
        } else {
            dir = format!("./log/{}/{}/", workload, protocol);
            file = format!("./log/{}/{}/thread-{}.json", workload, protocol, id);
        }

        if Path::new(&dir).exists() {
            if Path::new(&file).exists() {
                fs::remove_file(&file).unwrap(); // remove file if already exists
            }
        } else {
            panic!("dir should exist");
        }

        fh = Some(
            OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(&file)
                .expect("cannot open file"),
        );
    } else {
        fh = None;
    }

    let mut completed = 0;

    let timeout_start = Instant::now(); // timeout
    let runtime = Duration::new(timeout * 60, 0);
    let timeout_end = timeout_start + runtime;

    let start_worker = Instant::now();

    loop {
        if completed == max_transactions {
            tracing::debug!(
                "All transactions sent: {} = {}",
                completed,
                max_transactions
            );
            break;
        } else if Instant::now() > timeout_end {
            tracing::info!("Timeout reached: {} minute(s)", timeout);
            break;
        } else {
            let txn = generator.get_next(); // generate txn

            let mut restart = true;

            let start_latency = Instant::now();

            while restart {
                let ir = helper::execute(txn.clone(), scheduler, database); // execute txn

                let InternalResponse {
                    transaction,
                    outcome,
                    ..
                } = ir;

                match outcome {
                    Outcome::Committed { .. } => {
                        restart = false;
                        stats.record(transaction, outcome.clone(), restart);
                        wm.reset();
                        if log_results {
                            log_result(&mut fh, outcome.clone());
                        }
                        debug!("complete: committed");
                    }
                    Outcome::Aborted { ref reason } => {
                        if let NonFatalError::SmallBankError(_) = reason {
                            restart = false;
                            wm.reset();
                            stats.record(transaction, outcome.clone(), restart);
                            if log_results {
                                log_result(&mut fh, outcome.clone());
                            }
                            debug!("complete: aborted");
                        } else {
                            restart = true; // protocol abort
                            debug!("restart: {}", reason);
                            stats.record(transaction, outcome.clone(), restart);
                            let start_wm = Instant::now();
                            wm.wait();
                            stats.stop_wait_manager(start_wm);
                        }
                    }
                }
            }

            stats.stop_latency(start_latency);
            completed += 1;
        }
    }

    stats.stop_worker(start_worker);

    if record {
        tx.send(stats).unwrap();
    }
}

/// Run recon queries.
pub fn recon_run(
    scheduler: Arc<Scheduler>,
    config: Arc<Config>,
    tx: mpsc::Sender<LocalStatistics>,
) {
    let mut recon = Recon::new(config, scheduler, tx.clone());

    if let Some(thread) = recon.thread.take() {
        thread.join().unwrap();
    }

    drop(tx);
}

/// Execute a transaction.
pub fn execute<'a>(
    txn: Message,
    scheduler: &'a Scheduler,
    workload: &'a Database,
) -> InternalResponse {
    if let Message::Request {
        request_no,
        transaction,
        parameters,
    } = txn
    {
        let res = match transaction {
            Transaction::Tatp => unimplemented!(),
            // Transaction::Tatp(_) => {
            //     if let Parameters::Tatp(params) = parameters {
            //         match params {
            //             TatpTransactionProfile::GetSubscriberData(params) => {
            //                 tatp::procedures::get_subscriber_data(params, scheduler)
            //             }
            //             TatpTransactionProfile::GetNewDestination(params) => {
            //                 tatp::procedures::get_new_destination(params, scheduler)
            //             }
            //             TatpTransactionProfile::GetAccessData(params) => {
            //                 tatp::procedures::get_access_data(params, scheduler)
            //             }
            //             TatpTransactionProfile::UpdateSubscriberData(params) => {
            //                 tatp::procedures::update_subscriber_data(params, scheduler)
            //             }
            //             TatpTransactionProfile::UpdateLocationData(params) => {
            //                 tatp::procedures::update_location(params, scheduler)
            //             }
            //         }
            //     } else {
            //         panic!("transaction type and parameters do not match");
            //     }
            // }
            Transaction::SmallBank(_) => {
                if let Parameters::SmallBank(params) = parameters {
                    match params {
                        SmallBankTransactionProfile::Amalgamate(params) => {
                            smallbank::procedures::amalgmate(params, scheduler, workload)
                        }
                        SmallBankTransactionProfile::Balance(params) => {
                            smallbank::procedures::balance(params, scheduler, workload)
                        }
                        SmallBankTransactionProfile::DepositChecking(params) => {
                            smallbank::procedures::deposit_checking(params, scheduler, workload)
                        }
                        SmallBankTransactionProfile::SendPayment(params) => {
                            smallbank::procedures::send_payment(params, scheduler, workload)
                        }
                        SmallBankTransactionProfile::TransactSaving(params) => {
                            smallbank::procedures::transact_savings(params, scheduler, workload)
                        }
                        SmallBankTransactionProfile::WriteCheck(params) => {
                            smallbank::procedures::write_check(params, scheduler, workload)
                        }
                    }
                } else {
                    panic!("transaction type and parameters do not match");
                }
            } // Transaction::Acid(_) => {
              //     if let Parameters::Acid(params) = parameters {
              //         match params {
              //             AcidTransactionProfile::G0Write(params) => {
              //                 acid::procedures::g0_write(params, scheduler)
              //             }
              //             AcidTransactionProfile::G0Read(params) => {
              //                 acid::procedures::g0_read(params, scheduler)
              //             }
              //             AcidTransactionProfile::G1aRead(params) => {
              //                 acid::procedures::g1a_read(params, scheduler)
              //             }
              //             AcidTransactionProfile::G1aWrite(params) => {
              //                 acid::procedures::g1a_write(params, scheduler)
              //             }
              //             AcidTransactionProfile::G1cReadWrite(params) => {
              //                 acid::procedures::g1c_read_write(params, scheduler)
              //             }
              //             AcidTransactionProfile::ImpRead(params) => {
              //                 acid::procedures::imp_read(params, scheduler)
              //             }
              //             AcidTransactionProfile::ImpWrite(params) => {
              //                 acid::procedures::imp_write(params, scheduler)
              //             }
              //             AcidTransactionProfile::OtvRead(params) => {
              //                 acid::procedures::otv_read(params, scheduler)
              //             }
              //             AcidTransactionProfile::OtvWrite(params) => {
              //                 acid::procedures::otv_write(params, scheduler)
              //             }
              //             AcidTransactionProfile::LostUpdateRead(params) => {
              //                 acid::procedures::lu_read(params, scheduler)
              //             }
              //             AcidTransactionProfile::LostUpdateWrite(params) => {
              //                 acid::procedures::lu_write(params, scheduler)
              //             }
              //             AcidTransactionProfile::G2itemRead(params) => {
              //                 acid::procedures::g2_item_read(params, scheduler)
              //             }
              //             AcidTransactionProfile::G2itemWrite(params) => {
              //                 acid::procedures::g2_item_write(params, scheduler)
              //             }
              //         }
              //     } else {
              //         panic!("transaction type and parameters do not match");
              //     }
              //}
        };

        let outcome = match res {
            Ok(value) => Outcome::Committed { value: Some(value) },
            Err(reason) => Outcome::Aborted { reason },
        };

        InternalResponse {
            request_no,
            transaction,
            outcome,
        }
    } else {
        panic!("expected message request");
    }
}

/// Get transaction generator
pub fn get_transaction_generator(config: &Config) -> ParameterGenerator {
    let sf = config.get_int("scale_factor").unwrap() as u64;
    let set_seed = config.get_bool("set_seed").unwrap();
    let seed;
    if set_seed {
        let s = config.get_int("seed").unwrap() as u64;
        seed = Some(s);
    } else {
        seed = None;
    }
    match config.get_str("workload").unwrap().as_str() {
        // "acid" => {
        //     let anomaly = config.get_str("anomaly").unwrap();
        //     let delay = config.get_int("delay").unwrap() as u64;
        //     let gen = AcidGenerator::new(sf, set_seed, seed, &anomaly, delay);
        //     ParameterGenerator::Acid(gen)
        // }
        // "tatp" => {
        //     let use_nurand = config.get_bool("nurand").unwrap();

        //     let gen = TatpGenerator::new(sf, set_seed, seed, use_nurand);
        //     ParameterGenerator::Tatp(gen)
        // }
        "smallbank" => {
            let use_balance_mix = config.get_bool("use_balance_mix").unwrap();

            let gen = SmallBankGenerator::new(sf, set_seed, seed, use_balance_mix);
            ParameterGenerator::SmallBank(gen)
        }
        _ => unimplemented!(),
    }
}
