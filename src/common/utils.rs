use crate::common::error::NonFatalError;
use crate::common::message::{Message, Outcome, Parameters, Transaction};
use crate::common::parameter_generation::ParameterGenerator;
use crate::common::statistics::LocalStatistics;
use crate::scheduler::Scheduler;
use crate::storage::Database;
use crate::workloads::acid::paramgen::{AcidGenerator, AcidTransactionProfile};
use crate::workloads::dummy::paramgen::{DummyGenerator, DummyTransactionProfile};
use crate::workloads::smallbank::paramgen::{SmallBankGenerator, SmallBankTransactionProfile};
use crate::workloads::tatp::paramgen::{TatpGenerator, TatpTransactionProfile};
use crate::workloads::{acid, dummy, smallbank, tatp};

use config::Config;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tracing::Level;
use tracing::{debug, info};
use tracing_subscriber::FmtSubscriber;

pub fn init_config(file: &str) -> Config {
    info!("initialise configuration using {}", file);
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file)).unwrap();
    settings
}

pub fn set_log_level(config: &Config) {
    let level = match config.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };

    let file_appender =
        tracing_appender::rolling::hourly("/Users/jackwaudby/Documents/spaghetti", "prefix.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_thread_names(true)
        .with_target(false)
        .with_writer(non_blocking)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

pub fn create_log_dir(config: &Config) {
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

pub fn init_database(config: &Config) -> Database {
    Database::new(config).unwrap()
}

pub fn init_scheduler(config: &Config) -> Scheduler {
    Scheduler::new(config).unwrap()
}

pub fn run(
    thread_id: usize,
    config: &Config,
    scheduler: &Scheduler,
    database: &Database,
    tx: mpsc::Sender<LocalStatistics>,
    rx: mpsc::Receiver<i32>,
    thx: mpsc::SyncSender<i32>,
) {
    let timeout = config.get_int("timeout").unwrap() as u64;
    let p = config.get_str("protocol").unwrap();
    let w = config.get_str("workload").unwrap();
    let max_transactions = config.get_int("transactions").unwrap() as u32;
    let record = config.get_bool("record").unwrap();
    let log_results = config.get_bool("log_results").unwrap();
    let mut stats = LocalStatistics::new(thread_id as u32, &w, &p);
    let mut generator = get_transaction_generator(thread_id as u32, config); // initialise transaction generator

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
            file = format!(
                "./log/acid/{}/{}/thread-{}.json",
                protocol, anomaly, thread_id
            );
        } else {
            dir = format!("./log/{}/{}/", workload, protocol);
            file = format!("./log/{}/{}/thread-{}.json", workload, protocol, thread_id);
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
            info!("All transactions sent: {} ", completed);

            let res = thx.send(1);
            match res {
                Ok(_) => {}
                Err(e) => debug!("{}", e),
            }

            break;
        } else if Instant::now() > timeout_end {
            info!("Timeout reached: {} minute(s)", timeout);

            let res = thx.send(1);
            match res {
                Ok(_) => {}
                Err(e) => debug!("{}", e),
            }

            break;
        } else if rx.try_recv().is_ok() {
            info!("Received emergency shutdown");
            break;
        } else {
            let txn = generator.get_next(); // generate txn
            let start_latency = Instant::now(); // start measuring latency
            let response = execute(txn.clone(), scheduler, database); // execute txn

            // Check that thread has not run into a problem
            if let Message::Response { outcome, .. } = response.clone() {
                if let Outcome::Aborted(err) = outcome {
                    if let NonFatalError::Emergency = err {
                        let res = thx.send(2);
                        match res {
                            Ok(_) => {}
                            Err(e) => debug!("{}", e),
                        }
                        info!("Exit thread");
                        break;
                    }
                }
            }

            stats.record(&response); // record response
            if log_results {
                log_result(&mut fh, &response); // log response
            }
            stats.stop_latency(start_latency); // stop measuring latency
            completed += 1;
        }
    }

    stats.stop_worker(start_worker);

    if record {
        tx.send(stats).unwrap();
    }
}

/// Execute a transaction.
pub fn execute<'a>(txn: Message, scheduler: &'a Scheduler, workload: &'a Database) -> Message {
    if let Message::Request {
        request_no,
        transaction,
        parameters,
        isolation,
    } = txn
    {
        let res = match transaction {
            Transaction::Tatp(_) => {
                if let Parameters::Tatp(params) = parameters {
                    match params {
                        TatpTransactionProfile::GetSubscriberData(params) => {
                            tatp::procedures::get_subscriber_data(
                                params, scheduler, workload, isolation,
                            )
                        }
                        TatpTransactionProfile::GetAccessData(params) => {
                            tatp::procedures::get_access_data(
                                params, scheduler, workload, isolation,
                            )
                        }
                        TatpTransactionProfile::GetNewDestination(params) => {
                            tatp::procedures::get_new_destination(
                                params, scheduler, workload, isolation,
                            )
                        }
                        TatpTransactionProfile::UpdateLocationData(params) => {
                            tatp::procedures::update_location(
                                params, scheduler, workload, isolation,
                            )
                        }
                        TatpTransactionProfile::UpdateSubscriberData(params) => {
                            tatp::procedures::update_subscriber_data(
                                params, scheduler, workload, isolation,
                            )
                        }
                    }
                } else {
                    panic!("transaction type and parameters do not match");
                }
            }
            Transaction::Dummy(_) => {
                if let Parameters::Dummy(params) = parameters {
                    match params {
                        DummyTransactionProfile::Read(_params) => {
                            unimplemented!()
                            // tatp::procedures::get_subscriber_data(
                            //     params, scheduler, workload, isolation,
                            // )
                        }
                        DummyTransactionProfile::ReadWrite(_params) => {
                            unimplemented!()
                            // tatp::procedures::get_subscriber_data(
                            //     params, scheduler, workload, isolation,
                            // )
                        }
                        DummyTransactionProfile::Write(params) => {
                            dummy::procedures::write_only(params, scheduler, workload)
                        }

                        DummyTransactionProfile::WriteAbort(params) => {
                            dummy::procedures::write_only_abort(params, scheduler, workload)
                        }
                    }
                } else {
                    panic!("transaction type and parameters do not match");
                }
            }

            Transaction::Acid(_) => {
                if let Parameters::Acid(params) = parameters {
                    match params {
                        AcidTransactionProfile::G0Write(params) => {
                            acid::procedures::g0_write(params, scheduler, workload)
                        }
                        AcidTransactionProfile::G0Read(params) => {
                            acid::procedures::g0_read(params, scheduler, workload)
                        }
                        AcidTransactionProfile::G1aRead(params) => {
                            acid::procedures::g1a_read(params, scheduler, workload)
                        }
                        AcidTransactionProfile::G1aWrite(params) => {
                            acid::procedures::g1a_write(params, scheduler, workload)
                        }
                        AcidTransactionProfile::G1cReadWrite(params) => {
                            acid::procedures::g1c_read_write(params, scheduler, workload)
                        }
                        AcidTransactionProfile::ImpRead(params) => {
                            acid::procedures::imp_read(params, scheduler, workload)
                        }
                        AcidTransactionProfile::ImpWrite(params) => {
                            acid::procedures::imp_write(params, scheduler, workload)
                        }
                        AcidTransactionProfile::OtvRead(params) => {
                            acid::procedures::otv_read(params, scheduler, workload)
                        }
                        AcidTransactionProfile::OtvWrite(params) => {
                            acid::procedures::otv_write(params, scheduler, workload)
                        }
                        AcidTransactionProfile::LostUpdateRead(params) => {
                            acid::procedures::lu_read(params, scheduler, workload)
                        }
                        AcidTransactionProfile::LostUpdateWrite(params) => {
                            acid::procedures::lu_write(params, scheduler, workload)
                        }
                        AcidTransactionProfile::G2itemRead(params) => {
                            acid::procedures::g2_item_read(params, scheduler, workload)
                        }
                        AcidTransactionProfile::G2itemWrite(params) => {
                            acid::procedures::g2_item_write(params, scheduler, workload)
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
                            smallbank::procedures::amalgmate(params, scheduler, workload, isolation)
                        }
                        SmallBankTransactionProfile::Balance(params) => {
                            smallbank::procedures::balance(params, scheduler, workload, isolation)
                        }
                        SmallBankTransactionProfile::DepositChecking(params) => {
                            smallbank::procedures::deposit_checking(
                                params, scheduler, workload, isolation,
                            )
                        }
                        SmallBankTransactionProfile::SendPayment(params) => {
                            smallbank::procedures::send_payment(
                                params, scheduler, workload, isolation,
                            )
                        }
                        SmallBankTransactionProfile::TransactSaving(params) => {
                            smallbank::procedures::transact_savings(
                                params, scheduler, workload, isolation,
                            )
                        }
                        SmallBankTransactionProfile::WriteCheck(params) => {
                            smallbank::procedures::write_check(
                                params, scheduler, workload, isolation,
                            )
                        }
                    }
                } else {
                    panic!("transaction type and parameters do not match");
                }
            }
        };

        let outcome = match res {
            Ok(success) => Outcome::Committed(success),
            Err(failed) => Outcome::Aborted(failed),
        };

        Message::Response {
            request_no,
            transaction,
            isolation,
            outcome,
        }
    } else {
        panic!("expected message request");
    }
}

pub fn get_transaction_generator(thread_id: u32, config: &Config) -> ParameterGenerator {
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
        "smallbank" => {
            let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
            let gen = SmallBankGenerator::new(thread_id, sf, set_seed, seed, use_balance_mix);
            ParameterGenerator::SmallBank(gen)
        }
        "acid" => {
            let anomaly = config.get_str("anomaly").unwrap();
            let delay = config.get_int("delay").unwrap() as u64;
            let gen = AcidGenerator::new(thread_id, sf, set_seed, seed, &anomaly, delay);

            ParameterGenerator::Acid(gen)
        }
        "dummy" => {
            let gen = DummyGenerator::new(thread_id, sf, set_seed, seed);

            ParameterGenerator::Dummy(gen)
        }
        "tatp" => {
            let use_nurand = config.get_bool("nurand").unwrap();
            let gen = TatpGenerator::new(thread_id, sf, set_seed, seed, use_nurand);
            ParameterGenerator::Tatp(gen)
        }
        _ => unimplemented!(),
    }
}

pub fn log_result(fh: &mut Option<std::fs::File>, response: &Message) {
    if let Message::Response { .. } = response {
        if let Some(ref mut fh) = fh {
            let res = serde_json::to_string(response).unwrap();
            writeln!(fh, "{}", res).unwrap();

            // match outcome {
            //     Outcome::Committed(value) => {
            //         writeln!(fh, "{}", &value.unwrap()).unwrap();
            //     }
            //     Outcome::Aborted(reason) => {
            //         let x = format!("{}", reason);
            //         let value = serde_json::to_string(&x).unwrap();
            //         writeln!(fh, "{{\"id\":{},\"aborted\":{}}}", request_no, &value).unwrap();
            //     }
            // }
        }
    }
}
