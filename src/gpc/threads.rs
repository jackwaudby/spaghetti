use crate::common::error::NonFatalError;
use crate::common::message::InternalResponse;
use crate::common::message::Outcome;
use crate::common::statistics::LocalStatistics;
use crate::common::wait_manager::WaitManager;
use crate::gpc::helper;
use crate::scheduler::Scheduler;
use crate::workloads::Database;

use config::Config;

//use crossbeam_utils::thread;

use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};
use tracing::debug;

#[derive(Debug)]
pub struct Worker {
    id: usize,
    pub thread: Option<thread::JoinHandle<()>>,
}

#[derive(Debug)]
pub struct Recon {
    pub thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    // pub fn new<'a>(
    //     id: usize,
    //     core_id: Option<core_affinity::CoreId>,
    //     config: &'a Config,
    //     scheduler: &'a Scheduler,
    //     workload: &'a Database,
    //     tx: mpsc::Sender<LocalStatistics>,
    // ) -> Worker {
    //     let timeout = config.get_int("timeout").unwrap() as u64;
    //     let p = config.get_str("protocol").unwrap();
    //     let w = config.get_str("workload").unwrap();
    //     let max_transactions = config.get_int("transactions").unwrap() as u32;
    //     let record = config.get_bool("record").unwrap();
    //     let log_results = config.get_bool("log_results").unwrap();

    //     let mut stats = LocalStatistics::new(id as u32, &w, &p);

    //     let builder = thread::Builder::new().name(id.to_string());

    // let mut wm = WaitManager::new();

    // let thread = builder
    //     .spawn(move || {
    //         if let Some(core_id) = core_id {
    //             core_affinity::set_for_current(core_id); // pin thread to cpu core
    //         }
    //         let mut generator = helper::get_transaction_generator(config); // initialise transaction generator

    //         // create results file -- dir created by this point
    //         let mut fh;
    //         if log_results {
    //             let workload = config.get_str("workload").unwrap();
    //             let protocol = config.get_str("protocol").unwrap();

    //             let dir;
    //             let file;
    //             if workload.as_str() == "acid" {
    //                 let anomaly = config.get_str("anomaly").unwrap();
    //                 dir = format!("./log/{}/{}/{}/", workload, protocol, anomaly);
    //                 file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, id);
    //             } else {
    //                 dir = format!("./log/{}/{}/", workload, protocol);
    //                 file = format!("./log/{}/{}/thread-{}.json", workload, protocol, id);
    //             }

    //             if Path::new(&dir).exists() {
    //                 if Path::new(&file).exists() {
    //                     fs::remove_file(&file).unwrap(); // remove file if already exists
    //                 }
    //             } else {
    //                 panic!("dir should exist");
    //             }

    //             fh = Some(
    //                 OpenOptions::new()
    //                     .write(true)
    //                     .append(true)
    //                     .create(true)
    //                     .open(&file)
    //                     .expect("cannot open file"),
    //             );
    //         } else {
    //             fh = None;
    //         }

    //         let mut completed = 0;

    //         let timeout_start = Instant::now(); // timeout
    //         let runtime = Duration::new(timeout * 60, 0);
    //         let timeout_end = timeout_start + runtime;

    //         let start_worker = Instant::now();

    //         loop {
    //             if completed == max_transactions {
    //                 tracing::debug!(
    //                     "All transactions sent: {} = {}",
    //                     completed,
    //                     max_transactions
    //                 );
    //                 break;
    //             } else if Instant::now() > timeout_end {
    //                 tracing::info!("Timeout reached: {} minute(s)", timeout);
    //                 break;
    //             } else {
    //                 let txn = generator.get_next(); // generate txn

    //                 let mut restart = true;

    //                 let start_latency = Instant::now();

    //                 while restart {
    //                     let ir = helper::execute(txn.clone(), scheduler, workload); // execute txn

    //                     let InternalResponse {
    //                         transaction,
    //                         outcome,
    //                         ..
    //                     } = ir;

    //                     match outcome {
    //                         Outcome::Committed { .. } => {
    //                             restart = false;
    //                             stats.record(transaction, outcome.clone(), restart);
    //                             wm.reset();
    //                             if log_results {
    //                                 log_result(&mut fh, outcome.clone());
    //                             }
    //                             debug!("complete: committed");
    //                         }
    //                         Outcome::Aborted { ref reason } => {
    //                             if let NonFatalError::SmallBankError(_) = reason {
    //                                 restart = false;
    //                                 wm.reset();
    //                                 stats.record(transaction, outcome.clone(), restart);
    //                                 if log_results {
    //                                     log_result(&mut fh, outcome.clone());
    //                                 }
    //                                 debug!("complete: aborted");
    //                             } else {
    //                                 restart = true; // protocol abort
    //                                 debug!("restart: {}", reason);
    //                                 stats.record(transaction, outcome.clone(), restart);
    //                                 let start_wm = Instant::now();
    //                                 wm.wait();
    //                                 stats.stop_wait_manager(start_wm);
    //                             }
    //                         }
    //                     }
    //                 }

    //                 stats.stop_latency(start_latency);
    //                 completed += 1;
    //             }
    //         }

    //         stats.stop_worker(start_worker);

    //         if record {
    //             tx.send(stats).unwrap();
    //         }
    //     })
    //     .unwrap();

    // Worker {
    //     id,
    //     thread: Some(thread),
    // }
    //   }
}

impl Recon {
    /// Create a new thread for ACID post-execution recon queries.
    pub fn new(
        config: Arc<Config>,
        _scheduler: Arc<Scheduler>,
        tx: mpsc::Sender<LocalStatistics>,
    ) -> Recon {
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let anomaly = config.get_str("anomaly").unwrap();
        let _sf = config.get_int("scale_factor").unwrap() as u64;

        // let persons = *ACID_SF_MAP.get(&sf).unwrap();
        let stats = LocalStatistics::new(0, &workload, &protocol);

        let builder = thread::Builder::new().name("0".to_string()); // fix id - only thread running

        let thread = builder
            .spawn(move || {
                // create results file -- dir created by this point
                let dir = format!("./log/{}/{}/{}/", workload, protocol, anomaly);
                let file = format!("./log/acid/{}/{}/thread-recon.json", protocol, anomaly);

                if Path::new(&dir).exists() {
                    if Path::new(&file).exists() {
                        fs::remove_file(&file).unwrap(); // remove file if already exists
                    }
                } else {
                    panic!("dir: {} should exist", dir);
                }

                // let fh = Some(
                //     OpenOptions::new()
                //         .write(true)
                //         .append(true)
                //         .create(true)
                //         .open(&file)
                //         .expect("cannot open file"),
                // );

                // match anomaly.as_str() {
                //     "g0" => {
                //         log::info!("Executing {} recon queries", anomaly);

                //         for p1_id in (0..persons).step_by(2) {
                //             let p2_id = p1_id + 1;
                //             let payload = G0Read { p1_id, p2_id };

                //             let txn = Message::Request {
                //                 request_no: 0, // TODO
                //                 transaction: Transaction::Acid(AcidTransaction::G0Read),
                //                 parameters: Parameters::Acid(AcidTransactionProfile::G0Read(
                //                     payload,
                //                 )),
                //             };

                //             let ir = helper::execute(txn, Arc::clone(&scheduler)); // execute txn
                //             let InternalResponse {
                //                 transaction,
                //                 outcome,
                //                 latency,
                //                 ..
                //             } = ir;

                //             log_result(&mut fh, outcome.clone()); // log result
                //             stats.record(transaction, outcome.clone(), latency);
                //             // record txn
                //         }
                //     }
                //     "lu" => {
                //         log::info!("Executing {} recon queries", anomaly);

                //         for p_id in 0..persons {
                //             let payload = LostUpdateRead { p_id };

                //             let txn = Message::Request {
                //                 request_no: 0, // TODO
                //                 transaction: Transaction::Acid(AcidTransaction::LostUpdateRead),
                //                 parameters: Parameters::Acid(
                //                     AcidTransactionProfile::LostUpdateRead(payload),
                //                 ),
                //             };

                //             let ir = helper::execute(txn, Arc::clone(&scheduler)); // execute txn
                //             let InternalResponse {
                //                 transaction,
                //                 outcome,
                //                 latency,
                //                 ..
                //             } = ir;
                //             log_result(&mut fh, outcome.clone()); // log result
                //             stats.record(transaction, outcome.clone(), latency);
                //         }
                //     }
                //     "g2item" => {
                //         log::info!("Executing {} recon queries", anomaly);

                //         let p = persons * 4;
                //         for p_id in (0..p).step_by(2) {
                //             let payload = G2itemRead {
                //                 p1_id: p_id,
                //                 p2_id: p_id + 1,
                //             };

                //             let txn = Message::Request {
                //                 request_no: 0, // TODO
                //                 transaction: Transaction::Acid(AcidTransaction::G2itemRead),
                //                 parameters: Parameters::Acid(AcidTransactionProfile::G2itemRead(
                //                     payload,
                //                 )),
                //             };

                //             let ir = helper::execute(txn, Arc::clone(&scheduler)); // execute txn
                //             let InternalResponse {
                //                 transaction,
                //                 outcome,
                //                 latency,
                //                 ..
                //             } = ir;
                //             log_result(&mut fh, outcome.clone()); // log result
                //             stats.record(transaction, outcome.clone(), latency);
                //             // record txn
                //         }
                //     }
                //     _ => log::info!("No recon queries for {}", anomaly),
                // }

                tx.send(stats).unwrap();
            })
            .unwrap();

        Recon {
            thread: Some(thread),
        }
    }
}

pub fn log_result(fh: &mut Option<std::fs::File>, outcome: Outcome) {
    if let Some(ref mut fh) = fh {
        match outcome {
            Outcome::Committed { value } => {
                writeln!(fh, "{}", &value.unwrap()).unwrap();
            }
            Outcome::Aborted { reason } => {
                let x = format!("{}", reason);
                let value = serde_json::to_string(&x).unwrap();
                writeln!(fh, "{}", &value).unwrap();
            }
        }
    }
}
