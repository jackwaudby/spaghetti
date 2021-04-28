use crate::common::message::{InternalResponse, Message};
use crate::common::message::{Parameters, Transaction};
use crate::common::statistics::LocalStatistics;
use crate::gpc::helper;
use crate::server::scheduler::Protocol;
use crate::workloads::acid::paramgen::{
    AcidTransactionProfile, G0Read, G2itemRead, LostUpdateRead,
};
use crate::workloads::acid::{AcidTransaction, ACID_SF_MAP};

use config::Config;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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
    pub fn new(
        id: usize,
        core_id: Option<core_affinity::CoreId>,
        config: Arc<Config>,
        scheduler: Arc<Protocol>,
        tx: mpsc::Sender<LocalStatistics>,
    ) -> Worker {
        let mut generator = helper::get_transaction_generator(Arc::clone(&config)); // initialise transaction generator
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
                        let ir = helper::execute(txn, Arc::clone(&scheduler)); // execute txn
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

impl Recon {
    /// Create a new thread for ACID post-execution recon queries.
    pub fn new(
        config: Arc<Config>,
        scheduler: Arc<Protocol>,
        tx: mpsc::Sender<LocalStatistics>,
    ) -> Recon {
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let anomaly = config.get_str("anomaly").unwrap();
        let mut stats = LocalStatistics::new(0, &workload, &protocol);
        let sf = config.get_int("scale_factor").unwrap() as u64;
        let persons = *ACID_SF_MAP.get(&sf).unwrap();

        let jh = thread::spawn(move || {
            match anomaly.as_str() {
                "g0" => {
                    for p1_id in 0..persons {
                        let p2_id = p1_id + 1;
                        let payload = G0Read { p1_id, p2_id };

                        let txn = Message::Request {
                            request_no: 0, // TODO
                            transaction: Transaction::Acid(AcidTransaction::G0Read),
                            parameters: Parameters::Acid(AcidTransactionProfile::G0Read(payload)),
                        };

                        let ir = helper::execute(txn, Arc::clone(&scheduler)); // execute txn
                        let InternalResponse {
                            transaction,
                            outcome,
                            latency,
                            ..
                        } = ir;
                        stats.record(transaction, outcome.clone(), latency); // record txn
                    }
                }
                "lu" => {
                    for p_id in 0..persons {
                        let payload = LostUpdateRead { p_id };

                        let txn = Message::Request {
                            request_no: 0, // TODO
                            transaction: Transaction::Acid(AcidTransaction::LostUpdateRead),
                            parameters: Parameters::Acid(AcidTransactionProfile::LostUpdateRead(
                                payload,
                            )),
                        };

                        let ir = helper::execute(txn, Arc::clone(&scheduler)); // execute txn
                        let InternalResponse {
                            transaction,
                            outcome,
                            latency,
                            ..
                        } = ir;
                        stats.record(transaction, outcome.clone(), latency); // record txn
                    }
                }
                "g2item" => {
                    let p = persons * 4;
                    for p_id in (0..p).step_by(2) {
                        let payload = G2itemRead {
                            p1_id: p_id,
                            p2_id: p_id + 1,
                        };

                        let txn = Message::Request {
                            request_no: 0, // TODO
                            transaction: Transaction::Acid(AcidTransaction::G2itemRead),
                            parameters: Parameters::Acid(AcidTransactionProfile::G2itemRead(
                                payload,
                            )),
                        };

                        let ir = helper::execute(txn, Arc::clone(&scheduler)); // execute txn
                        let InternalResponse {
                            transaction,
                            outcome,
                            latency,
                            ..
                        } = ir;
                        stats.record(transaction, outcome.clone(), latency); // record txn
                    }
                }
                _ => panic!("no anomaly"),
            }

            tx.send(stats).unwrap();
        });

        Recon { thread: Some(jh) }
    }
}
