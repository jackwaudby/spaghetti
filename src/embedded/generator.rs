use crate::common::message::{InternalResponse, Message, Parameters, Transaction};
use crate::common::parameter_generation::ParameterGenerator;
use crate::workloads::acid::paramgen::{AcidGenerator, AcidTransactionProfile, LostUpdateRead};
use crate::workloads::acid::{AcidTransaction, ACID_SF_MAP};
use crate::workloads::smallbank::paramgen::SmallBankGenerator;
use crate::workloads::tatp::paramgen::TatpGenerator;

use config::Config;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::info;

pub struct Generator {
    /// Channel to send request to manager.
    req_tx: SyncSender<InternalRequest>,

    /// Channel to send response to the logger.
    logger_tx: SyncSender<InternalResponse>,

    next_rx: Receiver<()>,
}

/// Sent from read handlers to the transaction manager.
///
/// Contains a chanel to route the response to the correct write handler.
#[derive(Debug)]
pub struct InternalRequest {
    /// Request number from this client.
    pub request_no: u32,

    /// Transaction type
    pub transaction: Transaction,

    /// Trasaction parameters.
    pub parameters: Parameters,

    /// Channel to route response to `Logger`.
    pub response_sender: SyncSender<InternalResponse>,
}

impl Generator {
    /// Create a new `Generator`.
    pub fn new(
        req_tx: SyncSender<InternalRequest>,
        logger_tx: SyncSender<InternalResponse>,
        next_rx: Receiver<()>,
    ) -> Generator {
        Generator {
            req_tx,
            logger_tx,
            next_rx,
        }
    }

    /// Run generator.
    pub fn run(&mut self, config: Arc<Config>) {
        let max_transactions = config.get_int("transactions").unwrap() as u32;
        let timeout = config.get_int("timeout").unwrap() as u64;
        let sf = config.get_int("scale_factor").unwrap() as u64;
        let set_seed = config.get_bool("set_seed").unwrap();
        let seed;
        if set_seed {
            let s = config.get_int("seed").unwrap() as u64;
            seed = Some(s);
        } else {
            seed = None;
        }
        let mut generator = match config.get_str("workload").unwrap().as_str() {
            "acid" => {
                let anomaly = config.get_str("anomaly").unwrap();
                let delay = config.get_int("delay").unwrap() as u64;
                let gen = AcidGenerator::new(sf, set_seed, seed, &anomaly, delay);
                ParameterGenerator::Acid(gen)
            }
            "tatp" => {
                let use_nurand = config.get_bool("nurand").unwrap();
                let gen = TatpGenerator::new(sf, set_seed, seed, use_nurand);
                ParameterGenerator::Tatp(gen)
            }
            "smallbank" => {
                let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
                let hotspot_use_fixed_size = config.get_bool("hotspot_use_fixed_size").unwrap();
                let gen = SmallBankGenerator::new(
                    sf,
                    set_seed,
                    seed,
                    use_balance_mix,
                    hotspot_use_fixed_size,
                );
                ParameterGenerator::SmallBank(gen)
            }
            _ => unimplemented!(),
        };

        let mut sent = 0;

        // Set timeout
        let st = Instant::now();
        let runtime = Duration::new(timeout * 60, 0);
        let et = st + runtime;

        while let Ok(()) = self.next_rx.recv() {
            if sent == max_transactions {
                info!("All transactions sent: {} = {}", sent, max_transactions);

                // ACID TEST ONLY;
                /// send message read lost update for each person
                let workload = config.get_str("workload").unwrap().as_str();
                let anomaly = config.get_str("anomaly").unwrap().as_str();
                if workload == "acid" && anomaly == "lu" {
                    let sf = config.get_int("scale_factor").unwrap() as u64;
                    let persons = *ACID_SF_MAP.get(&sf).unwrap();

                    for p_id in 0..persons {
                        let payload = LostUpdateRead { p_id };

                        let message = Message::Request {
                            request_no: generator.get_generated() + 1,
                            transaction: Transaction::Acid(AcidTransaction::LostUpdateRead),
                            parameters: Parameters::Acid(AcidTransactionProfile::LostUpdateRead(
                                payload,
                            )),
                        };

                        let request = match message {
                            Message::Request {
                                request_no,
                                transaction,
                                parameters,
                            } => InternalRequest {
                                request_no,
                                transaction,
                                parameters,
                                response_sender: self.logger_tx.clone(),
                            },
                            _ => unimplemented!(),
                        };

                        self.req_tx.send(request).unwrap();
                    }
                }
                break;
            } else if Instant::now() > et {
                info!("Timeout reached: {} minute(s)", timeout);
                break;
            } else {
                let message = generator.get_next();
                let request = match message {
                    Message::Request {
                        request_no,
                        transaction,
                        parameters,
                    } => InternalRequest {
                        request_no,
                        transaction,
                        parameters,
                        response_sender: self.logger_tx.clone(),
                    },
                    _ => unimplemented!(),
                };

                self.req_tx.send(request).unwrap();
                sent += 1;
            }
        }
    }
}

/// Run logger a thread.
pub fn run(mut generator: Generator, config: Arc<Config>) {
    thread::spawn(move || {
        info!("Start generating transactions");
        generator.run(config);
        info!("Stop generating transactions");
    });
}
