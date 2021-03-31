use crate::common::message::{InternalResponse, Message, Parameters, Transaction};
use crate::common::parameter_generation::ParameterGenerator;
use crate::workloads::tatp::paramgen::TatpGenerator;

use config::Config;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::thread;
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

        let mut generator = match config.get_str("workload").unwrap().as_str() {
            "tatp" => {
                let subscribers = config.get_int("subscribers").unwrap();
                let gen = TatpGenerator::new(subscribers as u64, false);
                ParameterGenerator::Tatp(gen)
            }
            _ => unimplemented!(),
        };

        let mut sent = 0;
        while let Ok(()) = self.next_rx.recv() {
            if sent == max_transactions {
                info!("All transactions sent: {} = {}", sent, max_transactions);
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
