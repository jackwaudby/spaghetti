use crate::common::message::{InternalResponse, Message, Parameters, Transaction};
use crate::common::parameter_generation::ParameterGenerator;
use crate::workloads::tatp::generator::TatpGenerator;

use config::Config;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tracing::info;

pub struct Generator {
    /// Channel to send request to manager.
    req_tx: SyncSender<InternalRequest>,

    /// Channel to send response to the logger.
    logger_tx: SyncSender<InternalResponse>,
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
    ) -> Generator {
        Generator { req_tx, logger_tx }
    }

    /// Run logger.
    pub fn run(&mut self, config: Arc<Config>) {
        let transactions = 10;
        let mut generator = match config.get_str("workload").unwrap().as_str() {
            "tatp" => {
                let subscribers = 1;
                let gen = TatpGenerator::new(subscribers as u64, false);
                ParameterGenerator::Tatp(gen)
            }
            _ => unimplemented!(),
        };
        let now = Instant::now();

        for _ in 0..transactions {
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
        }
        info!("Generator finished in {} secs", now.elapsed().as_secs());
    }
}

/// Run logger a thread.
pub fn run(mut generator: Generator, config: Arc<Config>) {
    thread::spawn(move || {
        info!("Start generating transactions");
        generator.run(config);
    });
}
