use crate::common::message::{InternalResponse, Outcome};
use crate::common::statistics::LocalStatistics;
use crate::common::utils::BenchmarkPhase;
use crate::server::storage::datatype::Response;

use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;

use tracing::info;

pub struct Logger {
    /// Channel to send response to the logger.
    logger_rx: Receiver<InternalResponse>,

    /// Channel to  main to the logger.
    main_tx: SyncSender<LocalStatistics>,

    /// Local stats
    stats: Option<LocalStatistics>,

    /// Benchmark phase
    phase: BenchmarkPhase,

    /// Required warmup
    warmup: u32,
}

impl Logger {
    /// Create a new `Logger`.
    pub fn new(
        logger_rx: Receiver<InternalResponse>,
        main_tx: SyncSender<LocalStatistics>,
        stats: Option<LocalStatistics>,
        warmup: u32,
    ) -> Logger {
        Logger {
            logger_rx,
            main_tx,
            stats,
            phase: BenchmarkPhase::Warmup,
            warmup,
        }
    }

    fn start_execution(&mut self) {
        self.phase = BenchmarkPhase::Execution;
    }

    /// Run logger.
    pub fn run(&mut self) {
        let mut completed = 0;

        while let Ok(response) = self.logger_rx.recv() {
            let InternalResponse {
                transaction,
                outcome,
                latency,
                ..
            } = response;

            match self.phase {
                BenchmarkPhase::Warmup => {
                    completed += 1;

                    if completed == self.warmup {
                        self.start_execution();
                    }
                }
                BenchmarkPhase::Execution => {
                    let acid = true;
                    if acid {
                        if let Outcome::Committed { value } = outcome.clone() {
                            let resp: Response = serde_json::from_str(&value.unwrap()).unwrap();
                            assert_eq!(resp.val.get("version").unwrap(), "1");
                        }
                    }

                    self.stats
                        .as_mut()
                        .unwrap()
                        .record(transaction, outcome.clone(), latency);
                }
            }
        }

        let stats = self.stats.take().unwrap();
        self.main_tx.send(stats).unwrap();
    }
}

/// Run logger a thread.
pub fn run(mut logger: Logger) {
    thread::spawn(move || {
        info!("Start logger");
        logger.run();
        info!("Logger closing...");
    });
}
