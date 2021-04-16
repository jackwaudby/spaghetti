use crate::common::message::{InternalResponse, Outcome};
use crate::common::statistics::LocalStatistics;
use crate::common::utils::BenchmarkPhase;

use config::Config;
use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
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
    pub fn run(&mut self, config: Arc<Config>) {
        let workload = config.get_str("workload").unwrap();
        let protocol = config.get_str("protocol").unwrap();
        let test = "g1a";
        let file = format!("./log/acid/{}/{}.json", protocol, test);
        let dir = format!("./log/acid/{}/", protocol);

        if workload.as_str() == "acid" {
            // if directory exists
            if Path::new(&dir).exists() {
                // remove file if already exists.
                if Path::new(&file).exists() {
                    fs::remove_file(&file).unwrap();
                }
            // else create directory
            } else {
                fs::create_dir_all(&dir).unwrap();
            }
        }

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
                    if workload.as_str() == "acid" {
                        if let Outcome::Committed { value } = outcome.clone() {
                            let mut fh = OpenOptions::new()
                                .write(true)
                                .append(true)
                                .create(true)
                                .open(&file)
                                .expect("cannot open file");
                            write!(fh, "{}\n", &value.unwrap()).unwrap();
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
pub fn run(mut logger: Logger, config: Arc<Config>) {
    thread::spawn(move || {
        info!("Start logger");
        logger.run(config);
        info!("Logger closing...");
    });
}
