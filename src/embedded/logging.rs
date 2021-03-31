use crate::common::message::InternalResponse;
use crate::server::statistics::LocalStatistics;

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
}

impl Logger {
    /// Create a new `Logger`.
    pub fn new(
        logger_rx: Receiver<InternalResponse>,
        main_tx: SyncSender<LocalStatistics>,
        stats: Option<LocalStatistics>,
    ) -> Logger {
        Logger {
            logger_rx,
            main_tx,
            stats,
        }
    }

    /// Run logger.
    pub fn run(&mut self) {
        while let Ok(response) = self.logger_rx.recv() {
            let InternalResponse {
                transaction,
                outcome,
                latency,
                ..
            } = response;

            // Call record form stats.
            self.stats
                .as_mut()
                .unwrap()
                .record(transaction, outcome.clone(), latency);
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
