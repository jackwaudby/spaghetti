use crate::common::message::InternalResponse;

use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;

use tracing::info;

pub struct Logger {
    /// Channel to send response to the logger.
    logger_rx: Receiver<InternalResponse>,

    /// Channel to  main to the logger.
    _main_tx: SyncSender<()>,
}

impl Logger {
    /// Create a new `Logger`.
    pub fn new(logger_rx: Receiver<InternalResponse>, main_tx: SyncSender<()>) -> Logger {
        Logger {
            logger_rx,
            _main_tx: main_tx,
        }
    }

    /// Run logger.
    pub fn run(&mut self) {
        while let Ok(request) = self.logger_rx.recv() {
            info!("{:?}", request);
        }
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
