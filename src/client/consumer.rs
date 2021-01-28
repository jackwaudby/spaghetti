use crate::common::message::Message;
use crate::common::shutdown::Shutdown;

use tracing::{debug, info};

/// Receives server responses from the read handler and logs them.
pub struct Consumer {
    /// `Message` channel from `ReadHandler`.x
    read_task_rx: tokio::sync::mpsc::Receiver<Message>,
    /// Listen for `ReadHandler` to close channel (shutdown procedure).
    listen_rh_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
    // Notify `Producer` of `Consumer`s shutdown.
    _notify_p_tx: tokio::sync::mpsc::Sender<()>,
}

impl Consumer {
    /// Create new `Consumer`.
    pub fn new(
        read_task_rx: tokio::sync::mpsc::Receiver<Message>,
        listen_rh_rx: tokio::sync::mpsc::Receiver<()>,
        _notify_p_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Consumer {
        let listen_rh_rx = Shutdown::new_mpsc(listen_rh_rx);
        Consumer {
            read_task_rx,
            listen_rh_rx,
            _notify_p_tx,
        }
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        debug!("Drop Consumer");
    }
}

// Run the consumer.
pub async fn run(mut consumer: Consumer) {
    tokio::spawn(async move {
        // Attempt to read responses until shutdown.
        while !consumer.listen_rh_rx.is_shutdown() {
            // Concurrently listen for shutdown and receive responds.
            let message = tokio::select! {
                res = consumer.read_task_rx.recv() => res,
                _ = consumer.listen_rh_rx.recv() => {
                    // Shutdown signal received, terminate the task.
                    // Drain outstanding messages
                    while let Some(message) = consumer.read_task_rx.recv().await {
                        info!("Received {:?}", message);
                        // TODO: log to file.
                    }
                    return;
                }
            };

            if let Some(message) = message {
                info!("Received {:?}", message);
                // TODO: log to file.
            }
        }
    });
}
