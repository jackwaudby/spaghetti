//! `Shutdown؀` is used by `Handler` to listen for the server shutdown signal.
//!
//! A shutdown is triggered by the server listener which owns the `Sender` half of a broadcast//! channel.

use tokio::sync::{broadcast, mpsc};
use tracing::info;

#[derive(Debug)]
pub struct Shutdown {
    // true if the shutdown notification has been received.
    shutdown: bool,
    // Reciever half of a broadcast channel used to listen for the notification.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    ///
    /// This is a wrapper around the channels recv() fn.
    pub async fn recv(&mut self) {
        // Check if already received
        if self.shutdown {
            return;
        }

        let _ = self.notify.recv().await;

        self.shutdown = true;
    }
}

#[derive(Debug)]
pub struct NotifyTransactionManager {
    pub sender: mpsc::Sender<()>,
}

impl Drop for NotifyTransactionManager {
    fn drop(&mut self) {
        info!("Handler sending shutdown notification to transaction manager");
    }
}
