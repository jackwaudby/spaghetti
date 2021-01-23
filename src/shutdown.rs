use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

/// `Shutdown؀` is used by `Handler` to listen for the server shutdown signal.
/// A `Listener` owns the `Sender` half of a broadcast channel.
/// The `Handler` is shutdown when the listener closes the `Sender` half.
#[derive(Debug)]
pub struct Shutdown {
    // Indicates if shutdown notification has been received from the `Listener`.
    shutdown: bool,
    // Reciever half of a broadcast channel between `Handler` and `Listener`.
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

        let r = self.notify.recv().await;
        // Only `RecvError::Closed` can be received on this channel.
        match r {
            Ok(()) => panic!("No message should be recieved on this channel"),
            Err(RecvError::Closed) => info!("No more active senders"),
            Err(RecvError::Lagged(_)) => panic!("No messages are sent so not receiver should lag"),
        }

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
