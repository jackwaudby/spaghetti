use crate::common::message::Message;
use crate::common::shutdown::Shutdown;
use crate::Result;

use tracing::info;

pub struct Consumer {
    // Channel from read handler.
    read_task_rx: tokio::sync::mpsc::Receiver<Message>,
    // Listen for shutdown from read handler.
    listen_rh_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
    // Notify producer of consumer's shutdown.
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

    pub async fn run(&mut self) -> Result<()> {
        while !self.listen_rh_rx.is_shutdown() {
            let message = tokio::select! {
                res = self.read_task_rx.recv() => res,
                _ = self.listen_rh_rx.recv() => {
                    // Shutdown signal received, terminate the task.
                    // Drain outstanding messages
                    while let Some(message) = self.read_task_rx.recv().await {
                        info!("Client received {:?} from server", message);
                        // TODO: log to file.
                    }
                    return Ok(());
                }
            };

            info!("Client received {:?} from server", message);
        }
        Ok(())
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        info!("Drop Consumer");
    }
}
