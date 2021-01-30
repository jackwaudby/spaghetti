use crate::common::message::Message;
use crate::common::shutdown::Shutdown;

use std::fs::OpenOptions;
use std::io::prelude::*;

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
                        // TODO:5000 log to file.
                        let mut file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .append(true)
                            .open("result.txt")
                            .expect("cannot open file");
                        write!(file, "{}\n", message.to_string()).unwrap();
                    }
                    return;
                }
            };

            if let Some(message) = message {
                // TODO: log to file.amount
                let mut file = OpenOptions::new()
                    .write(true)
                    .append(true)
                    .create(true)
                    .open("result.txt")
                    .expect("cannot open file");
                write!(file, "{}\n", message.to_string()).unwrap();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::{Message, Response};
    use std::fs::File;
    use std::io::{self, BufRead};
    use std::path::Path;
    use std::sync::Once;
    use tokio::sync::mpsc::{self, Receiver, Sender};
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    static LOG: Once = Once::new();

    fn logging(on: bool) {
        if on {
            LOG.call_once(|| {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting default subscriber failed");
            });
        }
    }

    /// Unable to parse frame from underlying connection.
    #[tokio::test]
    async fn run_drain_test() {
        // Init logging.
        logging(false);
        // Delete file.
        std::fs::remove_file("result.txt").expect("could not remove file");

        // `ReadHandler` to `Consumer`
        let (notify_c_tx, listen_rh_rx) = tokio::sync::mpsc::channel(1);
        // `Consumer` to `Producer`
        let (notify_p_tx, _) = tokio::sync::mpsc::channel(1);
        // `ReadHandler` to `Consumer`.
        let (read_task_tx, mut read_task_rx): (Sender<Message>, Receiver<Message>) =
            mpsc::channel(32 as usize);
        // Pre-populate response queue.
        let response = Response::Committed {
            value: Some(String::from("test")),
        };
        for _ in 0..3 {
            let m = Message::Response(response.clone());
            read_task_tx.send(m).await;
        }

        // Create consumer
        let consumer = Consumer::new(read_task_rx, listen_rh_rx, notify_p_tx.clone());

        // Spawn task
        let h = tokio::spawn(async move { run(consumer).await });

        // Drop shutdown listener.
        drop(notify_c_tx);

        // join task
        h.await;

        let file = File::open("result.txt").unwrap();
        let count: Vec<_> = io::BufReader::new(file)
            .lines()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(3, count.len());
    }
}
