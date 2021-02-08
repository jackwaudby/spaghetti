use crate::common::message::Message;
use crate::Result;

use std::fs::OpenOptions;
use std::io::prelude::*;

use tracing::{debug, info};

/// Receives server responses from the read handler and logs them.
pub struct Consumer {
    /// `Message` channel from `ReadHandler`.
    read_task_rx: tokio::sync::mpsc::Receiver<Message>,
    // Notify `Producer` of `Consumer`s shutdown.
    _notify_m_tx: tokio::sync::mpsc::Sender<()>,
}

impl Consumer {
    /// Create new `Consumer`.
    pub fn new(
        read_task_rx: tokio::sync::mpsc::Receiver<Message>,
        _notify_m_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Consumer {
        Consumer {
            read_task_rx,
            _notify_m_tx,
        }
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        debug!("Drop Consumer");
    }
}

// Run the consumer.
pub async fn run(mut consumer: Consumer) -> Result<()> {
    // Spawn tokio task.
    let handle = tokio::spawn(async move {
        // Process messages until the channel is closed.
        while let Some(message) = consumer.read_task_rx.recv().await {
            debug!("Received {:?}", message);
            // Append to file.
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open("result.txt")
                .expect("cannot open file");
            write!(file, "{}\n", message.to_string()).unwrap();
            if let Message::ConnectionClosed = message {
                info!("Connection closed");
                return Ok(());
            } else {
                continue;
            }
        }
        Ok(())
    });

    handle.await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::{Message, Response};
    use std::fs::File;
    use std::io::{self, BufRead};
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

        // `Consumer` to `Main`
        let (notify_m_tx, _) = tokio::sync::mpsc::channel(1);
        // `ReadHandler` to `Consumer`.
        let (read_task_tx, read_task_rx): (Sender<Message>, Receiver<Message>) =
            mpsc::channel(32 as usize);
        // Pre-populate response queue.
        let response = Response::Committed {
            value: Some(String::from("test")),
        };

        for i in 0..3 {
            let m = Message::Response {
                request_no: i,
                resp: response.clone(),
            };
            read_task_tx.send(m).await.unwrap();
        }

        // Create consumer
        let consumer = Consumer::new(read_task_rx, notify_m_tx);

        // Spawn task
        let h = run(consumer);

        // Drop channel
        drop(read_task_tx);

        // Join task
        h.await.unwrap();

        let file = File::open("result.txt").unwrap();
        let count: Vec<_> = io::BufReader::new(file)
            .lines()
            .collect::<std::result::Result<_, _>>()
            .unwrap();

        assert_eq!(3, count.len());
    }
}
