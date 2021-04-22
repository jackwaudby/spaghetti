use crate::common::message::Message;
use crate::Result;

use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
use tracing::info;

/// Receives server responses from the read handler and logs them.
pub struct Consumer {
    /// `Message` channel from `ReadHandler`.
    read_task_rx: tokio::sync::mpsc::Receiver<Message>,
    // Notify `Producer` of `Consumer`s shutdown.
    _notify_m_tx: tokio::sync::mpsc::Sender<()>,
    /// Responses received.
    received: u32,
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
            received: 0,
        }
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        //     debug!("Drop Consumer");
    }
}

// Run the consumer.
pub async fn run(mut consumer: Consumer) -> Result<()> {
    // Spawn tokio task.
    let handle = tokio::spawn(async move {
        // Remove directory.
        if Path::new("./log").exists() {
            fs::remove_dir_all("./log").unwrap();
        }
        // Create directory
        fs::create_dir("./log").unwrap();

        // Process messages until the channel is closed.
        while let Some(message) = consumer.read_task_rx.recv().await {
            if consumer.received % 1000 == 0 {
                info!("Recevied: {}", consumer.received);
            }

            // Append to file.
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open("log/responses.txt")
                .expect("cannot open file");

            write!(file, "{}\n", message.to_string()).unwrap();

            if let Message::ConnectionClosed = message {
                info!("Connection closed");
                return Ok(());
            } else {
                consumer.received += 1;
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
    use crate::common::message::{Message, Outcome};
    use std::fs::File;
    use std::io::{self, BufRead};
    use test_env_log::test;
    use tokio::sync::mpsc::{self, Receiver, Sender};

    /// Unable to parse frame from underlying connection.
    #[test(tokio::test)]
    async fn run_drain_test() {
        // `Consumer` to `Main`
        let (notify_m_tx, _) = tokio::sync::mpsc::channel(1);
        // `ReadHandler` to `Consumer`.
        let (read_task_tx, read_task_rx): (Sender<Message>, Receiver<Message>) =
            mpsc::channel(32 as usize);
        // Pre-populate response queue.
        let response = Outcome::Committed {
            value: Some(String::from("test")),
        };

        for i in 0..3 {
            let m = Message::Response {
                request_no: i,
                outcome: response.clone(),
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

        // Create new file.
        let file = File::open("./log/responses.txt").unwrap();
        let count: Vec<_> = io::BufReader::new(file)
            .lines()
            .collect::<std::result::Result<_, _>>()
            .unwrap();

        assert_eq!(3, count.len());
    }
}
