use crate::common::connection::WriteConnection;
use crate::common::error::SpaghettiError;
use crate::common::message::Message;
use crate::Result;

use tokio::io::AsyncWrite;
use tracing::{debug, info};

/// Manages the write half of the `client` TCP stream.
pub struct WriteHandler<W: AsyncWrite + Unpin> {
    /// Write half of TCP stream wrapped with buffers.
    pub connection: WriteConnection<W>,

    /// Number of transaction write to the tcp stream.
    pub sent: u32,

    /// `Message` channel from `Producer`.
    pub write_task_rx: tokio::sync::mpsc::Receiver<Message>,
}

impl<W: AsyncWrite + Unpin> WriteHandler<W> {
    /// Create new `WriteHandler`.
    pub fn new(
        connection: WriteConnection<W>,
        write_task_rx: tokio::sync::mpsc::Receiver<Message>,
    ) -> WriteHandler<W> {
        WriteHandler {
            connection,
            write_task_rx,
            sent: 0,
        }
    }
}

impl<W: AsyncWrite + Unpin> Drop for WriteHandler<W> {
    fn drop(&mut self) {
        debug!("Drop write handler");
        info!("Sent {} transactions to server", self.sent);
    }
}

/// Run `WriteHandler`.
///
/// # Errors
///
/// - Returns `ConnectionUnexpectedlyClosed` if the TCP stream has been unexpectedly
/// closed.
pub async fn run<R: AsyncWrite + Unpin + Send + 'static>(mut wh: WriteHandler<R>) -> Result<()> {
    // Spawn tokio task.
    let handle = tokio::spawn(async move {
        // Process messages until the channel is closed.
        while let Some(message) = wh.write_task_rx.recv().await {
            // Convert to frame.
            let frame = message.into_frame();
            // Write to socket.
            let res = wh.connection.write_frame(&frame).await;
            // Increment messages sent.
            wh.sent += 1;
            // Check if error with tcp stream.
            match res {
                Ok(_) => {
                    // Do nothing
                    if let Message::CloseConnection = message {
                        debug!("Write handler has recevied close connection message");
                        info!("Close connection message sent");
                        // Do not count this message.
                        wh.sent -= 1;
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                Err(_) => {
                    // Close receiver end from producer.
                    debug!("Error with stream, close channel to producer");
                    wh.write_task_rx.close();
                    return Err(SpaghettiError::ConnectionUnexpectedlyClosed.into());
                }
            }
        }
        Ok(())
    });

    // Await on task.
    handle.await?
}
