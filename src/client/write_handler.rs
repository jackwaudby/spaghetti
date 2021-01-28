use crate::common::connection::WriteConnection;
use crate::common::error::SpaghettiError;
use crate::common::message::Message;
use crate::common::shutdown::Shutdown;
use crate::Result;

use tokio::io::AsyncWrite;
use tracing::debug;

/// Manages the write half of the `client` TCP stream.
pub struct WriteHandler<W: AsyncWrite + Unpin> {
    /// Write half of TCP stream wrapped with buffers.
    pub connection: WriteConnection<W>,
    /// `Message` channel from `Producer`.
    pub write_task_rx: tokio::sync::mpsc::Receiver<Message>,
    /// Listen for `Producer` to close channel (shutdown procedure).
    pub listen_p_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
    /// Indicates the write handler has received a close connection message.
    pub close_connection_message_received: bool,
}

impl<W: AsyncWrite + Unpin> WriteHandler<W> {
    /// Create new `WriteHandler`.
    pub fn new(
        connection: WriteConnection<W>,
        write_task_rx: tokio::sync::mpsc::Receiver<Message>,
        listen_p_rx: tokio::sync::mpsc::Receiver<()>,
    ) -> WriteHandler<W> {
        let listen_p_rx = Shutdown::new_mpsc(listen_p_rx);
        WriteHandler {
            connection,
            write_task_rx,
            listen_p_rx,
            close_connection_message_received: false,
        }
    }
}

impl<W: AsyncWrite + Unpin> Drop for WriteHandler<W> {
    fn drop(&mut self) {
        debug!("Drop WriteHandler");
    }
}

/// Run `WriteHandler`.
///
/// # Errors
pub async fn run<R: AsyncWrite + Unpin + Send + 'static>(mut wh: WriteHandler<R>) -> Result<()> {
    // Spawn tokio task.
    let handle = tokio::spawn(async move {
        // While a keyboard triggered shutdown or a close connection message has not been
        // received.
        while !wh.listen_p_rx.is_shutdown() && !wh.close_connection_message_received {
            // Attempt to receive messages from producer.
            if let Some(message) = wh.write_task_rx.recv().await {
                debug!("Writing {:?} to socket", message);
                // Convert to frame.
                let frame = message.into_frame();
                // Write to socket.
                if let Err(_) = wh.connection.write_frame(&frame).await {
                    return Err(SpaghettiError::ConnectionUnexpectedlyClosed);
                }
                // If message is a close connection message then write to socket and drop.
                if let Message::CloseConnection = message {
                    debug!("Closing write handler");
                    wh.close_connection_message_received = true;
                }
            }
        }
        // If shutdown triggered by keyboard some message could still be the queue from the
        // producer, so drain outstanding messages.
        if !wh.close_connection_message_received {
            while let Some(message) = wh.write_task_rx.recv().await {
                debug!("Writing {:?} to socket", message);
                // Convert to frame.
                let frame = message.into_frame();
                // Write to socket.
                wh.connection.write_frame(&frame).await.unwrap();
            }
        }
        debug!("Write handler dropped");
        Ok(())
    });

    // Get value from future.
    let value = handle.await;
    // Return () or error.
    match value {
        Ok(res) => match res {
            Ok(_) => return Ok(()),
            Err(e) => return Err(Box::new(e)),
        },
        Err(e) => return Err(Box::new(e)),
    }
}
