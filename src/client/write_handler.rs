use crate::common::connection::WriteConnection;
use crate::common::message::Message;
use crate::common::shutdown::Shutdown;

use tokio::io::AsyncWrite;
use tracing::info;

/// Manages the write half of a tcp stream.
pub struct WriteHandler<W: AsyncWrite + Unpin> {
    // Write half of stream.
    pub connection: WriteConnection<W>,
    // `Message` channel from `Producer`.
    pub write_task_rx: tokio::sync::mpsc::Receiver<Message>,
    // Listen for `Producer` to close channel (shutdown procedure).
    pub listen_p_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
    pub close_sent: bool,
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
            close_sent: false,
        }
    }
}

impl<W: AsyncWrite + Unpin> Drop for WriteHandler<W> {
    fn drop(&mut self) {
        info!("Drop WriteHandler");
    }
}
