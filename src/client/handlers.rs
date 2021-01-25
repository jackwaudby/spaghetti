use crate::common::connection::{ReadConnection, WriteConnection};
use crate::common::message::{Message, Response};
use crate::common::shutdown::Shutdown;
use tokio::io::{AsyncRead, AsyncWrite};
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

/// Manages the read half of a tcp stream.
pub struct ReadHandler<R: AsyncRead + Unpin> {
    // Read half of tcp stream.
    pub connection: ReadConnection<R>,
    // `Message` channel to `Consumer`.
    pub read_task_tx: tokio::sync::mpsc::Sender<Response>,
    /// Notify `Consumer` of shutdown.
    _notify_c_tx: tokio::sync::mpsc::Sender<()>,
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    pub fn new(
        connection: ReadConnection<R>,
        read_task_tx: tokio::sync::mpsc::Sender<Response>,
        _notify_c_tx: tokio::sync::mpsc::Sender<()>,
    ) -> ReadHandler<R> {
        ReadHandler {
            connection,
            read_task_tx,
            _notify_c_tx,
        }
    }
}

impl<R: AsyncRead + Unpin> Drop for ReadHandler<R> {
    fn drop(&mut self) {
        info!("Drop ReadHandler");
    }
}
