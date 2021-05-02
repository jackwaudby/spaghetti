use crate::common::connection::ReadConnection;
use crate::common::error::FatalError;
use crate::common::message::{InternalRequest, InternalResponse, Message};
use crate::common::shutdown::Shutdown;
use crate::Result;

use std::fmt::Debug;
use std::marker::Unpin;
use tokio::io::AsyncRead;

/// Represents the handler for the read half of a client connection.
#[derive(Debug)]
pub struct ReadHandler<R: AsyncRead + Unpin> {
    /// State
    pub state: State,

    /// Connection ID
    pub id: u32,

    /// Read half of tcp stream with buffers and frame parsing utils.
    pub connection: ReadConnection<R>,

    /// Requests received.
    pub requests: u32,

    /// Listen for server shutdown notifications.
    pub shutdown: Shutdown<tokio::sync::broadcast::Receiver<()>>,

    /// Channel for sending response from the transaction manager.
    /// Each new request gets a clone of it.
    pub response_tx: tokio::sync::mpsc::UnboundedSender<InternalResponse>,

    /// Channel for notifying the write handler of the state the read handler terminated in.
    pub notify_wh_requests: Option<tokio::sync::oneshot::Sender<State>>,

    /// Channel for sending requests to the transaction manager.
    /// Communication channel between async and sync code.
    pub work_tx: std::sync::mpsc::Sender<InternalRequest>,

    /// Channel for notifying the transaction manager of shutdown.
    /// Implicitly dropped when read handler is dropped.
    /// Communication channel between async and sync code.
    pub _notify_tm_tx: std::sync::mpsc::Sender<()>,
}

/// Read handler state.
#[derive(Debug, Copy, Clone)]
pub enum State {
    /// Normal operation.
    Active,

    /// Graceful shutdown path: (i) server timed out, (ii) client sent close connection
    /// message, or (iii) keyboard interrupt.
    Requests(u32),

    /// A fatal error occurred.
    InternalError,
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    /// Run the read handler for a connection.
    ///
    /// Whilst listening for a graceful shutdown the read handler reads from the
    /// underlying socket. If a message is received it is decoded, coverted to a request, and
    /// forwarded to the transaction manager. It tracks the number of requests it has
    /// received.
    ///
    /// # Shutdown
    ///
    /// Graceful shutdown:
    /// - Keyboard interrupt; listener closes/drops the channel between itself and the read
    /// handler.
    /// - Server timeout; listener closes/drops the channel between itself and the read
    /// handler.
    /// - Close connected message received; client has finished sending requests.
    ///
    /// In each event the read handler sends the number of requests it has received to its
    /// corresponding write handler. Then the tokio task the read handler resides in
    /// terminates, dropping the channel to the transaction manager notifying it to shutdown.
    ///
    /// Internal errors:
    /// - Encoding error arising from attempting to read a frame from the socket.
    /// - Serialisation error arising from attempting to decoded a message.
    /// - Received an unexpected message.
    /// - The read socket unexpectedly closed.
    /// - Channel to transaction manager was unexpectedly closed.
    ///
    /// In each event the read handler notifies the write handler of an internal error,
    /// In this case there is no guarantee the client will receive a response to each request it
    /// has sent.
    pub async fn run(&mut self) -> Result<()> {
        while !self.shutdown.is_shutdown() {
            // Attempt to read from socket and listen for shutdown.
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    break;
                }
            };

            // Check if socket unexpectedly closed.
            let frame =
                maybe_frame.ok_or_else(|| Box::new(FatalError::ReadSocketUnexpectedlyClosed))?;

            // Decode message.
            let decoded: Message = bincode::deserialize(&frame.get_payload())?;

            // Convert to internal request for transaction manager.
            let request = match decoded {
                Message::Request {
                    request_no,
                    transaction,
                    parameters,
                } => InternalRequest {
                    request_no,
                    transaction,
                    parameters,
                    response_sender: self.response_tx.clone(),
                },

                Message::CloseConnection => break,
                _ => {
                    return Err(Box::new(FatalError::UnexpectedMessage));
                }
            };
            // Increment transaction requests received.
            self.requests += 1;

            // Attempt to send to transaction manager.
            if self.work_tx.send(request).is_err() {
                return Err(Box::new(FatalError::ThreadPoolClosed));
            };
        }

        Ok(())
    }
}
