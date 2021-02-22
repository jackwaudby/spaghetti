use crate::common::connection::ReadConnection;
use crate::common::error::SpaghettiError;
use crate::common::message::{Message, Request, Transaction};
use crate::common::shutdown::Shutdown;
use crate::Result;

use std::fmt::Debug;
use std::marker::Unpin;
use tokio::io::AsyncRead;
use tracing::debug;

/// Represents the handler for the read half of a client connection.
#[derive(Debug)]
pub struct ReadHandler<R: AsyncRead + Unpin> {
    /// Connection ID
    pub id: u32,

    /// Read half of tcp stream with buffers and frame parsing utils.
    pub connection: ReadConnection<R>,

    /// Requests received.
    pub requests: u32,

    /// Listen for server shutdown notifications.
    pub shutdown: Shutdown<tokio::sync::broadcast::Receiver<()>>,

    /// Channel for sending response from the trasaction manager.
    /// Each new request gets a clone of it.
    pub response_tx: tokio::sync::mpsc::UnboundedSender<Message>,

    /// Channel for sending write handler the number of responses it should send before
    /// terminating.
    pub notify_wh_requests: Option<tokio::sync::oneshot::Sender<u32>>,

    /// Channel for sending requests to the transaction manager.
    /// Communication channel between async and sync code.
    pub work_tx: std::sync::mpsc::Sender<Request>,

    /// Channel for notifying the transaction manager of shutdown.
    /// Implicitly dropped when read handler is dropped.
    /// Communication channel between async and sync code.
    pub _notify_tm_tx: std::sync::mpsc::Sender<()>,
}

impl<R: AsyncRead + Unpin> Drop for ReadHandler<R> {
    fn drop(&mut self) {
        debug!("Drop read handler");
        debug!("{}", self.requests);
    }
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    /// Run the read handler for a connection.
    ///
    /// # Shutdown
    ///
    /// - Keyboard interrupt; listener closes/drops the channel between itself and the read
    /// handler.
    /// - Server timeout; TODO.
    /// - Close connected message received; client has finished sending requests.
    ///
    /// In each event the read handler sends the number of requests it has received to its
    /// corresponding write handler. Then the tokio task the read handler resides in terminates,
    /// dropping the channel to the transaction manager notifying it to shutdown.
    ///
    /// # Errors
    ///
    /// - Encoding error arising from attempting to read a frame from the socket.
    /// - Serialisation error arising from attempting to decoded a message.
    /// - Received an unexpected message.
    /// - The socket unexpectedly closed.
    /// - Channel to write handler unexpectedly closed.
    ///
    /// In the event of an error the tokio task the read handler resides in terminates, dropping
    /// the channel to the transaction manager notifying it to shutdown.
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
                maybe_frame.ok_or(Box::new(SpaghettiError::ReadSocketUnexpectedlyClosed))?;

            // Decode message.
            let decoded: Message = bincode::deserialize(&frame.get_payload())?;
            // Convert to request for transaction manager.
            let request = match decoded {
                Message::TatpTransaction { request_no, params } => Request {
                    request_no,
                    transaction: Transaction::Tatp(params),
                    response_sender: self.response_tx.clone(),
                },

                Message::CloseConnection => break,
                _ => {
                    return Err(Box::new(SpaghettiError::UnexpectedMessage));
                }
            };
            // Increment transaction requests received.
            self.requests += 1;
            // Send to transaction manager
            self.work_tx.send(request).unwrap();
        }

        // Read handler has been shutdown.
        // Send requests received to write handler.
        self.notify_wh_requests
            .take()
            .unwrap()
            .send(self.requests)
            .map_err(|_| Box::new(SpaghettiError::WriteHandlerUnexpectedlyClosed))?;

        Ok(())
    }
}
