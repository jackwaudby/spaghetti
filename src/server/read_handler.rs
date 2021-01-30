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
    /// Read half of tcp stream with buffers and frame parsing utils.
    pub connection: ReadConnection<R>,

    /// Requests received.
    pub requests: u32,

    /// Listen for server shutdown notifications.
    pub shutdown: Shutdown<tokio::sync::broadcast::Receiver<()>>,

    /// Channel for sending response from the trasaction manager.
    /// Each new request gets a clone of it.
    pub response_tx: tokio::sync::mpsc::UnboundedSender<Message>,

    // Channel receives the requests the read has received.
    pub notify_wh_requests: Option<tokio::sync::oneshot::Sender<u32>>,

    /// Channnel for sending transaction request to the transaction manager.
    /// Communication channel between async and sync code.
    pub work_tx: std::sync::mpsc::Sender<Request>,

    /// Channel for notify the transaction manager of shutdown.
    /// Implicitly dropped when handler is dropped (safely finished).
    /// Communication channel between async and sync code.
    pub _notify_tm_tx: std::sync::mpsc::Sender<()>,
}

impl<R: AsyncRead + Unpin> Drop for ReadHandler<R> {
    fn drop(&mut self) {
        debug!("Drop read handler");
    }
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    /// Process a single connection.
    ///
    /// Frames are requested from the socket and then processed.
    /// Responses are written back to the socket.
    pub async fn run(&mut self) -> Result<()> {
        debug!("Processing connection");
        // While no keyboard interupt try to read frames.
        while !self.shutdown.is_shutdown() {
            // While reading a requested frame listen for keyboard interrupt.
            debug!("Attempting to read frames");
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // Shutdown signal received, terminate the task.
                    debug!("Send expected responses: {:?}", self.requests);
                    self.notify_wh_requests
                        .take()
                        .unwrap()
                        .send(self.requests)
                        .unwrap();
                    return Ok(());
                }
            };

            // If `None` returned then the socket has been closed, terminate task.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => {
                    debug!("Read half socket closed");
                    debug!("Send expected responses: {:?}", self.requests);
                    self.notify_wh_requests
                        .take()
                        .unwrap()
                        .send(self.requests)
                        .unwrap();
                    return Ok(());
                }
            };

            let decoded: Message = bincode::deserialize(&frame.get_payload())?;
            let request = match decoded {
                Message::TatpTransaction(transaction) => {
                    debug!("Received Transaction");
                    // Wrap as request.
                    Request {
                        transaction: Transaction::Tatp(transaction),
                        id: None,
                        timestamp: None,
                        response_sender: self.response_tx.clone(),
                    }
                }
                Message::TpccTransaction(transaction) => {
                    // Wrap as request.
                    Request {
                        transaction: Transaction::Tpcc(transaction),
                        id: None,
                        timestamp: None,
                        response_sender: self.response_tx.clone(),
                    }
                }
                Message::CloseConnection => {
                    debug!("Received CloseConnection");
                    debug!("Send expected responses: {:?}", self.requests);
                    self.notify_wh_requests
                        .take()
                        .unwrap()
                        .send(self.requests)
                        .unwrap();
                    return Ok(());
                }
                _ => return Err(Box::new(SpaghettiError::UnexpectedMessage)),
            };
            // Increment transaction requests received.
            self.requests = self.requests + 1;
            debug!("Send transaction to transaction manager");
            // Send to transaction manager
            self.work_tx.send(request).unwrap();
        }
        Ok(())
    }
}
