use crate::common::connection::{ReadConnection, WriteConnection};
use crate::common::message::Sendable;
use crate::common::message::{CloseConnection, ConnectionClosed, Response};
use crate::common::shutdown::Shutdown;
use crate::workloads::{tatp, tpcc};
use crate::Result;

use config::Config;
use std::fmt::Debug;
use std::marker::Unpin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info};

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
    pub response_tx: tokio::sync::mpsc::UnboundedSender<Response>,

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
        info!("Drop read handler");
    }
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    /// Process a single connection.
    ///
    /// Frames are requested from the socket and then processed.
    /// Responses are written back to the socket.
    pub async fn run(&mut self, config: Arc<Config>) -> Result<()> {
        debug!("Processing connection");
        // While shutdown signal not received try to read frames.
        while !self.shutdown.is_shutdown() {
            // While reading a requested frame listen for shutdown.
            info!("Attempting to read frames");
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // Shutdown signal received, terminate the task.
                    return Ok(());
                }
            };

            // If `None` returned then the socket has been closed, terminate task.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => {
                    info!("Read half socket closed");
                    return Ok(());
                }
            };

            // Attempt to deserialise `Frame`.
            // If  `CloseConnection` then exit break
            let workload = config.get_str("workload")?;
            match workload.as_str() {
                "tatp" => {
                    let decoded: bincode::Result<tatp::TatpTransaction> =
                        bincode::deserialize(&frame.get_payload());
                    let transaction = match decoded {
                        Ok(transaction) => {
                            info!("Received {:?}", transaction);
                            transaction
                        }
                        Err(_) => {
                            // Received a closed connection message.
                            let closed: bincode::Result<CloseConnection> =
                                bincode::deserialize(&frame.get_payload());
                            match closed {
                                Ok(cc) => {
                                    info!("Received {:?}", cc);
                                    info!("Send expected responses: {:?}", self.requests);
                                }
                                Err(_) => panic!("Received unknown message"),
                            }
                            self.notify_wh_requests
                                .take()
                                .unwrap()
                                .send(self.requests)
                                .unwrap();
                            break;
                        }
                    };

                    // Increment transaction requests received.
                    self.requests = self.requests + 1;

                    info!("Send transaction to transaction manager");
                    // Wrap as request.
                    let request = Request {
                        transaction,
                        response_sender: self.response_tx.clone(),
                    };
                    // Send to transaction manager
                    self.work_tx.send(request).unwrap();
                }
                "tpcc" => {
                    // TODO
                    unimplemented!()
                }
                _ => unimplemented!(),
            };
        }
        Ok(())
    }
}

/// Represents the handler for the write half of a client connection.
#[derive(Debug)]
pub struct WriteHandler<R: AsyncWrite + Unpin> {
    /// Write half of tcp stream with buffers.
    pub connection: WriteConnection<R>,

    // Channel receives responses from transaction manager workers.
    pub response_rx: tokio::sync::mpsc::UnboundedReceiver<Response>,

    pub responses_sent: u32,

    pub expected_responses_sent: Option<u32>,

    // Channel receives the requests the read has received.
    pub listen_rh_requests: tokio::sync::oneshot::Receiver<u32>,

    // Listen for server shutdown notifications.
    pub shutdown: Shutdown<tokio::sync::broadcast::Receiver<()>>,
    // Channnel for sending transaction request to the transaction manager.
    // Communication channel between async and sync code.
    // pub notify_tm_job_tx: std::sync::mpsc::Sender<tatp::TatpTransaction>,

    // Channel for notify the transaction manager of shutdown.
    // Implicitly dropped when handler is dropped (safely finished).
    // Communication channel between async and sync code.
    pub _notify_listener_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl<R: AsyncWrite + Unpin> WriteHandler<R> {
    pub async fn run(&mut self, _config: Arc<Config>) -> Result<()> {
        while !self.shutdown.is_shutdown() {
            // If read handler has received a close connection request it sends the expected
            // number of transactions the write handler should send responses for.
            if let Ok(requests) = self.listen_rh_requests.try_recv() {
                info!("Write handler received expected responses: {:?}", requests);
                self.expected_responses_sent = Some(requests);
                let closed = ConnectionClosed;
                let c = closed.into_frame();
                info!("Send connection closed message");

                self.connection.write_frame(&c).await?;

                break;
            }

            // Receive responses from transaction manager until shutdown notification.
            let response = tokio::select! {
                res = self.response_rx.recv() => res,
                _ = self.shutdown.recv() => {
                    // Shutdown signal received, terminate the task.
                    //TODO: drain response queue here
                    return Ok(());
                }
            };

            // Increment responses sent.
            self.responses_sent = self.responses_sent + 1;

            match response {
                Some(response) => {
                    info!("Write handler sending response: {:?}", response);
                    let f = response.into_frame();

                    self.connection.write_frame(&f).await?;
                }
                None => {}
            }
        }

        // Keep sending responses until one sent for each request.
        // TODO: then send ClosedConnection message.
        while self.responses_sent != self.expected_responses_sent.unwrap() {
            let response = self.response_rx.recv().await;
            // increment sent
            self.responses_sent = self.responses_sent + 1;
            match response {
                Some(response) => {
                    info!("Write handler sending response: {:?}", response);
                    let f = response.into_frame();
                    self.connection.write_frame(&f).await?;
                }
                None => {}
            }
        }
        info!("Sent responses for each received request");
        Ok(())
    }
}

impl<R: AsyncWrite + Unpin> Drop for WriteHandler<R> {
    fn drop(&mut self) {
        info!("Drop write handler");
    }
}

#[derive(Debug)]
pub struct Request {
    pub transaction: tatp::TatpTransaction,
    pub response_sender: tokio::sync::mpsc::UnboundedSender<Response>,
}
