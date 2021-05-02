use crate::common::connection::WriteConnection;
use crate::common::message::{InternalResponse, Message};
use crate::common::shutdown::Shutdown;
use crate::common::statistics::LocalStatistics;
use crate::common::utils::BenchmarkPhase;
use crate::server::manager::State as TransactionManagerState;
use crate::server::read_handler::State as ReadHandlerState;
use crate::Result;

use config::Config;
use std::fmt::Debug;
use std::marker::Unpin;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tracing::{debug, info};

/// Represents the handler for the write half of a client connection.
#[derive(Debug)]
pub struct WriteHandler<R: AsyncWrite + Unpin> {
    /// Connection ID
    pub id: u32,

    /// Stats
    pub stats: Option<LocalStatistics>,

    /// Benchmark state
    pub benchmark_phase: BenchmarkPhase,

    /// Write half of tcp stream with buffers.
    pub connection: WriteConnection<R>,

    // Channel receives responses from transaction manager workers.
    pub response_rx: tokio::sync::mpsc::UnboundedReceiver<InternalResponse>,

    pub responses_sent: u32,

    pub expected_responses_sent: Option<u32>,

    // Channel receives the requests the read has received.
    pub listen_rh_requests: tokio::sync::oneshot::Receiver<ReadHandlerState>,

    // Listen for server shutdown notifications.
    pub shutdown: Shutdown<tokio::sync::broadcast::Receiver<TransactionManagerState>>,
    // Channnel for sending transaction request to the transaction manager.
    // Communication channel between async and sync code.

    // Channel for notify the transaction manager of shutdown.
    // Implicitly dropped when handler is dropped (safely finished).
    // Communication channel between async and sync code.
    pub notify_listener_tx: tokio::sync::broadcast::Sender<LocalStatistics>,
}

impl<R: AsyncWrite + Unpin> WriteHandler<R> {
    pub async fn run(&mut self, config: Arc<Config>) -> Result<()> {
        // Shutdown avenues:
        // (a) read handler sends requests or internal error
        // (b) tm closes channel.

        let warmup = config.get_int("warmup")? as u32;
        loop {
            match self.listen_rh_requests.try_recv() {
                // Read handler has terminated and sent message.
                Ok(message) => {
                    // If the message is the expected number of responses to send.
                    if let ReadHandlerState::Requests(requests) = message {
                        // Set expected responses to send.
                        self.expected_responses_sent = Some(requests);
                        // Get responses from manager until all sent.
                        while self.responses_sent != self.expected_responses_sent.unwrap() {
                            // Attempt to get internal response from manager.
                            let response = self.response_rx.recv().await;
                            match response {
                                // Message received.
                                Some(response) => {
                                    // Destructure of internal response.
                                    let InternalResponse {
                                        request_no,
                                        transaction,
                                        outcome,
                                        latency,
                                    } = response;

                                    // If in execution phase record statistics.
                                    if let BenchmarkPhase::Execution = self.benchmark_phase {
                                        // Call record form stats.
                                        self.stats.as_mut().unwrap().record(
                                            transaction,
                                            outcome.clone(),
                                            latency,
                                        );
                                    }

                                    // Inc. response sent.
                                    self.responses_sent += 1;

                                    // If warmup complete switch to execution phase.
                                    if self.responses_sent == warmup {
                                        self.benchmark_phase = BenchmarkPhase::Execution;
                                        info!("Client {} warmup phase complete", self.id);
                                    }

                                    // Convert to external response.
                                    let ex_response = Message::Response {
                                        request_no,
                                        outcome,
                                    };

                                    // Convert to frame and send to client.
                                    let f = ex_response.into_frame();
                                    self.connection.write_frame(&f).await?;
                                }
                                // Channel unexpectedly closed.
                                None => {
                                    debug!(
                                        "Channel closed before actual = expected: {} != {}",
                                        self.responses_sent,
                                        self.expected_responses_sent.unwrap()
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    // Shutdown the write handler and notify client.
                    let closed = Message::ConnectionClosed;
                    let c = closed.into_frame();
                    info!("Closed connection");
                    self.connection.write_frame(&c).await?;
                    return Ok(());
                }
                Err(_) => {
                    // Normal operation
                    let response = self.response_rx.recv().await;
                    if let Some(response) = response {
                        // Destructure of internal response.
                        let InternalResponse {
                            request_no,
                            transaction,
                            outcome,
                            latency,
                        } = response;

                        // If in execution phase record statistics.
                        if let BenchmarkPhase::Execution = self.benchmark_phase {
                            // Call record form stats.
                            self.stats.as_mut().unwrap().record(
                                transaction,
                                outcome.clone(),
                                latency,
                            );
                        }

                        // Inc. response sent.
                        self.responses_sent += 1;

                        // If warmup complete switch to execution phase.
                        if self.responses_sent == warmup {
                            self.benchmark_phase = BenchmarkPhase::Execution;
                            info!("Client {} warmup phase complete", self.id);
                        }

                        // Convert to external response.
                        let ex_response = Message::Response {
                            request_no,
                            outcome,
                        };

                        // Convert to frame and send to client.
                        let f = ex_response.into_frame();
                        self.connection.write_frame(&f).await?;
                    }
                }
            }
        }
    }
}

impl<R: AsyncWrite + Unpin> Drop for WriteHandler<R> {
    fn drop(&mut self) {
        debug!("Drop write handler");

        self.notify_listener_tx
            .send(self.stats.take().unwrap())
            .unwrap();

        debug!("Sent {} to client", self.responses_sent);
    }
}
