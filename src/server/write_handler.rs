use crate::common::connection::WriteConnection;
use crate::common::error::NonFatalError;
use crate::common::message::{Message, Response};
use crate::common::shutdown::Shutdown;
use crate::server::manager::State as TransactionManagerState;
use crate::server::read_handler::State as ReadHandlerState;
use crate::server::statistics::Statistics;
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
    pub stats: Option<Statistics>,

    /// Benchmark state
    pub benchmark_phase: BenchmarkPhase,

    /// Write half of tcp stream with buffers.
    pub connection: WriteConnection<R>,

    // Channel receives responses from transaction manager workers.
    pub response_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,

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
    pub notify_listener_tx: tokio::sync::broadcast::Sender<Statistics>,
}

#[derive(Debug)]
pub enum BenchmarkPhase {
    Warmup,
    Execution,
}

impl<R: AsyncWrite + Unpin> WriteHandler<R> {
    pub async fn run(&mut self, config: Arc<Config>) -> Result<()> {
        // Shutdown avenues:
        // (a) read handler sends requests or internal error
        // (b) tm closes channel.

        let warmup = config.get_int("warmup")? as u32;

        loop {
            // Check if read handler has sent message.
            match self.listen_rh_requests.try_recv() {
                Ok(message) => {
                    debug!("Write handler received {:?} from read handler", message);

                    if let ReadHandlerState::Requests(requests) = message {
                        // Set expected responses sent.
                        self.expected_responses_sent = Some(requests);
                        // Received until responses equal request.
                        while self.responses_sent != self.expected_responses_sent.unwrap() {
                            // Attempt to get message from channel
                            let response = self.response_rx.recv().await;
                            // Increment sent.
                            match response {
                                Some(response) => {
                                    // Update stats if execution phase
                                    if let BenchmarkPhase::Execution = self.benchmark_phase {
                                        if let Message::Response {
                                            ref resp, latency, ..
                                        } = response
                                        {
                                            match resp {
                                                Response::Committed { .. } => {
                                                    debug!("Increment committed");
                                                    self.stats.as_mut().unwrap().inc_committed();
                                                    let lat = latency.unwrap().as_nanos();
                                                    debug!("Increment latency {}", lat);
                                                    self.stats
                                                        .as_mut()
                                                        .unwrap()
                                                        .add_cum_latency(lat);
                                                }
                                                Response::Aborted { reason } => {
                                                    if let NonFatalError::RowNotFound(_, _) = reason
                                                    {
                                                        debug!("Increment committed");
                                                        self.stats
                                                            .as_mut()
                                                            .unwrap()
                                                            .inc_committed();
                                                        let lat = latency.unwrap().as_nanos();
                                                        debug!("Increment latency {}", lat);
                                                        self.stats
                                                            .as_mut()
                                                            .unwrap()
                                                            .add_cum_latency(lat);
                                                    } else {
                                                        self.stats.as_mut().unwrap().inc_aborted()
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    self.responses_sent = self.responses_sent + 1;
                                    if self.responses_sent == warmup {
                                        self.benchmark_phase = BenchmarkPhase::Execution;
                                        info!("Client {} warmup phase complete", self.id);
                                    }
                                    let f = response.into_frame();
                                    self.connection.write_frame(&f).await?;
                                }
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

                    let closed = Message::ConnectionClosed;
                    let c = closed.into_frame();
                    info!("Closed connection");
                    self.connection.write_frame(&c).await?;
                    return Ok(());
                }
                Err(_) => {
                    // Normal operation
                    let response = self.response_rx.recv().await;
                    match response {
                        Some(response) => {
                            // Update stats
                            if let BenchmarkPhase::Execution = self.benchmark_phase {
                                if let Message::Response {
                                    ref resp, latency, ..
                                } = response
                                {
                                    match resp {
                                        Response::Committed { .. } => {
                                            debug!("Increment committed");
                                            self.stats.as_mut().unwrap().inc_committed();
                                            let lat = latency.unwrap().as_nanos();
                                            debug!("Increment latency {}", lat);
                                            self.stats.as_mut().unwrap().add_cum_latency(lat);
                                        }
                                        Response::Aborted { reason } => {
                                            if let NonFatalError::RowNotFound(_, _) = reason {
                                                debug!("Increment committed");
                                                self.stats.as_mut().unwrap().inc_committed();
                                                let lat = latency.unwrap().as_nanos();
                                                debug!("Increment latency {}", lat);
                                                self.stats.as_mut().unwrap().add_cum_latency(lat);
                                            } else {
                                                self.stats.as_mut().unwrap().inc_aborted()
                                            }
                                        }
                                    }
                                }
                            }
                            // Increment responses sent.accept
                            self.responses_sent = self.responses_sent + 1;
                            if self.responses_sent == warmup {
                                self.benchmark_phase = BenchmarkPhase::Execution;
                                info!("Client {} warmup phase complete", self.id);
                            }
                            let f = response.into_frame();
                            self.connection.write_frame(&f).await?;
                        }
                        None => {}
                    }
                }
            }
        }
    }
}

impl<R: AsyncWrite + Unpin> Drop for WriteHandler<R> {
    fn drop(&mut self) {
        debug!("Drop write handler");

        let res = self.notify_listener_tx.send(self.stats.take().unwrap());
        match res {
            Ok(_) => {}
            Err(_) => {}
        }
        debug!("Sent {} to client", self.responses_sent);
    }
}
