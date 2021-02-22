use crate::common::connection::WriteConnection;
use crate::common::message::Message;
use crate::common::shutdown::Shutdown;
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
    /// Write half of tcp stream with buffers.
    pub connection: WriteConnection<R>,

    // Channel receives responses from transaction manager workers.
    pub response_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,

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
    pub notify_listener_tx: tokio::sync::broadcast::Sender<()>,
    // tokio::sync::mpsc::UnboundedSender<()>,
}

impl<R: AsyncWrite + Unpin> WriteHandler<R> {
    pub async fn run(&mut self, _config: Arc<Config>) -> Result<()> {
        // Loop until server is interrupted.
        // while !self.shutdown.is_shutdown() {

        loop {
            match self.listen_rh_requests.try_recv() {
                Ok(requests) => {
                    debug!("Write handler entered shutdown mode");
                    // Set expected responses sent.
                    self.expected_responses_sent = Some(requests);
                    // Receive responses from transaction manager.
                    while self.responses_sent != self.expected_responses_sent.unwrap() {
                        let response = self.response_rx.recv().await;
                        // Increment sent.
                        self.responses_sent = self.responses_sent + 1;
                        match response {
                            Some(response) => {
                                debug!("AAA- Write handler sending response: {:?}", response);
                                let f = response.into_frame();
                                self.connection.write_frame(&f).await?;
                            }
                            None => {}
                        }
                    }
                    debug!("Expected responses sent");
                    // Send connected closed message.
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
                            // Increment responses sent.accept
                            self.responses_sent = self.responses_sent + 1;
                            debug!("NORMAL OP - write handler sending response: {:?}", response);
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

        self.notify_listener_tx.send(()).unwrap();
        debug!("Sent {} to client", self.responses_sent);
    }
}
