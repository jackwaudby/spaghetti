use crate::common::connection::{ReadConnection, WriteConnection};
use crate::common::error::SpaghettiError;
use crate::common::message::Message;
use crate::common::shutdown::Shutdown;
use crate::workloads::tatp::TatpTransaction;
use crate::workloads::tpcc::TpccTransaction;
use crate::Result;

use config::Config;
use std::fmt::Debug;
use std::marker::Unpin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
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
                let closed = Message::ConnectionClosed;
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
