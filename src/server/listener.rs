use crate::common::connection::{ReadConnection, WriteConnection};
use crate::common::message::Message;
use crate::common::message::Request;
use crate::common::shutdown::Shutdown;
use crate::server::read_handler::ReadHandler;
use crate::server::write_handler::WriteHandler;
use crate::Result;

use config::Config;
use core::fmt::Debug;
use std::sync::Arc;
use tokio::io;
use tokio::net::TcpListener;
use tracing::info;

/// Server listener state.
#[derive(Debug)]
pub struct Listener {
    /// TCP listener.
    pub listener: TcpListener,

    /// Broadcast channel between the `Listener` and all active connections (`Handlers`).
    /// The `Listener' retains `Sender` and each `Handler` gets a `Receiver` handle.
    /// When the server is shutdown this `Sender` is explicitly dropped, closing the channel,
    /// and triggering the shutdown process.
    /// This is communication between async code (A -> A).
    pub notify_handlers_tx: tokio::sync::broadcast::Sender<()>,

    pub listener_rx: tokio::sync::mpsc::UnboundedReceiver<()>,

    // TEST
    pub rh_tx: std::sync::mpsc::Sender<()>,

    pub wh_rx: tokio::sync::broadcast::Receiver<()>,

    pub listener_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl Listener {
    /// Runs the listener component of the server.
    ///
    /// Initialise the workload and populates tables and indexes.
    /// Listens for inbound connections.
    pub async fn run(
        &mut self,
        notify_tm_tx: std::sync::mpsc::Sender<()>,
        work_tx: std::sync::mpsc::Sender<Request>,
        notify_wh_tx: tokio::sync::broadcast::Sender<()>,
        notify_listener_tx: tokio::sync::mpsc::UnboundedSender<()>,
        config: Arc<Config>,
    ) -> Result<()> {
        info!("Accepting new connections");
        loop {
            // Accept new incoming connection from tcp listener.
            let (socket, _) = self.listener.accept().await?;
            info!("New connection accepted");

            // For each `Handler` a `Response` channel is created.
            // This enables the `TransactionManager`s `Worker`s to send the response for a
            // request to the correct client.
            // A `Handler` wraps a transaction with a clone of the `Sender` to its response
            // channel into a `Request`.
            // Communication between sync code and async code (S -> A).
            // Use mpsc unbounded channel.
            let (response_tx, response_rx): (
                tokio::sync::mpsc::UnboundedSender<Message>,
                tokio::sync::mpsc::UnboundedReceiver<Message>,
            ) = tokio::sync::mpsc::unbounded_channel();

            let (sender, receiver) = tokio::sync::oneshot::channel::<u32>();

            // Create per-connection handler state.
            // Split socket into reader and writer handlers.
            let (rd, wr) = io::split(socket);

            let mut read_handler = ReadHandler {
                connection: ReadConnection::new(rd),
                requests: 0,
                shutdown: Shutdown::new_broadcast(self.notify_handlers_tx.subscribe()),
                response_tx,
                notify_wh_requests: Some(sender),
                work_tx: work_tx.clone(),
                _notify_tm_tx: notify_tm_tx.clone(),
            };

            let mut write_handler = WriteHandler {
                connection: WriteConnection::new(wr),
                response_rx,
                responses_sent: 0,
                expected_responses_sent: None,
                listen_rh_requests: receiver,
                shutdown: Shutdown::new_broadcast(notify_wh_tx.subscribe()),
                _notify_listener_tx: notify_listener_tx.clone(),
            };

            // Get handle to config.
            let cw = Arc::clone(&config);
            // Spawn new task to process the connection.
            tokio::spawn(async move {
                if let Err(err) = read_handler.run().await {
                    info!("{:?}", err);
                }
            });

            tokio::spawn(async move {
                if let Err(err) = write_handler.run(cw).await {
                    info!("{:?}", err);
                }
            });
        }
    }
}
