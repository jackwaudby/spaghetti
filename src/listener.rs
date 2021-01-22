use crate::connection::{ReadConnection, WriteConnection};
use crate::handler::{ReadHandler, WriteHandler};
use crate::handler::{Request, Response};
use crate::shutdown::Shutdown;
use crate::workloads::tatp;
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

    /// Broadcasts a shutdown signal to all active connections (`Handlers`).
    ///
    /// This is communication between async code.
    pub notify_handlers_tx: tokio::sync::broadcast::Sender<()>,

    /// This is dropped when the transaction manager has cleaned up.
    ///
    /// This is communication from sync code to async code.
    pub tm_listener_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
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
                tokio::sync::mpsc::UnboundedSender<Response>,
                tokio::sync::mpsc::UnboundedReceiver<Response>,
            ) = tokio::sync::mpsc::unbounded_channel();

            // Create per-connection handler state.
            // Split socket into reader and writer handlers.
            let (rd, wr) = io::split(socket);

            let mut read_handler = ReadHandler {
                connection: ReadConnection::new(rd),
                shutdown: Shutdown::new(self.notify_handlers_tx.subscribe()),
                response_tx,
                work_tx: work_tx.clone(),
                _notify_tm_tx: notify_tm_tx.clone(),
            };

            let mut write_handler = WriteHandler {
                connection: WriteConnection::new(wr),
                response_rx,
            };

            // Get handle to config.
            let cr = Arc::clone(&config);
            let cw = Arc::clone(&config);
            // Spawn new task to process the connection.
            tokio::spawn(async move {
                if let Err(err) = read_handler.run(cr).await {
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
