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

    /// Active connections.
    pub active_connections: u32,

    /// Sender of channel between `Listener` and `ReadHandler`s.
    /// Used for sending notification of shutdown triggered by keyboard interupt.
    pub notify_read_handlers_tx: tokio::sync::broadcast::Sender<()>,

    /// Sender of channel between `ReadHandler` and `TransactionManager`.
    /// Used to shutdown transaction manager when there are no active connections.
    pub notify_tm_tx: std::sync::mpsc::Sender<()>,

    /// Receiver of channel between `TransactionManager` and `WriteHandler`.
    /// Used to indicate shutdown of transaction manager has complleted when there are
    /// no active connections.
    pub wh_shutdown_rx: tokio::sync::broadcast::Receiver<()>,

    /// Sender of channel between `WriteHandler`s and `Listener`.
    /// Used to indicate shutdown of write handlers when there are no active connections.
    pub notify_listener_tx: tokio::sync::mpsc::UnboundedSender<()>,

    /// Receiver of channel between `WriteHandler`s and `Listener`.
    /// Used to indicate `WriteHandler`s have gracefully shutdown.
    pub listener_shutdown_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
}

impl Listener {
    /// Runs the listener component of the server.
    ///
    /// Initialise the workload and populates tables and indexes.
    /// Listens for inbound connections.
    pub async fn run(
        &mut self,
        work_tx: std::sync::mpsc::Sender<Request>,
        notify_wh_tx: tokio::sync::broadcast::Sender<()>,
        config: Arc<Config>,
    ) -> Result<()> {
        info!("Accepting new connections");
        loop {
            // Accept new incoming connection from tcp listener.
            let (socket, _) = self.listener.accept().await?;
            info!("New connection accepted");
            // Increment active connections.
            self.active_connections += 1;

            info!("Active connections: {}", self.active_connections);

            // Response routing channel between `TransactionManager`s `Worker` and the
            // corresponding 'WriteHandler`.
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
                shutdown: Shutdown::new_broadcast(self.notify_read_handlers_tx.subscribe()),
                response_tx,
                notify_wh_requests: Some(sender),
                work_tx: work_tx.clone(),
                _notify_tm_tx: self.notify_tm_tx.clone(),
            };

            let mut write_handler = WriteHandler {
                connection: WriteConnection::new(wr),
                response_rx,
                responses_sent: 0,
                expected_responses_sent: None,
                listen_rh_requests: receiver,
                shutdown: Shutdown::new_broadcast(notify_wh_tx.subscribe()),
                _notify_listener_tx: self.notify_listener_tx.clone(),
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
