use crate::common::connection::{ReadConnection, WriteConnection};
use crate::common::message::Message;
use crate::common::message::Request;
use crate::common::shutdown::Shutdown;
use crate::server::manager;
use crate::server::manager::TransactionManager;
use crate::server::read_handler::ReadHandler;
use crate::server::write_handler::WriteHandler;
use crate::Result;

use std::time::Duration;
use tokio::time::timeout;

use config::Config;
use core::fmt::Debug;
use std::sync::Arc;
use tokio::io;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Server listener state.
#[derive(Debug)]
pub struct Listener {
    /// TCP listener.
    pub listener: TcpListener,

    /// Connections IDs.
    pub next_id: u32,

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
    pub notify_listener_tx: tokio::sync::broadcast::Sender<u32>,

    /// Receiver of channel between `WriteHandler`s and `Listener`.
    /// Used to indicate `WriteHandler`s have gracefully shutdown.
    pub listener_shutdown_rx: tokio::sync::broadcast::Receiver<u32>,
}

impl Listener {
    /// Runs the listener component of the server.
    ///
    /// Initialise the workload and populates tables and indexes.
    /// Listens for inbound connections.1
    pub async fn run(
        &mut self,
        work_tx: std::sync::mpsc::Sender<Request>,
        notify_wh_tx: tokio::sync::broadcast::Sender<()>,
        config: Arc<Config>,
        tm: TransactionManager,
    ) -> Result<()> {
        // Start transaction manager.
        manager::run(tm);
        // Listen for connections.
        info!("Accepting new connections");
        loop {
            while let Ok(id) = self.listener_shutdown_rx.try_recv() {
                info!("Connection {} was closed", id);
                self.active_connections -= 1;
            }

            let to = config.get_int("timeout")?;

            // Start timer.
            match timeout(Duration::from_secs(to as u64), self.listener.accept()).await {
                Ok(res) => {
                    let (socket, _) = res?;
                    info!("New connection accepted");
                    self.active_connections += 1;
                    let (response_tx, response_rx): (
                        tokio::sync::mpsc::UnboundedSender<Message>,
                        tokio::sync::mpsc::UnboundedReceiver<Message>,
                    ) = tokio::sync::mpsc::unbounded_channel();
                    let (sender, receiver) = tokio::sync::oneshot::channel::<u32>();
                    let (rd, wr) = io::split(socket);
                    let mut read_handler = ReadHandler {
                        id: self.next_id,
                        connection: ReadConnection::new(rd),
                        requests: 0,
                        shutdown: Shutdown::new_broadcast(self.notify_read_handlers_tx.subscribe()),
                        response_tx,
                        notify_wh_requests: Some(sender),
                        work_tx: work_tx.clone(),
                        _notify_tm_tx: self.notify_tm_tx.clone(),
                    };
                    let mut write_handler = WriteHandler {
                        id: self.next_id,
                        connection: WriteConnection::new(wr),
                        response_rx,
                        responses_sent: 0,
                        expected_responses_sent: None,
                        listen_rh_requests: receiver,
                        shutdown: Shutdown::new_broadcast(notify_wh_tx.subscribe()),
                        notify_listener_tx: self.notify_listener_tx.clone(),
                    };
                    let cw = Arc::clone(&config);
                    tokio::spawn(async move {
                        if let Err(err) = read_handler.run().await {
                            error!("{:?}", err);
                        }
                    });
                    tokio::spawn(async move {
                        if let Err(err) = write_handler.run(cw).await {
                            error!("{:?}", err);
                        }
                    });
                    // Increment next id
                    self.next_id += 1;
                    info!("Active connections: {}", self.active_connections);
                }
                Err(_) => {
                    if self.active_connections == 0 {
                        info!("Server timed out");
                        break;
                    } else {
                        // Server still active
                        continue;
                    }
                }
            }
        }
        Ok(())
    }
}
