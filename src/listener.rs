use crate::connection::Connection;
use crate::shutdown::Shutdown;
use crate::workloads::{tatp, tpcc};
use crate::Result;

use config::Config;
use core::fmt::Debug;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, info};

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
    /// Runs the server.
    ///
    /// Initialise the workload and populates tables and indexes.
    /// Listens for inbound connections.
    pub async fn run(
        &mut self,
        notify_tm_tx: std::sync::mpsc::Sender<()>,
        notify_tm_job_tx: std::sync::mpsc::Sender<tatp::TatpTransaction>,
        config: Arc<Config>,
    ) -> Result<()> {
        info!("Accepting new connections");
        loop {
            // Accept new socket.
            let (socket, _) = self.listener.accept().await?;
            info!("New connection accepted");
            // Create per-connection handler state.

            let mut handler = Handler {
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_handlers_tx.subscribe()),
                notify_tm_job_tx: notify_tm_job_tx.clone(),
                _notify_tm_tx: notify_tm_tx.clone(),
            };

            // Get handle to config.
            let c = Arc::clone(&config);

            // Spawn new task to process the connection.
            tokio::spawn(async move {
                if let Err(err) = handler.run(c).await {
                    info!("{:?}", err);
                }
            });
        }
    }
}

/// Per-connection handler
#[derive(Debug)]
struct Handler {
    /// TCP connection: Tcp stream with buffers and frame parsing utils/
    connection: Connection,

    /// Listen for server shutdown notifications.
    shutdown: Shutdown,

    notify_tm_job_tx: std::sync::mpsc::Sender<tatp::TatpTransaction>,

    /// Notify transaction manager of shutdown.
    /// Implicitly dropped when handler is dropped (safely finished).
    ///
    /// This is communication from async to sync.
    _notify_tm_tx: std::sync::mpsc::Sender<()>,
}

// TODO: split into read and write channels.
impl Handler {
    /// Process a single connection.
    ///
    /// Frames are requested from the socket and then processed.
    /// Responses are written back to the socket.
    pub async fn run(&mut self, config: Arc<Config>) -> Result<()> {
        debug!("Processing connection");
        // While shutdown signal not received try to read frames.
        while !self.shutdown.is_shutdown() {
            // While reading a requested frame listen for shutdown.
            debug!("Attempting to read frames");
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // shutdown signal received, return from handler's run.
                    // this terminates the task
                    return Ok(());
                }
            };

            // If None is returned from maybe frame then the socket has been closed.
            // Terminate task
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // TODO: Deserialise based on workload.
            let w = config.get_str("workload").unwrap();
            // Deserialise to transaction.
            match w.as_str() {
                "tatp" => {
                    let decoded: tatp::TatpTransaction =
                        bincode::deserialize(&frame.get_payload()).unwrap();
                    info!("Received: {:?}", decoded);
                }
                "tpcc" => {
                    let decoded: tpcc::TpccTransaction =
                        bincode::deserialize(&frame.get_payload()).unwrap();
                    info!("Received: {:?}", decoded);
                }
                _ => unimplemented!(),
            };

            // TODO: fixed as GetSubscriberData transaction.
            let dat = tatp::GetSubscriberData { s_id: 0 };
            // let t = Box::new(tatp::TatpTransaction::GetSubscriberData(dat));
            let t = tatp::TatpTransaction::GetSubscriberData(dat);

            info!("Pushed transaction to work queue");
            // TODO: Needs hooking up.
            self.notify_tm_job_tx.send(t).unwrap();
            // self.work_queue.push(t);

            // Response placeholder.
            // let b = Bytes::copy_from_slice(b"ok");
            // let f = Frame::new(b); // Response placeholder
            // debug!("Sending reply: {:?}", f);
            // self.connection.write_frame(&f).await?;
        }
        Ok(())
    }
}
