use crate::connection::Connection;
use crate::frame::Frame;
use crate::shutdown::Shutdown;
use crate::transaction::Transaction;
use crate::workloads::tatp;
use crate::workloads::tpcc;
use crate::workloads::Workload;
use crate::Result;

use bytes::Bytes;
use config::Config;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};

/// Server listener state.
#[derive(Debug)]
struct Listener {
    /// TCP listener, address is currently hard coded.
    listener: TcpListener,

    /// Broadcasts a shutdown signal to all active connections (`Handlers`).
    notify_shutdown_tx: broadcast::Sender<()>,

    /// Ensure that active connections have safely completed.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    /// Run the server, listen for inbound connections.
    pub async fn run(&mut self, conf: Arc<Config>) -> Result<()> {
        info!(
            "Initialise {:?} workload",
            conf.get_str("workload").unwrap()
        );
        let workload = Arc::new(Workload::new(conf.clone()).unwrap());
        // Handle to the thread-local generator.
        let mut rng: StdRng = SeedableRng::from_entropy();
        workload.populate_tables(&mut rng);
        info!("Tables loaded");

        info!("Accepting new connections");
        loop {
            // Accept new socket
            let (socket, _) = self.listener.accept().await?;
            info!("New connection accepted");
            // Create per-connection handler state
            let mut handler = Handler {
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown_tx.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // Clone handle to workload
            let w = workload.clone();
            // spawn new task to process the connection
            tokio::spawn(async move {
                if let Err(err) = handler.run(w).await {
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

    /// Listen for shutdown notifications.
    shutdown: Shutdown,

    /// Implicitly dropped when handler is dropped (safely finished), nofities the listener.
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    /// Process a single connection.
    ///
    /// Frames are requested from the socket and then processed.
    /// Responses are written back to the socket.
    pub async fn run(&mut self, workload: Arc<Workload>) -> Result<()> {
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

            // Initialise parameter generator.
            let w = workload.get_internals().config.get_str("workload").unwrap();
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

            // TODO: Execute transaction.
            let resp = tatp::get_subscriber_data(0, workload.clone());
            info!("{:?}", resp);
            // TODO: Write response to connection.

            let b = Bytes::copy_from_slice(b"ok");
            let f = Frame::new(b); // Response placeholder
            debug!("Sending reply: {:?}", f);
            self.connection.write_frame(&f).await?;
        }
        Ok(())
    }
}

/// Runs the server.
///
/// Accepts connection on the listener address, spawns handler for each.
/// ctrl-c triggers the shutdown.
pub async fn run(conf: Arc<Config>) {
    // broadcast channel for informing active connections of shutdown
    let (notify_shutdown_tx, _) = broadcast::channel(1);
    // mpsc channel to ensure server waits for connections to finish before shutting down
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Get the address and port.
    let add = conf.get_str("address").unwrap();
    let port = conf.get_str("port").unwrap();
    info!("Server listening on {:?}", format!("{}:{}", add, port));
    // initialise tcp listener
    let listener = TcpListener::bind(format!("{}:{}", add, port))
        .await
        .unwrap();
    // initialise server listener state
    let mut server = Listener {
        listener,
        notify_shutdown_tx,
        shutdown_complete_rx,
        shutdown_complete_tx,
    };

    // concurrently run the server and listen for the shutdown signal
    tokio::select! {
        res = server.run(conf) => {
            // All errors bubble up to here
            if let Err(err) = res {
                error!("{:?}",err);
            }
        }
        _ = signal::ctrl_c() => {
            info!("shutting down");
            // broadcast message to connections
        }
    }

    // destructure server listener to extract broadcast receiver/transmitter and mpsc transmitter
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown_tx,
        ..
    } = server;
    // drop broadcast transmitter
    drop(notify_shutdown_tx);
    // drop listener's mpsc transmitter
    drop(shutdown_complete_tx);
    // wait until all transmitters on the mpsc channel have closed.
    let _ = shutdown_complete_rx.recv().await;
}
