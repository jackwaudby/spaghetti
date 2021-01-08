use crate::connection::Connection;
use crate::shutdown::Shutdown;
use crate::transaction::Transaction;
use crate::Result;

use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};

/// Server listener state.
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
    pub async fn run(&mut self) -> Result<()> {
        println!("Accepting inbound connections");
        loop {
            // Accept new socket
            let (socket, _) = self.listener.accept().await?;
            println!("Accepted connection!");
            // Create per-connection handler state
            let mut handler = Handler {
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown_tx.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // spawn new task to process the connection
            tokio::spawn(async move {
                println!("Run handler");
                if let Err(err) = handler.run().await {
                    println!("{:?}", err);
                }
            });
            println!("Closing connection!");
        }
    }
}

/// Per-connection handler
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
    pub async fn run(&mut self) -> Result<()> {
        // While shutdown signal not received try to read frames.
        while !self.shutdown.is_shutdown() {
            // While reading a requested frame listen for shutdown.
            println!("Read frames");
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // shutdown signal received, return from handler's run.
                    // this terminates the task
                    return Ok(());
                }
            };

            println!("{:?}", maybe_frame);
            // If None is returned from maybe frame then the socket has been closed.
            // Terminate task
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // TODO: convert into command.
            println!("{:?}", frame.get_payload());

            let decoded: Transaction = bincode::deserialize(&frame.get_payload()).unwrap();
            println!("{:?}", decoded);
        }
        println!("Close connection");
        Ok(())
    }
}

/// Runs the server.
///
/// Accepts connection on the listener address, spawns handler for each.
/// ctrl-c triggers the shutdown.
pub async fn run() {
    // broadcast channel for informing active connections of shutdown
    let (notify_shutdown_tx, _) = broadcast::channel(1);
    // mpsc channel to ensure server waits for connections to finish before shutting down
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // initialise tcp listener
    let listener = TcpListener::bind("127.0.0.1:6142").await.unwrap();
    // initialise server listener state
    let mut server = Listener {
        listener,
        notify_shutdown_tx,
        shutdown_complete_rx,
        shutdown_complete_tx,
    };

    // concurrently run the server and listen for the shutdown signal
    tokio::select! {
        res = server.run() => {
            // All errors bubble up to here
            if let Err(err) = res {
                println!("{:?}",err);
            }
        }
        _ = signal::ctrl_c() => {
            println!("shutting down");
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
