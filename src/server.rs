use crate::connection::Connection;
use crate::manager::TransactionManager;
use crate::scheduler::Scheduler;
use crate::shutdown::Shutdown;
use crate::workloads::{tatp, Workload};
use crate::Result;

use config::Config;
use crossbeam_queue::ArrayQueue;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};

/// Server listener state.
#[derive(Debug)]
struct Listener {
    /// TCP listener.
    listener: TcpListener,

    /// Handle to the work queue.
    work_queue: Arc<ArrayQueue<tatp::TatpTransaction>>,

    /// Broadcasts a shutdown signal to all active connections (`Handlers`).
    notify_shutdown_tx: broadcast::Sender<()>,

    /// Ensure that active connections have safely completed.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    /// Runs the server.
    ///
    /// Initialise the workload and populates tables and indexes.
    /// Listens for inbound connections.
    pub async fn run(&mut self) -> Result<()> {
        info!("Accepting new connections");
        loop {
            // Accept new socket.
            let (socket, _) = self.listener.accept().await?;
            info!("New connection accepted");
            // Create per-connection handler state.
            let mut handler = Handler {
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown_tx.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                work_queue: Arc::clone(&self.work_queue),
            };

            // Spawn new task to process the connection.
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
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

    /// Handle to the work queue,
    work_queue: Arc<ArrayQueue<tatp::TatpTransaction>>,

    /// Listen for shutdown notifications.
    shutdown: Shutdown,

    /// Implicitly dropped when handler is dropped (safely finished), nofities the listener.
    _shutdown_complete: mpsc::Sender<()>,
}

// TODO: split into read and write channels.
impl Handler {
    /// Process a single connection.
    ///
    /// Frames are requested from the socket and then processed.
    /// Responses are written back to the socket.
    pub async fn run(&mut self) -> Result<()> {
        debug!("Processing connection");
        // While shutdown signal not received try to read frames.0
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
            // let w = workload.get_internals().config.get_str("workload").unwrap();
            // // Deserialise to transaction.
            // let match w.as_str() {
            //     "tatp" => {
            //         let decoded: tatp::TatpTransaction =
            //             bincode::deserialize(&frame.get_payload()).unwrap();
            //         info!("Received: {:?}", decoded);
            //     }
            //     "tpcc" => {
            //         let decoded: tpcc::TpccTransaction =
            //             bincode::deserialize(&frame.get_payload()).unwrap();
            //         info!("Received: {:?}", decoded);
            //     }
            //     _ => unimplemented!(),
            // };

            // Deserialise transaction.
            let decoded: tatp::TatpTransaction =
                bincode::deserialize(&frame.get_payload()).unwrap();
            info!("Received: {:?}", decoded);

            // TODO: fixed as GetSubscriberData transaction.
            let dat = tatp::GetSubscriberData { s_id: 0 };
            let t = tatp::TatpTransaction::GetSubscriberData(dat);

            info!("Pushed transaction to work queue");
            self.work_queue.push(t).unwrap();

            // Response placeholder.
            // let b = Bytes::copy_from_slice(b"ok");
            // let f = Frame::new(b); // Response placeholder
            // debug!("Sending reply: {:?}", f);
            // self.connection.write_frame(&f).await?;
        }
        Ok(())
    }
}

/// Runs the server.
///
/// Accepts connection on the listener address, spawns handler for each.
/// ctrl-c triggers the shutdown.
pub async fn run(conf: Arc<Config>) {
    info!(
        "Initialise {:?} workload",
        conf.get_str("workload").unwrap()
    );

    info!("Initialise tables and indexes");
    let workload = Arc::new(Workload::new(conf.clone()).unwrap());

    info!("Populate tables and indexes");
    let mut rng: StdRng = SeedableRng::from_entropy();
    workload.populate_tables(&mut rng);
    info!("Tables loaded");

    info!("Initialise work queue");
    let work_queue = Arc::new(ArrayQueue::<tatp::TatpTransaction>::new(5));

    info!("Initialise listener");
    let add = conf.get_str("address").unwrap();
    let port = conf.get_str("port").unwrap();
    let listener = TcpListener::bind(format!("{}:{}", add, port))
        .await
        .unwrap();
    // Broadcast channel for informing active connections of shutdown.
    let (notify_shutdown_tx, _) = broadcast::channel(1);
    // Mpsc channel to ensure server waits for connections to finish before shutting down.
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    // Initialise server listener state.
    let mut server = Listener {
        listener,
        work_queue: Arc::clone(&work_queue),
        notify_shutdown_tx,
        shutdown_complete_rx,
        shutdown_complete_tx,
    };
    info!("Server listening on {:?}", format!("{}:{}", add, port));

    info!("Initialise transaction manager");
    // Get handle to workload.
    let w1 = Arc::clone(&workload);
    // Get handle to work queue.
    let wq = Arc::clone(&work_queue);
    // Start transaction manager's thread.
    let jh = thread::spawn(move || {
        info!("Started transaction manager");
        // TODO: Get threads from config.
        let tm = TransactionManager::new(2);
        info!("Started scheduler");
        let scheduler = Arc::new(Scheduler::new(w1));
        // TODO: Graceful shutdown of threadpool.
        loop {
            // If work queue is not empty.
            if !wq.is_empty() {
                // Pop job from work queue.
                let job = wq.pop().unwrap();
                // Get handle to scheduler.
                let s = Arc::clone(&scheduler);
                // Pass job to thread pool.
                tm.pool.execute(move || {
                    info!("Execute {:?}", job);
                    match job {
                        tatp::TatpTransaction::GetSubscriberData(payload) => {
                            s.register("txn1").unwrap();
                            let v = s.read(&payload.s_id.to_string(), "txn1", 1).unwrap();
                            info!("{:?}", v);
                        }
                        _ => unimplemented!(),
                    }
                })
            }
        }
    });

    // Concurrently run the server and listen for the shutdown signal.
    tokio::select! {
        res = server.run() => {
            // All errors bubble up to here.
            if let Err(err) = res {
                error!("{:?}",err);
            }
        }
        _ = signal::ctrl_c() => {
            info!("shutting down");
            // Broadcast message to connections.
        }
    }

    jh.join().unwrap();

    // Destructure server listener to extract broadcast receiver/transmitter and mpsc transmitter.
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown_tx,
        ..
    } = server;
    // Drop broadcast transmitter.
    drop(notify_shutdown_tx);
    // Drop listener's mpsc transmitter.
    drop(shutdown_complete_tx);
    // Wait until all transmitters on the mpsc channel have closed.
    let _ = shutdown_complete_rx.recv().await;
}
