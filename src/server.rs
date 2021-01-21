use crate::connection::Connection;
use crate::manager::TransactionManager;
use crate::scheduler::Scheduler;
use crate::shutdown::{NotifyTransactionManager, Shutdown};
use crate::transaction::Transaction;
use crate::workloads::tatp::TatpTransaction;
use crate::workloads::{tatp, tpcc, Workload};
use crate::Result;

use config::Config;
use core::fmt::Debug;
use crossbeam_queue::ArrayQueue;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{debug, error, info};

/// Server listener state.
#[derive(Debug)]
struct Listener {
    /// TCP listener.
    listener: TcpListener,

    /// Broadcasts a shutdown signal to all active connections (`Handlers`).
    ///
    /// This is communication between async code.
    notify_handlers_tx: tokio::sync::broadcast::Sender<()>,

    /// This is dropped when the transaction manager has cleaned up.
    ///
    /// This is communication from sync code to async code.
    tm_listener_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
}

impl Listener {
    /// Runs the server.
    ///
    /// Initialise the workload and populates tables and indexes.
    /// Listens for inbound connections.
    pub async fn run(
        &mut self,
        notify_tm_tx: std::sync::mpsc::Sender<()>,
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
            let txn = match w.as_str() {
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
            let t = Box::new(tatp::TatpTransaction::GetSubscriberData(dat));

            info!("Pushed transaction to work queue");
            // TODO: Needs hooking up.
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

/// Runs the server.
///
/// Accepts connection on the listener address, spawns handler for each.
/// ctrl-c triggers the shutdown.
pub async fn run(config: Arc<Config>) {
    info!(
        "Initialise {:?} workload",
        config.get_str("workload").unwrap()
    );

    info!("Initialise tables and indexes");
    let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());

    info!("Populate tables and indexes");
    let mut rng: StdRng = SeedableRng::from_entropy();
    workload.populate_tables(&mut rng);
    info!("Tables loaded");

    info!("Initialise listener");
    let add = config.get_str("address").unwrap();
    let port = config.get_str("port").unwrap();
    let listener = TcpListener::bind(format!("{}:{}", add, port))
        .await
        .unwrap();
    // Broadcast channel between listener and handlers, informing active connections of
    // shutdown. A -> A.
    let (notify_handlers_tx, _) = tokio::sync::broadcast::channel(1);

    // Mpsc channel between handlers and transaction manager, A-> S.
    let (notify_tm_tx, tm_listener_rx) = std::sync::mpsc::channel();

    // Mpsc channel between transaction manager and listener, S -> A.
    let (notify_main_tx, main_listener_rx) = tokio::sync::mpsc::unbounded_channel();

    // Initialise server listener state.
    let mut server = Listener {
        listener,
        notify_handlers_tx,
        tm_listener_rx: main_listener_rx,
    };
    info!("Server listening on {:?}", format!("{}:{}", add, port));

    info!("Initialise transaction manager");
    // Create transaction manager.
    let mut tm = TransactionManager::new(2, tm_listener_rx, notify_main_tx);
    // Create scheduler.
    // let s = Scheduler::new(Arc::clone(&workload));
    thread::spawn(move || {
        info!("Start transaction manager");
        tm.run();
    });

    // Concurrently run the server and listen for the shutdown signal.
    tokio::select! {
        res = server.run(notify_tm_tx, Arc::clone(&config)) => {
            // All errors bubble up to here.
            if let Err(err) = res {
                error!("{:?}",err);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Shutting down server");
            // Broadcast message to connections.
        }
    }

    // Destructure server listener to extract broadcast receiver/transmitter and mpsc transmitter.
    let Listener {
        // mut shutdown_complete_rx,
        // shutdown_complete_tx,
        notify_handlers_tx,
        ..
    } = server;
    // Drop broadcast transmitter.
    drop(notify_handlers_tx);
    // Drop listener's mpsc transmitter.
    // drop(shutdown_complete_tx);
    // // Wait until all transmitters on the mpsc channel have closed.
    // let _ = shutdown_complete_rx.recv().await;
    let _ = server.tm_listener_rx.recv().await;
}

impl Debug for dyn Transaction + Send {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Test")
    }
}

// async fn tm_run(
//     queue: Arc<ArrayQueue<Box<dyn Transaction + Send>>>,
//     config: Arc<Config>,
//     scheduler: Arc<Scheduler>,
//     tm: Arc<TransactionManager>,
// ) {
//     loop {
//         let job = match queue.pop() {
//             Some(job) => {
//                 let w = config.get_str("workload").unwrap();
//                 match w.as_str() {
//                     "tatp" => {
//                         let transaction: &TatpTransaction = job
//                             .as_any()
//                             .downcast_ref::<TatpTransaction>()
//                             .expect("not a TatpTransaction");
//                         let t = transaction.clone();
//                         let s = Arc::clone(&scheduler);
//                         tm.pool.execute(move || {
//                             match t {
//                                 tatp::TatpTransaction::GetSubscriberData(payload) => {
//                                     s.register("txn1").unwrap();
//                                     let v = s.read(&payload.s_id.to_string(), "txn1", 1).unwrap();
//                                     s.commit("txn1");
//                                     info!("{:?}", v);
//                                     // TODO: Send to response queue.
//                                 }
//                                 _ => unimplemented!(),
//                             }
//                         })
//                     }
//                     "tpcc" => {}
//                     _ => unimplemented!(),
//                 }
//             }
//             None => {}
//         };
//     }
// }

// //
// Pop job from work queue.
//     let job = match wq.pop() {
//         Some(job) => {
//             let w = c1.get_str("workload").unwrap();
//             match w.as_str() {
//                 "tatp" => {
//                     let transaction: &TatpTransaction = job
//                         .as_any()
//                         .downcast_ref::<TatpTransaction>()
//                         .expect("not a TatpTransaction");
//                     let t = transaction.clone();

//                     // Get handle to scheduler.
//                     let s = Arc::clone(&scheduler);
//                     // Pass job to thread pool.
//                     tm.pool.execute(move || {
//                         // info!("Execute {:?}", job);
//                         match t {
//                             tatp::TatpTransaction::GetSubscriberData(payload) => {
//                                 s.register("txn1").unwrap();
//                                 let v =
//                                     s.read(&payload.s_id.to_string(), "txn1", 1).unwrap();
//                                 s.commit("txn1");
//                                 info!("{:?}", v);
//                                 // TODO: Send to response queue.
//                             }
//                             _ => unimplemented!(),
//                         }
//                     })
//                 }
//                 "tpcc" => {}
//                 _ => unimplemented!(),
//             }
//         },
//         None =>    {},
//     };
// }

// }
