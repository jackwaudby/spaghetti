use crate::listener::Listener;
use crate::manager::TransactionManager;
use crate::scheduler::Scheduler;
use crate::transaction::Transaction;
use crate::workloads::{tatp, Workload};

use config::Config;
use core::fmt::Debug;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};

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

    // Mpsc channel between handler and transaction manager for sending jobs, A -> S.
    let (notify_tm_job_tx, tm_listener_job_rx): (
        Sender<tatp::TatpTransaction>,
        Receiver<tatp::TatpTransaction>,
    ) = std::sync::mpsc::channel();

    // Initialise server listener state.
    let mut list = Listener {
        listener,
        notify_handlers_tx,
        tm_listener_rx: main_listener_rx,
    };
    info!("Server listening on {:?}", format!("{}:{}", add, port));

    info!("Initialise transaction manager");
    // Create transaction manager.
    let mut tm = TransactionManager::new(2, tm_listener_job_rx, tm_listener_rx, notify_main_tx);
    // Create scheduler.
    let s = Arc::new(Scheduler::new(Arc::clone(&workload)));

    thread::spawn(move || {
        info!("Start transaction manager");
        tm.run(s);
    });

    // Concurrently run the server and listen for the shutdown signal.
    tokio::select! {
        res = list.run(notify_tm_tx, notify_tm_job_tx, Arc::clone(&config)) => {
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
    } = list;
    // Drop broadcast transmitter.
    drop(notify_handlers_tx);
    // Drop listener's mpsc transmitter.
    // drop(shutdown_complete_tx);
    // // Wait until all transmitters on the mpsc channel have closed.
    // let _ = shutdown_complete_rx.recv().await;
    let _ = list.tm_listener_rx.recv().await;
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
