use crate::handler::Request;
use crate::listener::Listener;
use crate::manager::{self, TransactionManager};
use crate::scheduler::Scheduler;
use crate::transaction::Transaction;
use crate::workloads::{tatp, Workload};

use config::Config;
use core::fmt::Debug;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
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
    ////// Shutdown //////
    // Broadcast channel between listener and handlers, informing active connections of
    // shutdown. A -> A.
    let (notify_handlers_tx, _) = tokio::sync::broadcast::channel(1);
    // Mpsc channel between handlers and transaction manager, A-> S.
    let (notify_tm_tx, tm_shutdown_rx) = std::sync::mpsc::channel();
    // Mpsc channel between transaction manager and listener, S -> A.
    let (notify_main_tx, main_listener_rx) = tokio::sync::mpsc::unbounded_channel();

    ///// Work sending /////
    // Mpsc channel between handlers (producers) and transaction manager (consumer).
    // Each `Handler` gets a clone of the  `Sender` end.
    // `TransactionManager` gets the `Receiver` end.
    // Communication between async code and sync code (A -> S).
    // Use std unbounded channel.
    let (work_tx, work_rx): (Sender<Request>, Receiver<Request>) = std::sync::mpsc::channel();

    // Initialise server listener state.
    let mut list = Listener {
        listener,
        notify_handlers_tx,
        tm_listener_rx: main_listener_rx,
    };
    info!("Server listening on {:?}", format!("{}:{}", add, port));

    info!("Initialise transaction manager");
    // Create transaction manager.
    let tm = TransactionManager::new(2, work_rx, tm_shutdown_rx, notify_main_tx);
    // Create scheduler.
    let s = Arc::new(Scheduler::new(Arc::clone(&workload)));
    manager::run(tm, s);

    // Concurrently run the server and listen for the shutdown signal.
    tokio::select! {
        res = list.run(notify_tm_tx, work_tx,Arc::clone(&config)) => {
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
    // Drop broadcast transmitter, notifies handlers, which terminate and drop
    // which notifies tm which terminates and drops
    // which notifies listener here and everything is done!
    drop(notify_handlers_tx);

    let _ = list.tm_listener_rx.recv().await;
}

impl Debug for dyn Transaction + Send {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Test")
    }
}
