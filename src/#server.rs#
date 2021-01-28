use crate::server::handler::Request;
use crate::server::listener::Listener;
use crate::server::manager::TransactionManager;
use crate::server::scheduler::Scheduler;
use crate::workloads::Workload;
use crate::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};

pub mod handler;

pub mod pool;

pub mod listener;

pub mod scheduler;

pub mod storage;

pub mod queue;

pub mod manager;

/// Runs the server.
///
/// Accepts connection on the listener address, spawns handler for each.
/// ctrl-c triggers the shutdown.
pub async fn run(config: Arc<Config>) -> Result<()> {
    info!("Initialise {:?} workload", config.get_str("workload")?);

    info!("Initialise tables and indexes");
    let workload = Arc::new(Workload::new(Arc::clone(&config))?);

    info!("Populate tables and indexes");
    let mut rng: StdRng = SeedableRng::from_entropy();
    workload.populate_tables(&mut rng)?;
    info!("Tables loaded");

    info!("Initialise listener");
    let add = config.get_str("address")?;
    let port = config.get_str("port")?;
    let listener = TcpListener::bind(format!("{}:{}", add, port)).await?;

    ////// Shutdown //////
    // Broadcast channel between listener and handlers, informing active connections of
    // shutdown. A -> A.
    let (notify_handlers_tx, _) = tokio::sync::broadcast::channel(1);
    // Mpsc channel between read handlers and transaction manager, A-> S.
    let (notify_tm_tx, tm_shutdown_rx) = std::sync::mpsc::channel();

    let (notify_wh_tx, _) = tokio::sync::broadcast::channel(1);

    // Each write handler gets a sender.
    let (notify_listener_tx, list_shutdown_rx) = tokio::sync::mpsc::unbounded_channel();

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
        listener_rx: list_shutdown_rx,
        rh_tx: notify_tm_tx.clone(),
        wh_rx: notify_wh_tx.subscribe(),
        listener_tx: notify_listener_tx.clone(),
    };
    info!("Server listening on {:?}", format!("{}:{}", add, port));

    info!("Initialise transaction manager");
    // Create transaction manager.
    let tm = TransactionManager::new(2, work_rx, tm_shutdown_rx, notify_wh_tx.clone());
    // Create scheduler.
    let s = Arc::new(Scheduler::new(Arc::clone(&workload)));
    manager::run(tm, s);

    // Concurrently run the server and listen for the shutdown signal.
    tokio::select! {
        res = list.run(notify_tm_tx, work_tx,  notify_wh_tx, notify_listener_tx, Arc::clone(&config)) => {
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
        mut listener_rx,
        notify_handlers_tx,
        rh_tx,
        mut wh_rx,
        listener_tx,
        ..
    } = list;
    // Drop broadcast transmitter, notifies handlers, which terminate and drop
    // which notifies tm which terminates and drops
    // which notifies listener here and everything is done!
    drop(notify_handlers_tx);
    // Closes TM
    info!("Drop listeners tm tx");
    drop(rh_tx);
    // Wait for TM
    wh_rx.recv().await;
    info!("Received shutdown from tm ");
    // Closes WH
    info!("Drop listeners wh tx");

    drop(listener_tx);

    listener_rx.recv().await;
    info!("Clean shutdown");
    Ok(())
}
