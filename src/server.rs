use crate::common::message::Request;
use crate::server::listener::Listener;
use crate::server::manager::TransactionManager;
use crate::server::scheduler::Protocol;
use crate::workloads::Workload;
use crate::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{debug, error, info};

pub mod read_handler;

pub mod write_handler;

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

    // Shutdown channels.
    let (notify_read_handlers_tx, _) = tokio::sync::broadcast::channel(1);
    let (notify_tm_tx, tm_shutdown_rx) = std::sync::mpsc::channel();
    let (notify_wh_tx, _) = tokio::sync::broadcast::channel(1);
    let (notify_listener_tx, listener_shutdown_rx) = tokio::sync::mpsc::unbounded_channel();

    // Work channels.
    let (work_tx, work_rx): (Sender<Request>, Receiver<Request>) = std::sync::mpsc::channel();

    // Initialise server listener state.
    let mut list = Listener {
        listener,
        notify_read_handlers_tx,
        notify_tm_tx: notify_tm_tx,
        wh_shutdown_rx: notify_wh_tx.subscribe(),
        notify_listener_tx,
        listener_shutdown_rx,
    };
    info!("Server listening on {:?}", format!("{}:{}", add, port));

    info!("Initialise transaction manager");
    // Create transaction manager.
    let tm = TransactionManager::new(2, work_rx, tm_shutdown_rx, notify_wh_tx.clone());
    // Create scheduler.
    let s = Arc::new(Protocol::new(Arc::clone(&workload))?);
    manager::run(tm, s);

    // Concurrently run the server and listen for the shutdown signal.
    tokio::select! {
        res = list.run(work_tx, notify_wh_tx, Arc::clone(&config)) => {
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
        notify_read_handlers_tx,
        notify_tm_tx,
        mut wh_shutdown_rx,
        notify_listener_tx,
        mut listener_shutdown_rx,
        ..
    } = list;
    debug!("Close channel to read handlers.");
    drop(notify_read_handlers_tx);
    debug!("Close channel to transaction manager.");
    drop(notify_tm_tx);
    debug!("Wait for transaction manger to shutdown.");
    match wh_shutdown_rx.recv().await {
        Ok(_) => (),
        Err(_) => (),
    }
    debug!("Close channel to listener.");
    drop(notify_listener_tx);
    debug!("Wait for write handlers to shutdown.");
    listener_shutdown_rx.recv().await;
    info!("Server shutdown");
    Ok(())
}
