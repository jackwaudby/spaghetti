use spaghetti::common::message::{InternalResponse, Message};
use spaghetti::common::parameter_generation::ParameterGenerator;
use spaghetti::embedded::generator::{self, Generator, InternalRequest};
use spaghetti::embedded::logging::{self, Logger};
use spaghetti::embedded::manager::{self, TransactionManager};
use spaghetti::workloads::tatp::generator::TatpGenerator;
use spaghetti::workloads::Workload;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

fn main() {
    // Config.
    let file = "Server.toml";
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file)).unwrap();
    let config = Arc::new(settings);

    // Logger.
    let level = match config.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Workload
    let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());
    let mut rng: StdRng = SeedableRng::from_entropy();
    workload.populate_tables(&mut rng).unwrap();

    // Pipes
    let (req_tx, req_rx): (SyncSender<InternalRequest>, Receiver<InternalRequest>) =
        std::sync::mpsc::sync_channel(32);

    let (resp_tx, resp_rx): (SyncSender<InternalResponse>, Receiver<InternalResponse>) =
        std::sync::mpsc::sync_channel(32);

    let (main_tx, main_rx): (SyncSender<()>, Receiver<()>) = std::sync::mpsc::sync_channel(32);

    // Generator
    let mut g = Generator::new(req_tx, resp_tx);
    generator::run(g, config);

    // Logger.
    let mut logger = Logger::new(resp_rx, main_tx);
    logging::run(logger);

    // Manager.
    let mut tm = TransactionManager::new(Arc::clone(&workload), req_rx);
    manager::run(tm);

    main_rx.recv();
}
