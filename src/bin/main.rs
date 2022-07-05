use spaghetti::common::coordinator;
use spaghetti::common::global_state::GlobalState;
use spaghetti::common::statistics::global::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::common::wait_manager::WaitManager;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

use clap::{arg, Command};
use crossbeam_utils::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use tracing::{info, Level};
use tracing_subscriber::fmt;

fn main() {
    // config file
    let mut config = utils::init_config("Settings.toml");

    // command line
    let matches = Command::new("MyApp")
        .version("0.1.0")
        .author("j. waudby <j.waudby2@newcastle.ac.uk>")
        .about("spaghetti")
        .arg(arg!(-w --workload <WORKLOAD> "Set a workload").required(false))
        .arg(arg!(-p --protocol <PROTOCOL> "Set a protocol").required(false))
        .arg(arg!(-s --scalefactor <SF> "Set a scale factor").required(false))
        .arg(arg!(-t --transactions <TRANSACTIONS> "Transactions per core").required(false))
        .arg(arg!(-c --cores <CORES> "Number of cores to use").required(false))
        .arg(arg!(-r --relevant <RELEVANT> "Reduced relevant DFS (MSGT only)").required(false))
        .get_matches();

    if let Some(w) = matches.get_one::<String>("workload") {
        config.set("workload", w.clone()).unwrap();
    }

    if let Some(p) = matches.get_one::<String>("protocol") {
        config.set("protocol", p.clone()).unwrap();
    }

    if let Some(s) = matches.get_one::<String>("scalefactor") {
        config.set("scale_factor", s.clone()).unwrap();
    }

    if let Some(t) = matches.get_one::<String>("transactions") {
        config.set("transactions", t.clone()).unwrap();
    }

    if let Some(c) = matches.get_one::<String>("cores") {
        config.set("cores", c.clone()).unwrap();
    }

    // MSGT
    if let Some(dfs) = matches.get_one::<String>("relevant") {
        config.set("relevant_dfs", dfs.clone()).unwrap();
    }

    // logging
    let level = match config.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };

    let subscriber = fmt::Subscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // log directory
    utils::create_log_dir(&config);

    // global stats
    let mut global_stats = GlobalStatistics::new(&config);
    let (tx, rx) = mpsc::channel(); // channel for thread local stats

    let cores = config.get_int("cores").unwrap() as usize;
    let core_ids = core_affinity::get_core_ids().unwrap();
    let database: Database = utils::init_database(&config);
    let scheduler: Scheduler = Scheduler::new(&config).unwrap();
    let wait_manager = WaitManager::new(cores);
    let global_state = GlobalState::new(config, scheduler, database, wait_manager);

    info!("Starting execution");
    global_stats.start();

    thread::scope(|s| {
        let global_state = &global_state;
        let mut shutdown_channels = Vec::new();

        for (thread_id, core_id) in core_ids[..cores].iter().enumerate() {
            let stats_tx = tx.clone();

            // Coordinator to thread shutdown
            let (tx, _): (Sender<i32>, Receiver<i32>) = mpsc::channel();
            shutdown_channels.push(tx);

            s.builder()
                .name(thread_id.to_string())
                .spawn(move |_| {
                    core_affinity::set_for_current(*core_id);
                    coordinator::run(thread_id, stats_tx, global_state);
                })
                .unwrap();
        }
    })
    .unwrap();

    drop(tx);

    global_stats.end();
    info!("Execution finished");

    info!("Collecting statistics..");
    while let Ok(local_stats) = rx.recv() {
        global_stats.merge(local_stats);
    }

    // let mut wtr = csv::Writer::from_path("aborted_latency.csv").unwrap();
    // wtr.serialize(&global_stats.aborted_latency).unwrap();
    // wtr.flush().unwrap();

    global_stats.print_to_console();

    global_stats.validate();
}
