use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::common::wait_manager::WaitManager;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

use clap::{arg, App};
use crossbeam_utils::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::Instant;
use tracing::{info, Level};
use tracing_subscriber::fmt;

fn main() {
    // config file
    let mut config = utils::init_config("Settings.toml");

    // command line
    let matches = App::new("MyApp")
        .version("0.1.0")
        .author("j. waudby <j.waudby2@newcastle.ac.uk>")
        .about("spaghetti")
        .arg(arg!(-w --workload <WORKLOAD> "Set a workload").required(false))
        .arg(arg!(-p --protocol <PROTOCOL> "Set a protocol").required(false))
        .arg(arg!(-s --scalefactor <SF> "Set a scale factor").required(false))
        .arg(arg!(-t --transactions <TRANSACTIONS> "Transactions per core").required(false))
        .arg(arg!(-c --cores <CORES> "Number of cores to use").required(false))
        .arg(arg!(-l --log <LOG> "Log level").required(false))
        .arg(arg!(-h --theta <THETA> "Contention (YCSB only)").required(false))
        .arg(arg!(-u --updaterate <UPDATERATE> "Update rate (YCSB only)").required(false))
        .arg(
            arg!(-i --serializablerate <SERIALIZABLERATE> "Serializable rate (YCSB only)")
                .required(false),
        )
        .arg(arg!(-m --watermark <WATERMARK> "Watermark (Attendez only)").required(false))
        .arg(arg!(-a --increase <INCREASE> "Additive increase (Attendez only)").required(false))
        .arg(
            arg!(-b --decrease <DECREASE> "Multiplicative decrease (Attendez only)")
                .required(false),
        )
        .arg(arg!(-d --nowait <NOWAIT> "No wait write (Attendez only)").required(false))
        .arg(arg!(-r --relevant <RELEVANT> "Reduced relevant DFS (MSGT only)").required(false))
        .arg(arg!(-o --types <TYPES> "Transaction types optimization (OWH only)").required(false))
        .get_matches();

    if let Some(w) = matches.value_of("workload") {
        config.set("workload", w).unwrap();
    }

    if let Some(p) = matches.value_of("protocol") {
        config.set("protocol", p).unwrap();
    }

    if let Some(s) = matches.value_of("scalefactor") {
        config.set("scale_factor", s).unwrap();
    }

    if let Some(t) = matches.value_of("transactions") {
        config.set("transactions", t).unwrap();
    }

    if let Some(c) = matches.value_of("cores") {
        config.set("cores", c).unwrap();
    }

    if let Some(l) = matches.value_of("log") {
        config.set("log", l).unwrap();
    }

    // YCSB
    if let Some(theta) = matches.value_of("theta") {
        config.set("theta", theta).unwrap();
    }

    if let Some(ur) = matches.value_of("updaterate") {
        config.set("update_rate", ur).unwrap();
    }

    if let Some(sr) = matches.value_of("serializablerate") {
        config.set("serializable_rate", sr).unwrap();
    }

    // Attendez
    if let Some(wm) = matches.value_of("watermark") {
        config.set("watermark", wm).unwrap();
    }

    if let Some(a) = matches.value_of("increase") {
        config.set("increase", a).unwrap();
    }

    if let Some(b) = matches.value_of("decrease") {
        config.set("decrease", b).unwrap();
    }

    if let Some(d) = matches.value_of("nowait") {
        config.set("no_wait_write", d).unwrap();
    }

    // MSGT
    if let Some(dfs) = matches.value_of("relevant") {
        config.set("relevant_dfs", dfs).unwrap();
    }

    // OWH
    if let Some(ta) = matches.value_of("types") {
        config.set("type_aware", ta).unwrap();
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

    // data generation
    let dg_start = Instant::now();
    let database: Database = utils::init_database(&config);
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    let scheduler: Scheduler = utils::init_scheduler(&config);

    info!("Starting execution");
    global_stats.start();

    let cores = config.get_int("cores").unwrap() as usize;
    let core_ids = core_affinity::get_core_ids().unwrap();

    let wm = WaitManager::new(cores);

    thread::scope(|s| {
        let scheduler = &scheduler;
        let database = &database;
        let wm = &wm;
        let config = &config;

        let mut shutdown_channels = Vec::new();

        for (thread_id, core_id) in core_ids[..cores].iter().enumerate() {
            let txc = tx.clone();

            // Coordinator to thread shutdown
            let (tx, _): (Sender<i32>, Receiver<i32>) = mpsc::channel();
            shutdown_channels.push(tx);

            s.builder()
                .name(thread_id.to_string())
                .spawn(move |_| {
                    core_affinity::set_for_current(*core_id); // pin thread to cpu core
                    utils::run(thread_id, config, scheduler, database, txc, wm);
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
        global_stats.merge_into(local_stats);
    }
    global_stats.write_to_file();
}
