use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::common::wait_manager::WaitManager;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

use clap::{arg, Command};
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
    let matches = Command::new("MyApp")
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

    if let Some(l) = matches.get_one::<String>("log") {
        config.set("log", l.clone()).unwrap();
    }

    // YCSB
    if let Some(theta) = matches.get_one::<String>("theta") {
        config.set("theta", theta.clone()).unwrap();
    }

    if let Some(ur) = matches.get_one::<String>("updaterate") {
        config.set("update_rate", ur.clone()).unwrap();
    }

    if let Some(sr) = matches.get_one::<String>("serializablerate") {
        config.set("serializable_rate", sr.clone()).unwrap();
    }

    // Attendez
    if let Some(wm) = matches.get_one::<String>("watermark") {
        config.set("watermark", wm.clone()).unwrap();
    }

    if let Some(a) = matches.get_one::<String>("increase") {
        config.set("increase", a.clone()).unwrap();
    }

    if let Some(b) = matches.get_one::<String>("decrease") {
        config.set("decrease", b.clone()).unwrap();
    }

    if let Some(d) = matches.get_one::<String>("nowait") {
        config.set("no_wait_write", d.clone()).unwrap();
    }

    // MSGT
    if let Some(dfs) = matches.get_one::<String>("relevant") {
        config.set("relevant_dfs", dfs.clone()).unwrap();
    }

    // OWH
    if let Some(ta) = matches.get_one::<String>("types") {
        config.set("type_aware", ta.clone()).unwrap();
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
