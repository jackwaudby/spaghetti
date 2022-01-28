use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

use clap::{arg, App};
use crossbeam_utils::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::time::Instant;
use tracing::Level;
use tracing::{debug, error, info};
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

    // logging
    let level = match config.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };

    // TODO: hack; alternative solution: https://github.com/tokio-rs/tracing/issues/597#issuecomment-814507031
    let file_appender = tracing_appender::rolling::hourly("./log/", "debug.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    if let Level::DEBUG = level {
        let subscriber = fmt::Subscriber::builder()
            .with_writer(non_blocking)
            .with_max_level(level)
            .with_thread_names(true)
            .with_target(false)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    } else {
        let subscriber = fmt::Subscriber::builder().finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    };

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

    // shutdown channel: thread(s) -> main
    let (thread_tx, main_rx): (SyncSender<i32>, Receiver<i32>) = mpsc::sync_channel(32);

    let scheduler: Scheduler = utils::init_scheduler(&config);

    info!("Starting execution");
    global_stats.start();

    let cores = config.get_int("cores").unwrap() as usize;
    let core_ids = core_affinity::get_core_ids().unwrap();

    thread::scope(|s| {
        let scheduler = &scheduler;
        let database = &database;
        let config = &config;

        let mut shutdown_channels = Vec::new();

        for (thread_id, core_id) in core_ids[..cores].iter().enumerate() {
            let txc = tx.clone();
            let thread_txc = thread_tx.clone();

            // Coordinator to thread shutdown
            let (tx, rx): (Sender<i32>, Receiver<i32>) = mpsc::channel();
            shutdown_channels.push(tx);

            s.builder()
                .name(thread_id.to_string())
                .spawn(move |_| {
                    core_affinity::set_for_current(*core_id); // pin thread to cpu core
                    utils::run(thread_id, config, scheduler, database, txc, rx, thread_txc);
                })
                .unwrap();
        }

        // Shutdown management
        let mut received = 0;
        loop {
            // Each thread should send a message when it terminates
            // This message can be: (i) exited normally (1) or (ii) deadlocked (2)
            // It should receive 1 message from each core

            let res = main_rx.try_recv();

            match res {
                Ok(value) => {
                    received += 1;

                    // message was from a clean shutdown
                    if value == 1 {
                        // clean shutdown
                        debug!("Exited normally");
                    }

                    // message was from a problemed threa
                    if value == 2 {
                        error!("Deadlock detected!");

                        // broadcast shutdown to all
                        for channel in &shutdown_channels {
                            let res = channel.send(1); // may already be shutdown
                            match res {
                                Ok(_) => {}
                                Err(e) => debug!("{}", e),
                            }
                        }
                    }
                }
                Err(_) => {
                    // shouldn't get here
                }
            }

            if received == cores {
                break;
            }
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
