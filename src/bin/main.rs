use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

use clap::clap_app;
use crossbeam_utils::thread;
// use pbr::MultiBar;
use std::sync::mpsc;
use std::time::Instant;
use tracing::info;
use tracing::Level;
use tracing_subscriber::fmt;

fn main() {
    // config file
    let mut config = utils::init_config("Settings.toml");

    // command line
    let matches = clap_app!(spag =>
                            (version: "0.1.0")
                            (author: "j. waudby <j.waudby2@newcastle.ac.uk>")
                            (about: "spaghetti")
                            (@arg WORKLOAD: -w --workload +takes_value "Set a workload")
                            (@arg PROTOCOL: -p --protocol +takes_value "Set a protocol")
                            (@arg SF: -s --scalefactor +takes_value "Set a scale factor")
                            (@arg TRANSACTIONS: -t --transactions +takes_value "Transactions per core")
                            (@arg CORES: -c --cores +takes_value "Number of cores to use")
                            (@arg LOG: -l --log +takes_value "Log level")
    )
    .get_matches();

    if let Some(w) = matches.value_of("WORKLOAD") {
        config.set("workload", w).unwrap();
    }

    if let Some(p) = matches.value_of("PROTOCOL") {
        config.set("protocol", p).unwrap();
    }

    if let Some(s) = matches.value_of("SF") {
        config.set("scale_factor", s).unwrap();
    }

    if let Some(t) = matches.value_of("TRANSACTIONS") {
        config.set("transactions", t).unwrap();
    }

    if let Some(c) = matches.value_of("CORES") {
        config.set("cores", c).unwrap();
    }

    if let Some(l) = matches.value_of("LOG") {
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

    let mut global_stats = GlobalStatistics::new(&config);

    let dg_start = Instant::now();
    let database: Database = utils::init_database(&config);
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    let scheduler: Scheduler = utils::init_scheduler(&config);
    let (tx, rx) = mpsc::channel();

    info!("Starting execution");
    global_stats.start();

    let cores = config.get_int("cores").unwrap() as usize;
    let core_ids = core_affinity::get_core_ids().unwrap();

    thread::scope(|s| {
        let scheduler = &scheduler;
        let database = &database;
        let config = &config;

        // let mb = MultiBar::new(); // progress bar

        for (thread_id, core_id) in core_ids[..cores].iter().enumerate() {
            let txc = tx.clone();

            // let mut p = mb.create_bar(100); // create bar
            // p.show_speed = false;
            // p.show_counter = false;

            s.builder()
                .name(thread_id.to_string())
                .spawn(move |_| {
                    core_affinity::set_for_current(*core_id); // pin thread to cpu core
                                                              // utils::run(thread_id, config, scheduler, database, txc, Some(p));
                    utils::run(thread_id, config, scheduler, database, txc, None);
                })
                .unwrap();
        }

        // mb.listen();
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
