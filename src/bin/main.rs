use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

use clap::clap_app;
use crossbeam_utils::thread;
use std::sync::mpsc;
use std::time::Instant;
use tracing::info;

fn main() {
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

    let mut config = utils::init_config("Settings.toml");

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

    utils::set_log_level(&config);
    utils::create_results_dir(&config);

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

        for (_, core_id) in core_ids[..cores].iter().enumerate() {
            let txc = tx.clone();
            // TODO: give thread an id
            s.spawn(move |_| {
                core_affinity::set_for_current(*core_id); // pin thread to cpu core
                utils::run(config, scheduler, database, txc);
            });
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
