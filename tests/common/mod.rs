use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::datatype::SuccessMessage;
use spaghetti::storage::Database;

use config::Config;
use crossbeam_utils::thread;
//use petgraph::algo;
//use petgraph::graph::Graph;
use log::info;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::sync::mpsc;
use std::time::Instant;

pub fn setup_config(protocol: &str, anomaly: &str) -> Config {
    let mut c = utils::init_config("./tests/Acid.toml");
    let cores = core_affinity::get_core_ids().unwrap().len() as i64; // always use max cores available
    c.set("cores", cores).unwrap();
    c.set("protocol", protocol).unwrap();
    c.set("anomaly", anomaly).unwrap();
    c
}

pub fn run(protocol: &str, anomaly: &str) {
    let config = setup_config(protocol, anomaly); // set up config
    utils::create_log_dir(&config); // log transaction results

    let mut global_stats = GlobalStatistics::new(&config); // global stats collector
    let (tx, rx) = mpsc::channel();

    let dg_start = Instant::now();
    let database: Database = utils::init_database(&config); // populate database
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    let scheduler: Scheduler = utils::init_scheduler(&config); // create scheduler

    info!("Starting execution");
    global_stats.start();

    let core_ids = core_affinity::get_core_ids().unwrap(); // always use max cores available

    thread::scope(|s| {
        let scheduler = &scheduler;
        let database = &database;
        let config = &config;

        for (thread_id, core_id) in core_ids.iter().enumerate() {
            let txc = tx.clone();

            s.builder()
                .name(thread_id.to_string())
                .spawn(move |_| {
                    core_affinity::set_for_current(*core_id); // pin thread to cpu core
                    utils::run(thread_id, config, scheduler, database, txc);
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

/// Aborted Read (G1a).
///
/// # Anomaly check
///
/// Transactions write version = 2 but then abort. Each read should return version=1. Otherwise, a G1a anomaly has occurred.
pub fn g1a(protocol: &str) {
    let anomaly = "g1a";
    run(protocol, anomaly);

    log::info!("Starting {} anomaly check", anomaly);

    let cores = core_affinity::get_core_ids().unwrap().len(); // always use max cores available

    for i in 0..cores {
        let file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, i);
        let fh = File::open(&file).unwrap();
        log::info!("Checking file: {}", file);

        let reader = BufReader::new(fh);

        for line in reader.lines() {
            if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
                let version = resp
                    .get_values()
                    .unwrap()
                    .get("version")
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                assert_eq!(version, 1, "expected: {}, actual: {}", 1, version);
            }
        }
    }
    log::info!("{} anomaly check complete", anomaly);
}
