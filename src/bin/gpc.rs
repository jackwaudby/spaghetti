use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::gpc::coordinator;
use spaghetti::gpc::helper;

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let config = helper::init_config("Embedded.toml"); // init config

    helper::set_log_level(Arc::clone(&config)); // set log level

    let mut global_stats = GlobalStatistics::new(Arc::clone(&config)); // init stats

    if config.get_str("workload").unwrap().as_str() == "acid" {
        let anomaly = config.get_str("anomaly").unwrap();
        let delay = config.get_int("delay").unwrap();
        tracing::info!("ACID test: {}", anomaly);
        tracing::info!("Aritifical operation delay (secs): {}", delay);
    }

    let dg_start = Instant::now(); // init database
    let workload = helper::init_database(Arc::clone(&config));
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    let workers = config.get_int("workers").unwrap() as usize;
    let scheduler = helper::init_scheduler(workload, workers); // init scheduler
    let (tx, rx) = mpsc::channel(); // channel to send statistics

    tracing::info!("Starting execution");
    global_stats.start();
    coordinator::run(workers, scheduler, config, tx);
    global_stats.end();
    tracing::info!("Execution finished");

    // TODO: ACID recon queries
    // TODO: logging

    tracing::info!("Collecting statistics..");
    while let Ok(local_stats) = rx.recv() {
        global_stats.merge_into(local_stats);
    }
    global_stats.write_to_file();
}
