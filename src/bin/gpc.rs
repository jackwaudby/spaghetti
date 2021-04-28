use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::gpc::helper;

use clap::clap_app;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let matches = clap_app!(spag =>
    (version: "0.1.0")
    (author: "j. waudby <j.waudby2@newcastle.ac.uk>")
    (about: "spaghetti")
    (@arg WORKLOAD: -w --workload +takes_value "Set a workload")
    (@arg PROTOCOL: -p --port +takes_value "Set a protocol")
    (@arg SF: -s --scalefactor +takes_value "Set a scale factor")
    (@arg TRANSACTIONS: -t --transactions +takes_value "Transactions per core")
    (@arg CORES: -c --cores +takes_value "Number of cores to use")
    )
    .get_matches();

    let mut settings = helper::init_config("Embedded.toml"); // init config

    // For each flag overwrite default with any supplied runtime value.
    if let Some(w) = matches.value_of("WORKLOAD") {
        settings.set("workload", w).unwrap();
    }

    if let Some(p) = matches.value_of("PROTOCOL") {
        settings.set("protocol", p).unwrap();
    }

    if let Some(s) = matches.value_of("SF") {
        settings.set("scale_factor", s).unwrap();
    }

    if let Some(t) = matches.value_of("TRANSACTIONS") {
        settings.set("transactions", t).unwrap();
    }

    if let Some(c) = matches.value_of("CORES") {
        settings.set("workers", c).unwrap();
    }

    let config = Arc::new(settings);

    helper::set_log_level(Arc::clone(&config)); // set log level

    helper::create_results_dir(Arc::clone(&config)); // create results dir

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
    helper::run(workers, scheduler, Arc::clone(&config), tx);
    global_stats.end();
    tracing::info!("Execution finished");

    if config.get_str("workload").unwrap().as_str() == "acid" {
        // run recon thread
    }

    tracing::info!("Collecting statistics..");
    while let Ok(local_stats) = rx.recv() {
        global_stats.merge_into(local_stats);
    }
    global_stats.write_to_file();
}
