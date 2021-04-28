use spaghetti::gpc::helper;

use config::Config;
use spaghetti::common::statistics::GlobalStatistics;
use std::sync::Arc;
use std::time::Instant;
use test_env_log::test;

fn setup_config() -> Arc<Config> {
    let mut c = Config::default();
    c.merge(config::File::with_name("./tests/Test-basic-sgt.toml"))
        .unwrap();
    Arc::new(c)
}

#[test]
fn basic_sgt_integration_test() {
    let config = setup_config(); // init config
    let mut global_stats = GlobalStatistics::new(Arc::clone(&config)); // init stats

    log::info!("Initialise database");
    let dg_start = Instant::now(); // init database
    let workload = helper::init_database(Arc::clone(&config));
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    let workers = config.get_int("workers").unwrap() as usize;
    let scheduler = helper::init_scheduler(workload, workers); // init scheduler
    let (tx, rx) = std::sync::mpsc::channel(); // channel to send statistics

    log::info!("Start execution");
    global_stats.start();
    helper::run(workers, scheduler, config, tx);
    global_stats.end();
    log::info!("Execution finished");

    log::info!("Collecting statistics..");
    while let Ok(local_stats) = rx.recv() {
        global_stats.merge_into(local_stats);
    }
    global_stats.write_to_file();
}
