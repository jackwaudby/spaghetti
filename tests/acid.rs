use spaghetti::common::message::InternalResponse;
use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::statistics::LocalStatistics;
use spaghetti::embedded::generator::{self, Generator, InternalRequest};
use spaghetti::embedded::logging::{self, Logger};
use spaghetti::embedded::manager::{self, TransactionManager};
use spaghetti::server::storage::datatype::Response;
use spaghetti::workloads::Workload;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::time::Instant;
use test_env_log::test;

/// Set up configuration for ACID test suite.
fn setup_config(protocol: &str) -> Arc<Config> {
    let mut c = Config::default();
    c.merge(config::File::with_name("./tests/Test-acid.toml"))
        .unwrap();
    c.set("protocol", protocol).unwrap();
    Arc::new(c)
}

/// Run embedded mode.
fn run(config: Arc<Config>) {
    let mut global_stats = GlobalStatistics::new(Arc::clone(&config)); // init global stats

    // Workload
    let dg_start = Instant::now();
    let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());
    let mut rng: StdRng = SeedableRng::from_entropy();
    workload.populate_tables(&mut rng).unwrap();
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    global_stats.start();

    // Pipes
    let (req_tx, req_rx): (SyncSender<InternalRequest>, Receiver<InternalRequest>) =
        std::sync::mpsc::sync_channel(32);

    let (resp_tx, resp_rx): (SyncSender<InternalResponse>, Receiver<InternalResponse>) =
        std::sync::mpsc::sync_channel(32);

    let (main_tx, main_rx): (SyncSender<LocalStatistics>, Receiver<LocalStatistics>) =
        std::sync::mpsc::sync_channel(32);

    let (next_tx, next_rx): (SyncSender<()>, Receiver<()>) = std::sync::mpsc::sync_channel(32);

    // Generator
    let g = Generator::new(req_tx, resp_tx, next_rx);
    generator::run(g, Arc::clone(&config));

    // Logger.
    let protocol = config.get_str("protocol").unwrap();
    let w = config.get_str("workload").unwrap();
    let warmup = config.get_int("warmup").unwrap() as u32;
    let stats = Some(LocalStatistics::new(1, &w, &protocol));
    let logger = Logger::new(resp_rx, main_tx, stats, warmup);
    logging::run(logger, Arc::clone(&config));

    // Manager.
    let tm = TransactionManager::new(Arc::clone(&workload), req_rx, next_tx);
    manager::run(tm);

    let local_stats = main_rx.recv().unwrap();
    global_stats.merge_into(local_stats);
    global_stats.end();
    global_stats.write_to_file();
}

#[test]
fn acid_test_sgt() {
    let config = setup_config("sgt");
    run(config);

    let f = format!("./log/acid/sgt/g1a.json");
    let file = File::open(f).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let resp: Response = serde_json::from_str(&line.unwrap()).unwrap();
        let version = resp.val.get("version").unwrap().parse::<u64>().unwrap();
        assert_eq!(version, 1, "expected: {}, actual: {}", version, 1);
    }
}

#[test]
fn acid_test_2pl() {
    let config = setup_config("2pl");
    run(config);

    let f = format!("./log/acid/2pl/g1a.json");
    let file = File::open(f).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let resp: Response = serde_json::from_str(&line.unwrap()).unwrap();
        let version = resp.val.get("version").unwrap().parse::<u64>().unwrap();
        assert_eq!(version, 1, "expected: {}, actual: {}", version, 1);
    }
}

#[test]
fn acid_test_hit() {
    let config = setup_config("hit");
    run(config);

    let f = format!("./log/acid/hit/g1a.json");
    let file = File::open(f).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let resp: Response = serde_json::from_str(&line.unwrap()).unwrap();
        let version = resp.val.get("version").unwrap().parse::<u64>().unwrap();
        assert_eq!(version, 1, "expected: {}, actual: {}", version, 1);
    }
}

#[test]
fn acid_test_opt_hit() {
    let config = setup_config("opt-hit");
    run(config);

    let f = format!("./log/acid/opt-hit/g1a.json");
    let file = File::open(f).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let resp: Response = serde_json::from_str(&line.unwrap()).unwrap();
        let version = resp.val.get("version").unwrap().parse::<u64>().unwrap();
        assert_eq!(version, 1, "expected: {}, actual: {}", version, 1);
    }
}
