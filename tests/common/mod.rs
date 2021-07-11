use spaghetti::common::message::{InternalResponse, Message};
use spaghetti::common::message::{Parameters, Transaction};
use spaghetti::common::statistics::{GlobalStatistics, LocalStatistics};
use spaghetti::common::utils;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::datatype::SuccessMessage;
use spaghetti::storage::Database;
use spaghetti::workloads::acid::paramgen::{
    AcidTransactionProfile, G0Read, G2itemRead, LostUpdateRead,
};
use spaghetti::workloads::acid::{AcidTransaction, ACID_SF_MAP};
use spaghetti::workloads::IsolationLevel;

use config::Config;
use crossbeam_utils::thread;
use lazy_static::lazy_static;
use log::info;
use petgraph::algo;
use petgraph::graph::Graph;
use std::fs::{self, File, OpenOptions};
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::sync::mpsc;
use std::time::Instant;

lazy_static! {
    static ref CORES: i64 = core_affinity::get_core_ids().unwrap().len() as i64;
    static ref PERSONS: i64 = {
        let sf = 1;             // TODO: receive from configuration.
        *ACID_SF_MAP.get(&sf).unwrap() as i64
    };
}

pub fn setup_config(protocol: &str, anomaly: &str) -> Config {
    let mut c = utils::init_config("./tests/Acid.toml");
    c.set("cores", *CORES).unwrap();
    c.set("protocol", protocol).unwrap();
    c.set("anomaly", anomaly).unwrap();
    c
}

pub fn run_recon(
    config: &Config,
    database: &Database,
    scheduler: &Scheduler,
    tx: mpsc::Sender<LocalStatistics>,
) {
    let protocol = config.get_str("protocol").unwrap();
    let workload = config.get_str("workload").unwrap();
    let anomaly = config.get_str("anomaly").unwrap();
    let sf = config.get_int("scale_factor").unwrap() as u64;

    let persons = *ACID_SF_MAP.get(&sf).unwrap();
    let mut stats = LocalStatistics::new(0, &workload, &protocol);

    thread::scope(|s| {
        let scheduler = &scheduler;
        let database = &database;

        s.builder()
            .name("0".to_string())
            .spawn(move |_| {
                let dir = format!("./log/{}/{}/{}/", workload, protocol, anomaly);
                let file = format!("./log/acid/{}/{}/thread-recon.json", protocol, anomaly);

                if Path::new(&dir).exists() {
                    if Path::new(&file).exists() {
                        fs::remove_file(&file).unwrap(); // remove file if already exists
                    }
                } else {
                    panic!("dir: {} should exist", dir);
                }

                let mut fh = Some(
                    OpenOptions::new()
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(&file)
                        .expect("cannot open file"),
                );

                match anomaly.as_str() {
                    "g0" => {
                        info!("Executing {} recon queries", anomaly);

                        for p1_id in (0..persons).step_by(2) {
                            let p2_id = p1_id + 1;
                            let payload = G0Read { p1_id, p2_id };

                            let txn = Message::Request {
                                request_no: 0, // TODO
                                transaction: Transaction::Acid(AcidTransaction::G0Read),
                                parameters: Parameters::Acid(AcidTransactionProfile::G0Read(
                                    payload,
                                )),
                                isolation: IsolationLevel::Serializable,
                            };

                            let ir = utils::execute(txn, scheduler, database); // execute txn
                            let InternalResponse {
                                transaction,
                                outcome,
                                request_no,
                            } = ir;

                            utils::log_result(&mut fh, outcome.clone(), request_no); // log result
                            stats.record(transaction, outcome.clone());
                            // record txn
                        }
                    }
                    "lu" => {
                        info!("Executing {} recon queries", anomaly);

                        for p_id in 0..persons {
                            let payload = LostUpdateRead { p_id };

                            let txn = Message::Request {
                                request_no: 0, // TODO
                                transaction: Transaction::Acid(AcidTransaction::LostUpdateRead),
                                parameters: Parameters::Acid(
                                    AcidTransactionProfile::LostUpdateRead(payload),
                                ),
                                isolation: IsolationLevel::Serializable,
                            };

                            let ir = utils::execute(txn, scheduler, database); // execute txn
                            let InternalResponse {
                                transaction,
                                outcome,
                                request_no,
                            } = ir;
                            utils::log_result(&mut fh, outcome.clone(), request_no); // log result
                            stats.record(transaction, outcome.clone());
                        }
                    }
                    "g2item" => {
                        info!("Executing {} recon queries", anomaly);

                        let p = persons;
                        for p_id in (0..p).step_by(2) {
                            let payload = G2itemRead {
                                p1_id: p_id,
                                p2_id: p_id + 1,
                            };

                            let txn = Message::Request {
                                request_no: 0, // TODO
                                transaction: Transaction::Acid(AcidTransaction::G2itemRead),
                                parameters: Parameters::Acid(AcidTransactionProfile::G2itemRead(
                                    payload,
                                )),
                                isolation: IsolationLevel::Serializable,
                            };

                            let ir = utils::execute(txn, scheduler, database); // execute txn
                            let InternalResponse {
                                transaction,
                                outcome,
                                request_no,
                            } = ir;
                            utils::log_result(&mut fh, outcome.clone(), request_no); // log result
                            stats.record(transaction, outcome.clone());
                            // record txn
                        }
                    }
                    _ => info!("No recon queries for {}", anomaly),
                }
                tx.send(stats).unwrap();
            })
            .unwrap();
    })
    .unwrap();
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

    run_recon(&config, &database, &scheduler, tx.clone());

    drop(tx);
    global_stats.end();
    info!("Execution finished");
    info!("Collecting statistics..");
    while let Ok(local_stats) = rx.recv() {
        global_stats.merge_into(local_stats);
    }
    global_stats.write_to_file();
}

/// Dirty Write (G0).
///
/// # Anomaly check
///
/// For each person pair in the test graph:
/// (i) TODO: prune each versionHistory list to remove any version numbers that do not appear in all lists,
/// (ii) perform an element-wise comparison between versionHistory lists for each entity,
/// (iii) If lists do not agree a G0 anomaly has occurred.
pub fn g0(protocol: &str) {
    let anomaly = "g0";

    run(protocol, anomaly);

    let fh = File::open(format!(
        "./log/acid/{}/{}/thread-recon.json",
        protocol, anomaly
    ))
    .unwrap();
    let reader = BufReader::new(fh);

    info!("Start {} anomaly check", anomaly);
    for line in reader.lines() {
        let resp: SuccessMessage = serde_json::from_str(&line.unwrap()).unwrap();

        if let Some(vals) = resp.get_values() {
            let p1vh = vals.get("p1_version_history").unwrap().clone();
            let p2vh = vals.get("p2_version_history").unwrap().clone();

            // TODO: implement (i)
            let x = string_to_vec64(p1vh);
            let y = string_to_vec64(p2vh);

            assert_eq!(x, y);
        }
    }
    info!("{} anomaly check complete", anomaly);
}

fn string_to_vec64(mut s: String) -> Vec<u64> {
    s.retain(|c| !c.is_whitespace());
    s.remove(0);
    s.pop();
    let res: Vec<u64> = s.split(",").map(|x| x.parse::<u64>().unwrap()).collect();
    res
}

/// Aborted Read (G1a).
///
/// # Anomaly check
///
/// Transactions write version = 2 but then abort. Each read should return version=1. Otherwise, a G1a anomaly has occurred.
pub fn g1a(protocol: &str) {
    let anomaly = "g1a";
    run(protocol, anomaly);

    info!("Starting {} anomaly check", anomaly);
    for i in 0..*CORES {
        let file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, i);
        let fh = File::open(&file).unwrap();
        info!("Checking file: {}", file);

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
    info!("{} anomaly check complete", anomaly);
}

/// Circular information flow (G1c).
///
/// # Anomaly check
///
/// From result tuples (transaction_id, version_read) build a dependency graph;
/// Each tuple represents a directed wr edge: (version_read)  --> (transaction_id).
/// The resulting graph should be acyclic.
pub fn g1c(protocol: &str) {
    let anomaly = "g1c";
    run(protocol, anomaly);

    info!("Starting {} anomaly check", anomaly);
    info!("Initialise dependency graph");
    let mut graph = Graph::<u64, (), petgraph::Directed>::new(); // directed and unlabeled

    for i in 0..*CORES {
        let file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, i);
        let fh = File::open(&file).unwrap();
        info!("Adding {} to graph", file);
        let reader = BufReader::new(fh);

        for line in reader.lines() {
            if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
                let values = resp.get_values().unwrap(); // (transaction_id, version_id) = (version_id/tb) --wr--> (transaction_id/ta)
                let transaction_id = values
                    .get("transaction_id")
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                let version_read = values.get("version").unwrap().parse::<u64>().unwrap();

                let a = match graph.node_indices().find(|i| graph[*i] == transaction_id) {
                    Some(node_index) => node_index, // ta already exists in the graph; get index
                    None => graph.add_node(transaction_id), // insert ta; get index
                };

                match graph.node_indices().find(|i| graph[*i] == version_read) {
                    Some(b) => {
                        graph.add_edge(b, a, ()); // tb already exists; add edge
                    }

                    None => {
                        let b = graph.add_node(version_read);
                        graph.add_edge(b, a, ()); // insert tb; add edge
                    }
                }
            }
        }
    }

    info!("Added {} nodes", graph.node_count());
    info!("Added {} edges", graph.edge_count());
    info!("Checking for cycles...");
    assert_eq!(algo::is_cyclic_directed(&graph), false);

    info!("{} anomaly check complete", anomaly);
}

/// Item-many-preceders (IMP)
///
/// # Anomaly check
///
/// The first version read of a data item should be equal to the second version read.
pub fn imp(protocol: &str) {
    let anomaly = "imp";
    run(protocol, anomaly);

    info!("Starting {} anomaly check", anomaly);
    for i in 0..*CORES {
        let file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, i);
        let fh = File::open(&file).unwrap();
        info!("Checking file: {}", file);
        let reader = BufReader::new(fh);
        for line in reader.lines() {
            if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
                if let Some(vals) = resp.get_values() {
                    let first = vals.get("first_read").unwrap().parse::<u64>().unwrap();
                    let second = vals.get("second_read").unwrap().parse::<u64>().unwrap();
                    assert_eq!(first, second, "first: {}, second: {}", first, second);
                }
            }
        }
    }
    info!("{} anomaly check complete", anomaly);
}

/// Observed Transaction Vanishes (OTV)
///
/// # Anomaly check
///
/// The list of versions read should be monontically increasing.
/// Valid; 4, 5, 5, 6
/// Invalid: 4, 3, 4, 5
/// Once a version has been observed each subsequent version read should be equal or more recent.
pub fn otv(protocol: &str) {
    let anomaly = "otv";
    run(protocol, anomaly);

    info!("Starting {} anomaly check", anomaly);
    for i in 0..*CORES {
        let file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, i);
        let fh = File::open(&file).unwrap();
        info!("Checking file: {}", file);
        let reader = BufReader::new(fh);
        for line in reader.lines() {
            if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
                if let Some(vals) = resp.get_values() {
                    let p1 = vals.get("p1_version").unwrap().parse::<u64>().unwrap();
                    let p2 = vals.get("p2_version").unwrap().parse::<u64>().unwrap();
                    let p3 = vals.get("p3_version").unwrap().parse::<u64>().unwrap();
                    let p4 = vals.get("p4_version").unwrap().parse::<u64>().unwrap();
                    let reads = vec![p1, p2, p3, p4];

                    for i in 0..3 {
                        assert!(reads[i] <= reads[i + 1], "{} > {}", reads[i], reads[i + 1]);
                    }
                }
            }
        }
    }
    info!("{} anomaly check complete", anomaly);
}

/// Fractured Read (FR)
///
/// # Anomaly check
///
/// The list of versions read should consistent within a transaction.
/// Valid; 5, 5, 5, 5
/// Invalid: 4, 4, 4, 5
/// The first version should equal all versions.
pub fn fr(protocol: &str) {
    let anomaly = "fr";
    run(protocol, anomaly);

    info!("Starting {} anomaly check", anomaly);
    for i in 0..*CORES {
        let file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, i);
        let fh = File::open(&file).unwrap();
        info!("Checking file: {}", file);
        let reader = BufReader::new(fh);
        for line in reader.lines() {
            if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
                // get read transaction responses
                if let Some(vals) = resp.get_values() {
                    let p1 = vals.get("p1_version").unwrap().parse::<u64>().unwrap();
                    let p2 = vals.get("p2_version").unwrap().parse::<u64>().unwrap();
                    let p3 = vals.get("p3_version").unwrap().parse::<u64>().unwrap();
                    let p4 = vals.get("p4_version").unwrap().parse::<u64>().unwrap();
                    let reads = vec![p1, p2, p3, p4];

                    for i in 0..3 {
                        assert!(reads[i] == reads[i + 1], "{} != {}", reads[i], reads[i + 1]);
                    }
                }
            }
        }
    }
    info!("{} anomaly check complete", anomaly);
}

/// Lost Update (LU)
///
/// # Anomaly check
///
/// TODO
pub fn lu(protocol: &str) {
    let anomaly = "lu";
    run(protocol, anomaly);

    info!("Starting {} anomaly check", anomaly);
    let mut expected = vec![]; // calculate expected number from # of commits
    for _ in 0..*PERSONS {
        expected.push(0);
    }

    for i in 0..*CORES {
        let file = format!("./log/acid/{}/{}/thread-{}.json", protocol, anomaly, i);
        let fh = File::open(&file).unwrap();
        info!("Checking file: {}", file);

        let reader = BufReader::new(fh);

        for line in reader.lines() {
            if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
                if let Some(vec) = resp.get_updated() {
                    let (_, p_id) = vec[0];
                    expected[p_id as usize] += 1;
                }
            }
        }
    }

    let file = format!("./log/acid/{}/{}/thread-recon.json", protocol, anomaly);
    info!("Checking file: {}", file);
    let fh = File::open(&file).unwrap();
    let reader = BufReader::new(fh);
    for line in reader.lines() {
        if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
            if let Some(vals) = resp.get_values() {
                let p_id = vals.get("p_id").unwrap().parse::<u64>().unwrap() as usize;
                let nf = vals.get("num_friends").unwrap().parse::<u64>().unwrap();
                info!(
                    "person {}: expected: {}, actual: {}",
                    p_id, expected[p_id], nf
                );

                assert_eq!(
                    expected[p_id], nf,
                    "expected: {}, actual: {}",
                    expected[p_id], nf
                );
            }
        }
    }
    info!("{} anomaly check complete", anomaly);
}

/// Write Skew (G2item)
///
/// # Anomaly check
///
///
pub fn g2item(protocol: &str) {
    let anomaly = "g2item";
    run(protocol, anomaly);

    info!("Starting {} anomaly check", anomaly);
    let file = format!("./log/acid/{}/{}/thread-recon.json", protocol, anomaly);
    let fh = File::open(&file).unwrap();
    info!("Checking file: {}", file);

    let reader = BufReader::new(fh);

    for line in reader.lines() {
        if let Ok(resp) = serde_json::from_str::<SuccessMessage>(&line.unwrap()) {
            // get read transaction responses
            if let Some(vals) = resp.get_values() {
                let p1 = vals.get("p1_value").unwrap().parse::<i64>().unwrap();
                let p2 = vals.get("p2_value").unwrap().parse::<i64>().unwrap();

                assert!(p1 + p2 > 0, "p1: {}, p2: {}", p1, p2);
            }
        }
    }
    info!("{} anomaly check complete", anomaly);
}
