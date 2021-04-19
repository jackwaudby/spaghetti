use spaghetti::common::message::InternalResponse;
use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::statistics::LocalStatistics;
use spaghetti::embedded::generator::{self, Generator, InternalRequest};
use spaghetti::embedded::logging::{self, Logger};
use spaghetti::embedded::manager::{self, TransactionManager};
use spaghetti::server::storage::datatype::SuccessMessage;
use spaghetti::workloads::acid::ACID_SF_MAP;
use spaghetti::workloads::Workload;

use config::Config;
use petgraph::algo;
use petgraph::graph::Graph;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::time::Instant;

/// Set up configuration for ACID test suite.
pub fn setup_config(protocol: &str, anomaly: &str) -> Arc<Config> {
    let mut c = Config::default();
    c.merge(config::File::with_name("./tests/Test-acid.toml"))
        .unwrap();
    c.set("protocol", protocol).unwrap();
    c.set("anomaly", anomaly).unwrap();
    Arc::new(c)
}

/// Run embedded mode.
pub fn run(config: Arc<Config>) {
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

    if w.as_str() == "acid" {
        let anomaly = config.get_str("anomaly").unwrap();
        let delay = config.get_int("delay").unwrap();
        tracing::info!("ACID test: {}", anomaly);
        tracing::info!("Aritifical operation delay: {} (secs)", delay);
    }

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

/// Aborted Read (G1a).
///
/// # Anomaly check
///
/// Transactions write version=2 but then abort.
/// Each read should return version=1.
/// Otherwise, a G1a anomaly has occurred.
pub fn g1a(protocol: &str) {
    let anomaly = "g1a";
    let config = setup_config(protocol, anomaly);

    run(config);

    let fh = File::open(format!("./log/acid/{}/{}.json", protocol, anomaly)).unwrap();
    let reader = BufReader::new(fh);

    for line in reader.lines() {
        let resp: SuccessMessage = serde_json::from_str(&line.unwrap()).unwrap();
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

/// Dirty Write (G0).
///
/// # Anomaly check
///
///
pub fn g0(protocol: &str) {
    let anomaly = "g0";
    let config = setup_config(protocol, anomaly);

    run(config);

    let fh = File::open(format!("./log/acid/{}/{}.json", protocol, anomaly)).unwrap();
    let reader = BufReader::new(fh);

    for line in reader.lines() {
        let resp: SuccessMessage = serde_json::from_str(&line.unwrap()).unwrap();
        log::info!("{:?}", resp);
    }
}

/// Circular information flow (G1c).
///
/// # Anomaly check
///
/// From result tuples (transaction_id, version_read) build a dependency graph;
/// Each tuple represents a directed wr edge (transaction_id) --> (version_read).
/// The resulting graph should be acyclic.
pub fn g1c(protocol: &str) {
    let anomaly = "g1c";
    let config = setup_config(protocol, anomaly);

    run(config);

    let file = format!("./log/acid/{}/{}.json", protocol, anomaly);
    let fh = match File::open(file.clone()) {
        Ok(fh) => fh,
        Err(_) => panic!("file: {} not found", file),
    };
    let reader = BufReader::new(fh);

    let mut graph = Graph::<u64, (), petgraph::Directed>::new(); // directed and unlabeled

    for line in reader.lines() {
        let resp: SuccessMessage = serde_json::from_str(&line.unwrap()).unwrap();
        let values = resp.get_values().unwrap(); // (transaction_id, version_id) = (version_id/tb) --wr--> (transaction_id/ta)
        let transaction_id = values
            .get("transaction_id")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let version_read = values.get("version").unwrap().parse::<u64>().unwrap();

        let a = match graph.node_indices().find(|i| graph[*i] == transaction_id) {
            // ta already exists in the graph; get index
            Some(node_index) => node_index,
            // insert ta; get index
            None => graph.add_node(transaction_id),
        };

        match graph.node_indices().find(|i| graph[*i] == version_read) {
            // tb already exists; add edge
            Some(b) => {
                graph.add_edge(b, a, ());
            }
            // insert tb; add edge
            None => {
                let b = graph.add_node(version_read);
                graph.add_edge(b, a, ());
            }
        }
    }
    assert_eq!(algo::is_cyclic_directed(&graph), false);
}

/// IMP
///
/// # Anomaly check
///
///
///
///
pub fn imp(protocol: &str) {
    let anomaly = "imp";
    let config = setup_config(protocol, anomaly);

    run(config);

    let fh = File::open(format!("./log/acid/{}/{}.json", protocol, anomaly)).unwrap();
    let reader = BufReader::new(fh);

    for line in reader.lines() {
        let resp: SuccessMessage = serde_json::from_str(&line.unwrap()).unwrap();
        if let Some(vals) = resp.get_values() {
            let first = vals.get("first_read").unwrap().parse::<u64>().unwrap();
            let second = vals.get("second_read").unwrap().parse::<u64>().unwrap();
            assert_eq!(first, second, "first: {}, second: {}", first, second);
        }
    }
}

pub fn lu(protocol: &str) {
    let anomaly = "lu";
    let config = setup_config(protocol, anomaly);
    let sf = config.get_int("scale_factor").unwrap() as u64;

    run(config);

    let fh = File::open(format!("./log/acid/{}/{}.json", protocol, anomaly)).unwrap();
    let reader = BufReader::new(fh);

    let persons = *ACID_SF_MAP.get(&sf).unwrap();

    let mut expected = vec![];
    for _ in 0..persons {
        expected.push(0);
    }

    for line in reader.lines() {
        let resp: SuccessMessage = serde_json::from_str(&line.unwrap()).unwrap();
        if let Some(p_id) = resp.get_updated() {
            expected[p_id as usize] += 1;
        }

        if let Some(vals) = resp.get_values() {
            let p_id = vals.get("p_id").unwrap().parse::<u64>().unwrap() as usize;
            let nf = vals.get("num_friends").unwrap().parse::<u64>().unwrap();

            assert_eq!(
                expected[p_id], nf,
                "expected: {}, actual: {}",
                expected[p_id], nf
            );
        }
    }
}