use config::Config;
use serde_json::json;

use std::fs::OpenOptions;
use std::time::Duration;
use std::time::Instant;

use super::local::LocalStatistics;

#[derive(Debug)]
pub struct GlobalStatistics {
    scale_factor: u64,
    protocol: String,
    workload: String,
    cores: u64,
    txn_per_core: u64,
    start: Option<Instant>,
    end: Option<Duration>,
    total_time: u128,
    commits: u64,
    aborts: u64,
    logic_aborts: u64,
    commit_aborts: u64,
    write_cf: u64,
    read_cf: u64,
    rwrite_cf: u64,
    read_ca: u64,
    write_ca: u64,
    not_found: u64,
    tx: u128,
    commit: u128,
    wait_manager: u128,
    latency: u128,
    edges_inserted: u64,
    conflict_detected: u64,
    rw_conflict_detected: u64,
    wr_conflict_detected: u64,
    ww_conflict_detected: u64,

    txn_not_found_cum: u128,
    txn_commit_cum: u128,
    txn_commit_abort_cum: u128,
    txn_logic_abort_cum: u128,

    retries: u64,
    cum_retries: u64,
    max_retries: u64,

    max_txn_logic_abort: u128,
    max_txn_commit_abort: u128,

    pub aborted_latency: Vec<u64>,

    g0: u64,
    g1: u64,
    g2: u64,

    path_len: usize,
}

impl GlobalStatistics {
    pub fn new(config: &Config) -> Self {
        let scale_factor = config.get_int("scale_factor").unwrap() as u64;
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let cores = config.get_int("cores").unwrap() as u64;
        let txn_per_core = config.get_int("transactions").unwrap() as u64;

        GlobalStatistics {
            scale_factor,
            protocol,
            workload,
            cores,
            txn_per_core,
            start: None,
            end: None,
            total_time: 0,
            commits: 0,
            aborts: 0,
            logic_aborts: 0,
            commit_aborts: 0,
            write_cf: 0,
            read_cf: 0,
            read_ca: 0,
            rwrite_cf: 0,
            write_ca: 0,
            not_found: 0,
            tx: 0,
            commit: 0,
            wait_manager: 0,
            latency: 0,
            edges_inserted: 0,
            conflict_detected: 0,
            rw_conflict_detected: 0,
            wr_conflict_detected: 0,
            ww_conflict_detected: 0,

            txn_not_found_cum: 0,
            txn_commit_cum: 0,
            txn_commit_abort_cum: 0,
            txn_logic_abort_cum: 0,

            retries: 0,
            cum_retries: 0,
            max_retries: 0,

            max_txn_logic_abort: 0,
            max_txn_commit_abort: 0,
            aborted_latency: Vec::new(),

            g0: 0,
            g1: 0,
            g2: 0,

            path_len: 0,
        }
    }

    pub fn inc_cores(&mut self) {
        self.cores += 1;
    }

    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    pub fn end(&mut self) {
        self.end = Some(self.start.unwrap().elapsed());
    }

    pub fn get_runtime(&self) -> u64 {
        match self.end {
            Some(elasped) => elasped.as_millis() as u64,
            None => 0,
        }
    }

    pub fn validate(&self) {
        let completed = self.commits + self.not_found;
        let expected = self.cores * self.txn_per_core;

        assert!(
            completed == expected,
            "completed: {}, expected: {}",
            completed,
            expected
        );
    }

    pub fn merge(&mut self, mut local: LocalStatistics) {
        self.total_time += local.get_worker_cum();
        self.commits += local.get_commits();
        self.aborts += local.get_aborts();
        self.commit_aborts += local.get_commit_aborts();
        self.logic_aborts += local.get_logic_aborts();
        self.not_found += local.get_not_found();
        self.tx += local.get_tx_cum();
        self.commit += local.get_commit_cum();
        self.wait_manager += local.get_wait_manager_cum();
        self.latency += local.get_latency_cum();
        self.read_cf += local.get_read_cf();
        self.write_cf += local.get_write_cf();
        self.read_ca += local.get_read_ca();
        self.write_ca += local.get_write_ca();
        self.rwrite_cf += local.get_rwrite_cf();
        self.edges_inserted += local.get_edges_inserted();
        self.conflict_detected += local.get_conflict_detected();
        self.rw_conflict_detected += local.get_rw_conflict_detected();
        self.wr_conflict_detected += local.get_wr_conflict_detected();
        self.ww_conflict_detected += local.get_ww_conflict_detected();

        self.txn_not_found_cum += local.get_txn_not_found();
        self.txn_commit_cum += local.get_txn_commit();
        self.txn_commit_abort_cum += local.get_txn_commit_abort();
        self.txn_logic_abort_cum += local.get_txn_logic_abort();

        self.retries += local.get_retries();
        self.cum_retries += local.get_cum_retries();

        if self.max_retries < local.get_max_retries() {
            self.max_retries = local.get_max_retries();
        }

        if self.max_txn_logic_abort < local.get_max_txn_logic_abor() {
            self.max_txn_logic_abort = local.get_max_txn_logic_abor();
        }

        if self.max_txn_commit_abort < local.get_max_txn_commit_abor() {
            self.max_txn_commit_abort = local.get_max_txn_commit_abor();
        }

        self.aborted_latency.append(&mut local.aborted_latency);

        self.g0 += local.get_g0();
        self.g1 += local.get_g1();
        self.g2 += local.get_g2();

        self.path_len += local.get_path_len();
    }

    fn get_total_time(&self) -> u64 {
        (self.total_time / 1000000) as u64
    }

    fn get_txn_time(&self) -> u64 {
        (self.tx / 1000000) as u64
    }

    fn get_wait_time(&self) -> u64 {
        (self.wait_manager / 1000000) as u64
    }

    fn get_commit_time(&self) -> u64 {
        (self.commit / 1000000) as u64
    }

    fn get_latency(&self) -> u64 {
        (self.latency / 1000000) as u64
    }

    fn get_thpt(&self) -> f64 {
        let total = (self.commits + self.not_found) as f64;
        total / (self.get_runtime() as f64 / 1000.0)
    }

    fn get_abr(&self) -> f64 {
        (self.aborts as f64 / (self.commits + self.not_found + self.aborts) as f64) * 100.0
    }

    pub fn print_to_console(&mut self) {
        let pr = json!({
            "workload": self.workload,
            "sf": self.scale_factor,
            "cores": self.cores,
            "protocol": self.protocol,
            "runtime (ms)": self.get_runtime(),
            "cum_runtime (ms)":  self.get_total_time(),
            "commits": self.commits,
            "aborts": self.aborts,
            "not_found": self.not_found as u64,
            "txn_time (ms)": self.get_txn_time(),
            "commit_time (ms)": self.get_commit_time(),
            "wait_time (ms)": self.get_wait_time(),
            "latency (ms)": self.get_latency(),
            "thpt": format!("{:.2}",self.get_thpt()),
            "abr": format!("{:.2}",self.get_abr()),
            "conflicts deteted": self.conflict_detected,
            "   rw": self.rw_conflict_detected,
            "   wr": self.wr_conflict_detected,
            "   ww": self.ww_conflict_detected,
            "cycles found": (self.g0 + self.g1 + self.g2),
            "   g0": self.g0,
            "   g1": self.g1,
            "   g2": self.g2,
            "path len": (self.path_len),
            "av. path len": (self.path_len as f64 / (self.g0 + self.g1 + self.g2)as f64  ),
        });

        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());
    }

    pub fn write_to_file(&mut self, config: &Config) {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open("./results.csv")
            .unwrap();

        let mut wtr = csv::Writer::from_writer(file);

        let theta = config.get_float("theta").unwrap();
        let serializable_rate = config.get_float("serializable_rate").unwrap();
        let update_rate = config.get_float("update_rate").unwrap();
        let cycle_check_strategy = config.get_str("dfs").unwrap();

        wtr.serialize((
            // parameters
            self.scale_factor,
            &self.protocol,
            &self.workload,
            self.cores,
            theta,
            serializable_rate,
            update_rate,
            cycle_check_strategy,
            // raw stats
            self.get_runtime(),
            self.commits,
            self.aborts,
            self.not_found as u64,
            self.get_txn_time(),
            self.get_commit_time(),
            self.get_wait_time(),
            self.get_latency(),
        ))
        .unwrap();
    }
}
