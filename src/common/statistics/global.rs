use config::Config;
use serde_json::json;

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
    ww_conflict_detected: u64,

    txn_not_found_cum: u128,
    txn_commit_cum: u128,
    txn_commit_abort_cum: u128,
    txn_logic_abort_cum: u128,

    retries: u64,
    cum_retries: u64,
    max_retries: u64,

    max_txn_logic_abort: u128,
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
            ww_conflict_detected: 0,

            txn_not_found_cum: 0,
            txn_commit_cum: 0,
            txn_commit_abort_cum: 0,
            txn_logic_abort_cum: 0,

            retries: 0,
            cum_retries: 0,
            max_retries: 0,

            max_txn_logic_abort: 0,
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

    pub fn get_runtime(&mut self) -> u64 {
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

    pub fn merge(&mut self, local: LocalStatistics) {
        self.total_time += local.get_worker_cum();
        self.commits += local.get_commits();
        self.aborts += local.get_aborts();
        self.commit_aborts += local.get_commit_aborts();
        self.logic_aborts += local.get_logic_aborts();
        self.not_found += local.get_not_found();
        // self.tx += local.get_tx_cum();
        self.commit += local.get_commit_cum();
        self.wait_manager += local.get_wait_manager_cum();
        // self.latency += local.get_latency_cum();
        self.read_cf += local.get_read_cf();
        self.write_cf += local.get_write_cf();
        self.read_ca += local.get_read_ca();
        self.write_ca += local.get_write_ca();
        self.rwrite_cf += local.get_rwrite_cf();
        self.edges_inserted += local.get_edges_inserted();
        self.conflict_detected += local.get_conflict_detected();
        self.rw_conflict_detected += local.get_rw_conflict_detected();
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

    fn get_thpt(&mut self) -> f64 {
        let total = (self.commits + self.not_found) as f64;
        total / (self.get_runtime() as f64 / 1000.0)
    }

    fn get_abr(&mut self) -> f64 {
        (self.aborts as f64 / (self.commits + self.not_found + self.aborts) as f64) * 100.0
    }

    fn get_txn_not_found(&self) -> u64 {
        (self.txn_not_found_cum / 1000000) as u64
    }

    fn get_txn_commit(&self) -> u64 {
        (self.txn_commit_cum / 1000000) as u64
    }

    fn get_txn_logic_abort(&self) -> u64 {
        (self.txn_logic_abort_cum / 1000000) as u64
    }

    fn get_max_txn_logic_abort(&self) -> u64 {
        (self.max_txn_logic_abort) as u64
    }

    fn get_txn_commit_abort(&self) -> u64 {
        (self.txn_commit_abort_cum / 1000000) as u64
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
            "   commits": self.commit_aborts,
            "   logic": self.logic_aborts,
            "   read_cf": self.read_cf,
            "   write_cf": self.write_cf,
            "   rwrite_cf": self.rwrite_cf,
            "   read_ca": self.read_ca,
            "   write_ca": self.write_ca,

            "not_found": self.not_found as u64,
            "txn_time (ms)": self.get_txn_time(),
            "commit_time (ms)": self.get_commit_time(),
            "wait_time (ms)": self.get_wait_time(),
            "latency (ms)": self.get_latency(),
            "thpt": format!("{:.2}",self.get_thpt()),
            "abr": format!("{:.2}",self.get_abr()),
            "edges_inserted": self.edges_inserted,
            "conflicts deteted": self.conflict_detected,
            "   rw": self.rw_conflict_detected,
            "   ww": self.ww_conflict_detected
        });

        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());

        let pr = json!({
            "runtime (ms)": self.get_runtime(),
            "cum_runtime (ms)":  self.get_total_time(),
            "txn_not_found_time (ms)": self.get_txn_not_found(),
            "txn_logic_abort_time (ms)": self.get_txn_logic_abort(),
            "max txn_logic_abort_time": self.get_max_txn_logic_abort(),
            "txn_commit_abort_time (ms)": self.get_txn_commit_abort(),
            "txn_commit_time (ms)": self.get_txn_commit(),
            "retried request": self.retries,
            "cum retries": self.cum_retries,
            "max retries": self.max_retries
        });

        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());
    }
}
