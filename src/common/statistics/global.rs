use config::Config;
use csv::WriterBuilder;
use serde::Serialize;
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
    not_found: u64,
    tx: u128,
    commit: u128,
    wait_manager: u128,
    latency: u128,
    rw_conflicts: u64,
    wr_conflicts: u64,
    ww_conflicts: u64,
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
            not_found: 0,
            tx: 0,
            commit: 0,
            wait_manager: 0,
            latency: 0,
            rw_conflicts: 0,
            wr_conflicts: 0,
            ww_conflicts: 0,
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
        // match self.end {
        //     Some(elasped) => elasped.as_millis() as u64,
        //     None => 0,
        // }

        self.get_total_time() / self.cores
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

        self.not_found += local.get_not_found();
        self.tx += local.get_tx_cum();
        self.commit += local.get_commit_cum();
        self.wait_manager += local.get_wait_manager_cum();
        self.latency += local.get_latency_cum();

        self.rw_conflicts += local.get_rw_conflicts();
        self.wr_conflicts += local.get_wr_conflicts();
        self.ww_conflicts += local.get_ww_conflicts();

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

    fn get_total_conflicts(&self) -> u64 {
        self.ww_conflicts + self.wr_conflicts + self.rw_conflicts
    }

    fn get_total_cycles(&self) -> u64 {
        self.g0 + self.g1 + self.g2
    }

    fn get_av_path_len(&self) -> f64 {
        self.path_len as f64 / (self.g0 + self.g1 + self.g2) as f64
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
            "conflicts deteted": self.get_total_conflicts(),
            "   rw": self.rw_conflicts,
            "   wr": self.wr_conflicts,
            "   ww": self.ww_conflicts,
            "cycles found": self.get_total_cycles(),
            "   g0": self.g0,
            "   g1": self.g1,
            "   g2": self.g2,
            "path len": self.path_len,
            "av. path len": self.get_av_path_len() ,
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

        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(file);

        let cycle_check_strategy = config.get_str("dfs").unwrap();

        // YCSB
        let theta = config.get_float("theta").unwrap();
        let serializable_rate = config.get_float("serializable_rate").unwrap();
        let update_rate = config.get_float("update_rate").unwrap();
        let queries = config.get_int("queries").unwrap() as u64;

        wtr.serialize(CsvOutput {
            scale_factor: self.scale_factor,
            protocol: self.protocol.clone(),
            workload: self.workload.clone(),
            cores: self.cores,
            theta,
            serializable_rate,
            update_rate,
            queries,
            cycle_check_strategy,
            runtime: self.get_runtime(),
            commits: self.commits,
            aborts: self.aborts,
            not_found: self.not_found,
            txn_time: self.get_txn_time(),
            commit_time: self.get_commit_time(),
            wait_time: self.get_wait_time(),
            latency: self.get_latency(),
            rw_conflicts: self.rw_conflicts,
            wr_conflicts: self.wr_conflicts,
            ww_conflicts: self.ww_conflicts,
            g0: self.g0,
            g1: self.g1,
            g2: self.g2,
            path_len: self.path_len,
        })
        .unwrap();
    }
}

#[derive(Debug, Serialize)]
struct CsvOutput {
    scale_factor: u64,
    protocol: String,
    workload: String,
    cores: u64,
    theta: f64,
    serializable_rate: f64,
    update_rate: f64,
    queries: u64,
    cycle_check_strategy: String,
    runtime: u64,
    commits: u64,
    aborts: u64,
    not_found: u64,
    txn_time: u64,
    commit_time: u64,
    wait_time: u64,
    latency: u64,
    rw_conflicts: u64,
    wr_conflicts: u64,
    ww_conflicts: u64,
    g0: u64,
    g1: u64,
    g2: u64,
    path_len: usize,
}
