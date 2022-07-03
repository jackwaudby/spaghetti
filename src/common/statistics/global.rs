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
    not_found: u64,
    tx: u128,
    commit: u128,
    wait_manager: u128,
    latency: u128,
    logic_cum: u128,
    begin_cum: u128,
    a_commit_cum: u128,
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
            not_found: 0,
            tx: 0,
            commit: 0,
            wait_manager: 0,
            latency: 0,
            logic_cum: 0,
            begin_cum: 0,
            a_commit_cum: 0,
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
        self.tx += local.get_tx_cum();
        self.commit += local.get_commit_cum();
        self.wait_manager += local.get_wait_manager_cum();
        self.latency += local.get_latency_cum();
        self.logic_cum += local.get_logic_cum();
        self.begin_cum += local.get_begin_cum();
        self.a_commit_cum += local.get_a_commit_cum();
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

    fn get_logic_time(&self) -> u64 {
        (self.logic_cum / 1000000) as u64
    }

    fn get_begim_time(&self) -> u64 {
        (self.begin_cum / 1000000) as u64
    }

    fn get_a_commit_time(&self) -> u64 {
        (self.a_commit_cum / 1000000) as u64
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
            "   commits aborts": self.commit_aborts,
            "   logic aborts": self.logic_aborts,
            "not_found": self.not_found as u64,
            "txn_time (ms)": self.get_txn_time(),
            "commit_time (ms)": self.get_commit_time(),
            "wait_time (ms)": self.get_wait_time(),
            "latency (ms)": self.get_latency(),
            "   begin time (ms)": self.get_begim_time(),
            "   logic time in aborts (ms)": self.get_logic_time(),
            "   commit time in aborts (ms)": self.get_a_commit_time(),
        });

        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());
    }
}
