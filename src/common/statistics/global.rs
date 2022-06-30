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
    cores: u32,
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
}

impl GlobalStatistics {
    pub fn new(config: &Config) -> Self {
        let scale_factor = config.get_int("scale_factor").unwrap() as u64;
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let cores = config.get_int("cores").unwrap() as u32;

        GlobalStatistics {
            scale_factor,
            protocol,
            workload,
            cores,
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

    pub fn merge(&mut self, local: LocalStatistics) {
        self.total_time += local.get_worker_cum();
        self.commits += local.get_commits();
        self.aborts += local.get_aborts();
        self.not_found += local.get_not_found();
        self.tx += local.get_tx_cum();
        self.commit += local.get_commit_cum();
        self.wait_manager += local.get_wait_manager_cum();
        self.latency += local.get_latency_cum();
    }

    fn get_total_time(&self) -> u64 {
        (self.total_time / 1000000) as u64
    }

    fn get_latency(&self) -> u64 {
        (self.latency / 1000000) as u64
    }

    pub fn print_to_console(&mut self) {
        let pr = json!({
            "workload": self.workload,
            "sf": self.scale_factor,
            "cores": self.cores,
            "protocol": self.protocol,
            "runtime (ms)": self.get_runtime(),
            "total_time (ms)":  self.get_total_time(),
            "commits": self.commits,
            "aborts": self.aborts,
            "not_found": self.not_found as u64,
            "txn_time (ms)": self.tx as u64,
            "commit_time (ms)": self.commit as u64,
            "wait_time (ms)": self.wait_manager as u64,
            "latency (ms)": self.get_latency()
        });

        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());
    }
}
