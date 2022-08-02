use std::time::Instant;

use crate::common::stats_bucket::StatsBucket;

#[derive(Debug, Clone)]
pub struct LocalStatistics {
    worker_start: Instant,
    commits: u64,
    aborts: u64,
    not_found: u64,
    tx_cum: u128,
    commit_start: Instant,
    commit_cum: u128,
    wait_manager_start: Instant,
    wait_manager_cum: u128,
    latency_start: Instant,
    latency_cum: u128,
    rw_conflicts: u64,
    wr_conflicts: u64,
    ww_conflicts: u64,
    worker_cum: u128,
    tx_start: Instant,
    g0: u64,
    g1: u64,
    g2: u64,
    path_len: usize,
    early_commit: u64,
}

impl LocalStatistics {
    pub fn new() -> Self {
        LocalStatistics {
            worker_start: Instant::now(),
            commits: 0,
            aborts: 0,
            not_found: 0,
            tx_cum: 0,
            commit_start: Instant::now(),
            commit_cum: 0,
            wait_manager_start: Instant::now(),
            wait_manager_cum: 0,
            latency_start: Instant::now(),
            latency_cum: 0,
            rw_conflicts: 0,
            wr_conflicts: 0,
            ww_conflicts: 0,
            worker_cum: 0,
            tx_start: Instant::now(),
            g0: 0,
            g1: 0,
            g2: 0,
            path_len: 0,
            early_commit: 0,
        }
    }

    pub fn start_worker(&mut self) {
        self.worker_start = Instant::now();
    }

    pub fn stop_worker(&mut self) {
        self.worker_cum = self.worker_start.elapsed().as_nanos();
    }

    pub fn get_worker_cum(&self) -> u128 {
        self.worker_cum
    }

    pub fn inc_commits(&mut self) {
        self.commits += 1;
    }

    pub fn get_commits(&self) -> u64 {
        self.commits
    }

    pub fn inc_aborts(&mut self) {
        self.aborts += 1;
    }

    pub fn get_aborts(&self) -> u64 {
        self.aborts
    }

    pub fn get_conflicts(&self) -> u64 {
        self.ww_conflicts + self.wr_conflicts + self.rw_conflicts
    }

    pub fn get_ww_conflicts(&self) -> u64 {
        self.ww_conflicts
    }

    pub fn get_wr_conflicts(&self) -> u64 {
        self.wr_conflicts
    }

    pub fn get_rw_conflicts(&self) -> u64 {
        self.rw_conflicts
    }

    pub fn inc_conflicts(&mut self, meta: &StatsBucket) {
        self.ww_conflicts += meta.get_ww_conflicts();
        self.wr_conflicts += meta.get_wr_conflicts();
        self.rw_conflicts += meta.get_rw_conflicts();
    }

    pub fn inc_not_found(&mut self) {
        self.not_found += 1;
    }

    pub fn get_not_found(&self) -> u64 {
        self.not_found
    }

    pub fn start_tx(&mut self) {
        self.tx_start = Instant::now();
    }

    pub fn stop_tx(&mut self) -> u128 {
        let cum = self.tx_start.elapsed().as_nanos();
        self.tx_cum += cum;
        cum
        // self.tx_start.elapsed().as_nanos()
    }

    pub fn get_tx_cum(&self) -> u128 {
        self.tx_cum
    }

    pub fn start_commit(&mut self) {
        self.commit_start = Instant::now();
    }

    pub fn stop_commit(&mut self) -> u128 {
        let cum = self.commit_start.elapsed().as_nanos();
        self.commit_cum += cum;
        cum
    }

    pub fn get_commit_cum(&self) -> u128 {
        self.commit_cum
    }

    pub fn start_wait_manager(&mut self) {
        self.wait_manager_start = Instant::now();
    }

    pub fn stop_wait_manager(&mut self) {
        self.wait_manager_cum += self.wait_manager_start.elapsed().as_nanos();
    }

    pub fn get_wait_manager_cum(&self) -> u128 {
        self.wait_manager_cum
    }

    pub fn start_latency(&mut self) {
        self.latency_start = Instant::now();
    }

    pub fn stop_latency(&mut self, tx_time: u128) {
        let add = self.latency_start.elapsed().as_nanos() - tx_time;
        // pub fn stop_latency(&mut self) {
        // self.latency_cum += self.latency_start.elapsed().as_nanos();
        self.latency_cum += add;
    }

    pub fn get_latency_cum(&self) -> u128 {
        self.latency_cum
    }

    pub fn inc_g0(&mut self) {
        self.g0 += 1;
    }

    pub fn get_g0(&mut self) -> u64 {
        self.g0
    }

    pub fn inc_g1(&mut self) {
        self.g1 += 1;
    }

    pub fn get_g1(&mut self) -> u64 {
        self.g1
    }

    pub fn inc_g2(&mut self) {
        self.g2 += 1;
    }

    pub fn get_g2(&mut self) -> u64 {
        self.g2
    }

    pub fn inc_path_len(&mut self, path_len: usize) {
        self.path_len += path_len;
    }

    pub fn get_path_len(&self) -> usize {
        self.path_len
    }

    pub fn inc_early_commi(&mut self) {
        self.early_commit += 1;
    }

    pub fn get_early_commit(&mut self) -> u64 {
        self.early_commit
    }
}
