use std::time::Instant;

#[derive(Debug, Clone)]
pub struct LocalStatistics {
    worker_start: Instant,
    worker_cum: u128,
    commits: u64,
    aborts: u64,
    logic_aborts: u64,
    commit_aborts: u64,
    not_found: u64,
    tx_start: Instant,
    tx_cum: u128,
    commit_start: Instant,
    commit_cum: u128,
    wait_manager_start: Instant,
    wait_manager_cum: u128,
    latency_start: Instant,
    latency_cum: u128,
}

impl LocalStatistics {
    pub fn new() -> Self {
        LocalStatistics {
            worker_start: Instant::now(),
            worker_cum: 0,
            commits: 0,
            aborts: 0,
            logic_aborts: 0,
            commit_aborts: 0,
            not_found: 0,
            tx_start: Instant::now(),
            tx_cum: 0,
            commit_start: Instant::now(),
            commit_cum: 0,
            wait_manager_start: Instant::now(),
            wait_manager_cum: 0,
            latency_start: Instant::now(),
            latency_cum: 0,
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

    pub fn inc_commit_aborts(&mut self) {
        self.commit_aborts += 1;
    }

    pub fn get_commit_aborts(&self) -> u64 {
        self.commit_aborts
    }

    pub fn get_logic_aborts(&self) -> u64 {
        self.logic_aborts
    }

    pub fn inc_logic_aborts(&mut self) {
        self.logic_aborts += 1;
    }

    pub fn inc_aborts(&mut self) {
        self.aborts += 1;
    }

    pub fn get_aborts(&self) -> u64 {
        self.aborts
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
}
