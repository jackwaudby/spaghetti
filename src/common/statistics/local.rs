use std::time::Instant;

#[derive(Debug, Clone)]
pub struct LocalStatistics {
    worker_start: Instant,

    commits: u64,
    aborts: u64,
    logic_aborts: u64,
    commit_aborts: u64,
    read_cf: u64,
    rwrite_cf: u64,
    write_cf: u64,
    read_ca: u64,
    write_ca: u64,
    not_found: u64,
    tx_cum: u128,
    commit_start: Instant,
    commit_cum: u128,
    wait_manager_start: Instant,
    wait_manager_cum: u128,
    latency_start: Instant,
    latency_cum: u128,
    edges_inserted: u64,
    conflict_detected: u64,
    rw_conflict_detected: u64,
    ww_conflict_detected: u64,

    retries: u64,
    cum_retries: u64,
    max_retries: u64,

    // runtime breakdown
    worker_cum: u128,
    tx_start: Instant,

    txn_not_found_cum: u128,
    txn_commit_cum: u128,
    txn_commit_abort_cum: u128,
    txn_logic_abort_cum: u128,

    max_txn_logic_abort: u128,
    max_txn_commit_abort: u128,

    pub aborted_latency: Vec<u64>,
}

impl LocalStatistics {
    pub fn new() -> Self {
        LocalStatistics {
            worker_start: Instant::now(),
            commits: 0,
            aborts: 0,
            logic_aborts: 0,
            commit_aborts: 0,
            read_cf: 0,
            write_cf: 0,
            rwrite_cf: 0,
            read_ca: 0,
            write_ca: 0,
            not_found: 0,
            tx_cum: 0,
            commit_start: Instant::now(),
            commit_cum: 0,
            wait_manager_start: Instant::now(),
            wait_manager_cum: 0,
            latency_start: Instant::now(),
            latency_cum: 0,
            edges_inserted: 0,
            conflict_detected: 0,
            rw_conflict_detected: 0,
            ww_conflict_detected: 0,

            // runtime breakdown
            worker_cum: 0,
            tx_start: Instant::now(),
            txn_not_found_cum: 0,
            txn_commit_cum: 0,
            txn_commit_abort_cum: 0,
            txn_logic_abort_cum: 0,

            retries: 0,
            cum_retries: 0,
            max_retries: 0,

            aborted_latency: Vec::new(),

            max_txn_logic_abort: 0,
            max_txn_commit_abort: 0,
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

    pub fn inc_retries(&mut self) {
        self.retries += 1;
    }

    pub fn get_retries(&self) -> u64 {
        self.retries
    }

    pub fn add_cum_retries(&mut self, add: u64) {
        self.cum_retries += add;

        if self.max_retries < add {
            self.max_retries = add;
        }
    }

    pub fn get_max_retries(&self) -> u64 {
        self.max_retries
    }

    pub fn get_cum_retries(&self) -> u64 {
        self.cum_retries
    }

    pub fn inc_commits(&mut self) {
        self.commits += 1;
    }

    pub fn get_commits(&self) -> u64 {
        self.commits
    }

    pub fn inc_read_cf(&mut self) {
        self.read_cf += 1;
    }

    pub fn get_read_cf(&self) -> u64 {
        self.read_cf
    }

    pub fn inc_write_cf(&mut self) {
        self.write_cf += 1;
    }

    pub fn get_write_cf(&self) -> u64 {
        self.write_cf
    }

    pub fn inc_rwrite_cf(&mut self) {
        self.rwrite_cf += 1;
    }

    pub fn get_rwrite_cf(&self) -> u64 {
        self.rwrite_cf
    }

    pub fn inc_read_ca(&mut self) {
        self.read_ca += 1;
    }

    pub fn get_read_ca(&self) -> u64 {
        self.read_ca
    }

    pub fn inc_write_ca(&mut self) {
        self.write_ca += 1;
    }

    pub fn get_write_ca(&self) -> u64 {
        self.write_ca
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

    pub fn inc_edges_inserted(&mut self) {
        self.edges_inserted += 1;
    }

    pub fn get_edges_inserted(&self) -> u64 {
        self.edges_inserted
    }

    pub fn inc_conflict_detected(&mut self) {
        self.conflict_detected += 1;
    }

    pub fn get_conflict_detected(&self) -> u64 {
        self.conflict_detected
    }

    pub fn inc_rw_conflict_detected(&mut self) {
        self.rw_conflict_detected += 1;
    }

    pub fn get_rw_conflict_detected(&self) -> u64 {
        self.rw_conflict_detected
    }

    pub fn inc_ww_conflict_detected(&mut self) {
        self.ww_conflict_detected += 1;
    }

    pub fn get_ww_conflict_detected(&self) -> u64 {
        self.ww_conflict_detected
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

    pub fn stop_txn_not_found(&mut self, txn_time: u128) {
        self.txn_not_found_cum += txn_time;
    }

    pub fn stop_txn_commit(&mut self, txn_time: u128) {
        self.txn_commit_cum += txn_time;
    }

    pub fn stop_txn_commit_abort(&mut self, txn_time: u128) {
        if self.max_txn_commit_abort < txn_time {
            self.max_txn_commit_abort = txn_time;
        }
        self.txn_commit_abort_cum += txn_time;
    }

    pub fn stop_txn_logic_abort(&mut self, txn_time: u128) {
        if self.max_txn_logic_abort < txn_time {
            self.max_txn_logic_abort = txn_time;
        }
        self.txn_logic_abort_cum += txn_time;
    }

    pub fn get_max_txn_logic_abor(&self) -> u128 {
        self.max_txn_logic_abort
    }

    pub fn get_max_txn_commit_abor(&self) -> u128 {
        self.max_txn_commit_abort
    }
    pub fn get_txn_not_found(&self) -> u128 {
        self.txn_not_found_cum
    }

    pub fn get_txn_commit(&self) -> u128 {
        self.txn_commit_cum
    }

    pub fn get_txn_commit_abort(&self) -> u128 {
        self.txn_commit_abort_cum
    }

    pub fn get_txn_logic_abort(&self) -> u128 {
        self.txn_logic_abort_cum
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
