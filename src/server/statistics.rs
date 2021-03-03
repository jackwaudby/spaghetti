use std::fmt;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct GlobalStatistics {
    start: Option<Instant>,
    end: Option<Duration>,
    completed: u32,
    committed: u32,
    aborted: u32,
    cum_latency: u128,
    thpt: Option<f64>,
}

impl GlobalStatistics {
    pub fn new() -> GlobalStatistics {
        GlobalStatistics {
            start: None,
            end: None,
            completed: 0,
            committed: 0,
            aborted: 0,
            cum_latency: 0,
            thpt: None,
        }
    }

    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    pub fn end(&mut self) {
        self.end = Some(self.start.unwrap().elapsed());

        self.thpt = Some(self.completed as f64 / self.end.unwrap().as_secs() as f64)
    }

    pub fn merge_into(&mut self, local: Statistics) {
        self.completed += local.completed;
        self.committed += local.committed;
        self.aborted += local.aborted;
        self.cum_latency += local.cum_latency;
    }
}

impl fmt::Display for GlobalStatistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let latency = self.cum_latency / 1000 / self.committed as u128;

        write!(
            f,
            "\nGlobal Statistics\nRuntime:{}\nCompleted:{}\nCommitted:{}\nAborted:{}\nTxn/s:{}\nLatency/ms:{}",
            self.end.unwrap().as_secs() as f64,
            self.completed,
            self.committed,
            self.aborted,
            self.thpt.unwrap(),
            latency as f64 / 1000.0,
        )
    }
}

#[derive(Debug, Clone)]
pub struct Statistics {
    client_id: u32,
    completed: u32,
    committed: u32,
    aborted: u32,
    cum_latency: u128,
}

impl Statistics {
    pub fn new(client_id: u32) -> Statistics {
        Statistics {
            client_id,
            completed: 0,
            committed: 0,
            aborted: 0,
            cum_latency: 0,
        }
    }

    pub fn get_client_id(&self) -> u32 {
        self.client_id
    }

    pub fn inc_committed(&mut self) {
        self.committed += 1;
        self.completed += 1;
    }

    pub fn inc_aborted(&mut self) {
        self.aborted += 1;
        self.completed += 1;
    }

    pub fn add_cum_latency(&mut self, latency: u128) {
        self.cum_latency = self.cum_latency + latency;
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Client:{}\nCompleted:{}\nCommitted:{}\nAborted:{}\nLatency:{}",
            self.client_id, self.completed, self.committed, self.aborted, self.cum_latency
        )
    }
}
