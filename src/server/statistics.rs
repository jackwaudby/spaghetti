use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct GlobalStatistics {
    /// Time the server began listening for connections.
    start: Option<Instant>,

    /// Time the server shutdown.
    end: Option<Duration>,

    /// Number of clients.
    clients: Option<u32>,

    /// Number of completed transactions (committed and aborted).
    completed: u32,

    /// Number of transactions that successfully committed.
    committed: u32,

    /// Number of transactions that aborted.
    aborted: u32,

    /// Cumulative latency of all committed transactions.
    cum_latency: u128,

    /// Throughput of committed transactions.
    thpt: Option<f64>,

    /// Average latency of committed transactions.
    av_latency: Option<f64>,

    /// Time taken to populate tables, measured in seconds.
    data_generation: Option<Duration>,
}

impl GlobalStatistics {
    /// Create global stats tracker.
    pub fn new() -> GlobalStatistics {
        GlobalStatistics {
            start: None,
            end: None,
            clients: None,
            completed: 0,
            committed: 0,
            aborted: 0,
            cum_latency: 0,
            thpt: None,
            av_latency: None,
            data_generation: None,
        }
    }

    /// Set time taken to generate data.
    pub fn set_data_generation(&mut self, duration: Duration) {
        self.data_generation = Some(duration);
    }

    /// Increment number of clients.
    pub fn inc_clients(&mut self) {
        match self.clients {
            Some(clients) => self.clients = Some(clients + 1),
            None => self.clients = Some(1),
        }
    }

    /// Set server start time.
    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    /// Set server end time.
    pub fn end(&mut self) {
        self.end = Some(self.start.unwrap().elapsed());
    }

    /// Calculate throughput.
    pub fn calculate_throughput(&mut self) {
        self.thpt = Some(self.completed as f64 / self.end.unwrap().as_secs() as f64);
    }

    /// Calculate latency.
    pub fn calculate_latency(&mut self) {
        let lat = self.cum_latency / 1000 / self.committed as u128;
        self.av_latency = Some(lat as f64 / 1000.0);
    }

    /// Merge local stats into global stats.
    pub fn merge_into(&mut self, local: Statistics) {
        self.inc_clients();
        self.completed += local.completed;
        self.committed += local.committed;
        self.aborted += local.aborted;
        self.cum_latency += local.cum_latency;
    }

    pub fn write_to_file(&mut self) {
        // Remove directory.
        if Path::new("./results").exists() {
            fs::remove_dir_all("./results").unwrap();
        }
        // Create directory
        fs::create_dir("./results").unwrap();

        // Create file.
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("results/stats.txt")
            .expect("cannot open file");
        // Data generation
        match self.data_generation {
            Some(time) => {
                write!(file, "data generation: {}(secs)\n", time.as_secs()).unwrap();
            }
            None => {
                write!(file, "No data generated").unwrap();
            }
        }

        match self.clients {
            Some(clients) => {
                write!(file, "clients: {}\n", clients).unwrap();
                // Transaction counts
                write!(file, "completed transactions: {}\n", self.completed).unwrap();
                write!(file, "committed transactions: {}\n", self.committed).unwrap();
                write!(file, "aborted transactions: {}\n", self.aborted).unwrap();
                // Calculate throughput
                self.calculate_throughput();
                write!(file, "throughput: {}(txn/s)\n", self.thpt.unwrap()).unwrap();
                // Calculate latency
                self.calculate_latency();
                write!(file, "latency: {}(ms)\n", self.thpt.unwrap()).unwrap();
            }
            None => {
                write!(file, "No clients\n").unwrap();
            }
        }
    }
}

impl fmt::Display for GlobalStatistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.clients {
            Some(clients) => {
                write!(
                    f,
                    "\nglobal statistics\nclients:{}\ndata generation: {}(secs)\nruntime:{}(secs)\ncompleted:{}\ncommitted:{}\naborted:{}\nthroughput: {}(txn/s)\nlatency:{}(ms)",
                    clients,
                    self.data_generation.unwrap().as_secs(),
                    self.end.unwrap().as_secs() as f64,
                    self.completed,
                    self.committed,
                    self.aborted,
                    self.thpt.unwrap(),
                    self.av_latency.unwrap(),
                )
            }
            None => {
                write!(  f,
                         "\nno clients")
            }
        }
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
