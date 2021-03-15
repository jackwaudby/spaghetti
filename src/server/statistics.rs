use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;
use tracing::debug;

/// Each write handler track statistics in its own instance of `LocalStatisitics`.
/// After the benchmark has completed the statisitics are merged into `GlobalStatistics`.
#[derive(Debug, Clone)]
pub struct GlobalStatistics {
    /// Time the server began listening for connections.
    start: Option<Instant>,

    /// Time the server shutdown.
    end: Option<Duration>,

    /// Number of clients.
    clients: Option<u32>,

    /// Protocol.
    protocol: String,

    /// Workload.
    workload: String,

    /// Subscribers - TATP only.
    subscribers: u32,

    /// Number of completed transactions (committed and aborted).
    completed: u32,

    /// Number of transactions that successfully committed.
    committed: u32,

    /// Number of transactions that aborted.
    aborted: u32,

    /// Row already exisited.
    row_already_exists: u32,

    /// Row was marked dirty.
    row_dirty: u32,

    /// Row was marked as deleted.
    row_deleted: u32,

    /// Predecessor aborted - SGT only.
    parent_aborted: u32,

    /// Transaction denied a read lock - 2PL only.
    read_lock_denied: u32,

    /// Transaction denied a write lock - 2PL only.
    write_lock_denied: u32,

    /// Cumulative latency of all committed transactions.
    cum_latency: u128,

    /// Throughput of committed transactions.
    /// committed / (end - start)
    throughput: Option<f64>,

    /// Average latency of committed transactions.
    /// cum_latency / committed
    av_latency: Option<f64>,

    /// Time taken to populate tables, measured in seconds.
    data_generation: Option<Duration>,
}

impl GlobalStatistics {
    /// Create global stats tracker.
    pub fn new(subscribers: u32, workload: &str, protocol: &str) -> GlobalStatistics {
        GlobalStatistics {
            start: None,
            end: None,
            clients: None,
            completed: 0,
            committed: 0,
            aborted: 0,
            cum_latency: 0,
            throughput: None,
            av_latency: None,
            data_generation: None,
            row_already_exists: 0,
            row_dirty: 0,
            parent_aborted: 0,
            row_deleted: 0,
            read_lock_denied: 0,
            write_lock_denied: 0,
            subscribers,
            workload: workload.to_string(),
            protocol: protocol.to_string(),
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
        self.throughput = Some(self.completed as f64 / self.end.unwrap().as_secs() as f64);
    }

    /// Calculate latency.
    pub fn calculate_latency(&mut self) {
        let lat = self.cum_latency / 1000 / self.committed as u128;
        self.av_latency = Some(lat as f64 / 1000.0);
    }

    /// Merge local stats into global stats.
    pub fn merge_into(&mut self, local: LocalStatistics) {
        debug!("Merge local stats into global");
        self.inc_clients();
        self.completed += local.completed;
        self.committed += local.committed;
        self.aborted += local.aborted;
        self.cum_latency += local.cum_latency;
        self.row_already_exists += local.row_already_exists;
        self.row_deleted += local.row_deleted;
        self.row_dirty += local.row_dirty;
        self.read_lock_denied += local.read_lock_denied;
        self.write_lock_denied += local.write_lock_denied;
        self.parent_aborted += local.parent_aborted;
    }

    pub fn write_to_file(&mut self) {
        // Remove directory.
        if Path::new("./results").exists() {
            fs::remove_dir_all("./results").unwrap();
        }
        // Create directory
        fs::create_dir("./results").unwrap();

        let p = format!("./results-{}-{}", self.protocol, self.workload);
        // Create file.
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&p)
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
                write!(file, "protocol: {}\n", self.protocol).unwrap();
                write!(file, "workload: {}\n", self.workload).unwrap();
                write!(file, "subscribers: {}\n", self.subscribers).unwrap();

                // Transaction counts
                write!(file, "completed transactions: {}\n", self.completed).unwrap();
                write!(file, "committed transactions: {}\n", self.committed).unwrap();
                write!(file, "aborted transactions: {}\n", self.aborted).unwrap();
                write!(file, "row already existed: {}\n", self.row_already_exists).unwrap();
                write!(file, "row marked for delete: {}\n", self.row_deleted).unwrap();
                write!(file, "row marked as dirty: {}\n", self.row_dirty).unwrap();
                write!(file, "parent aborted: {}\n", self.parent_aborted).unwrap();
                write!(file, "read lock denied: {}\n", self.read_lock_denied).unwrap();
                write!(file, "write lock denied: {}\n", self.write_lock_denied).unwrap();
                // Calculate throughput
                self.calculate_throughput();
                write!(file, "throughput: {}(txn/s)\n", self.throughput.unwrap()).unwrap();
                // Calculate latency
                self.calculate_latency();
                write!(file, "latency: {}(ms)\n", self.av_latency.unwrap()).unwrap();
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
                    self.throughput.unwrap(),
                    self.av_latency.unwrap(),
                )
            }
            None => {
                write!(  f,
                         "\no clients")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalStatistics {
    client_id: u32,
    completed: u32,
    committed: u32,
    aborted: u32,
    row_already_exists: u32,
    row_dirty: u32,
    row_deleted: u32,
    read_lock_denied: u32,
    write_lock_denied: u32,
    parent_aborted: u32,
    cum_latency: u128,
}

impl LocalStatistics {
    pub fn new(client_id: u32) -> LocalStatistics {
        LocalStatistics {
            client_id,
            completed: 0,
            committed: 0,
            aborted: 0,
            row_already_exists: 0,
            parent_aborted: 0,
            row_dirty: 0,
            row_deleted: 0,
            cum_latency: 0,
            read_lock_denied: 0,
            write_lock_denied: 0,
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

    pub fn inc_row_already_exists(&mut self) {
        self.row_already_exists += 1;
    }

    pub fn inc_row_dirty(&mut self) {
        self.row_dirty += 1;
    }

    pub fn inc_row_deleted(&mut self) {
        self.row_deleted += 1;
    }

    pub fn inc_parent_aborted(&mut self) {
        self.parent_aborted += 1;
    }

    pub fn inc_read_lock_denied(&mut self) {
        self.read_lock_denied += 1;
    }

    pub fn inc_write_lock_denied(&mut self) {
        self.write_lock_denied += 1;
    }

    pub fn add_cum_latency(&mut self, latency: u128) {
        self.cum_latency = self.cum_latency + latency;
    }
}

impl fmt::Display for LocalStatistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Client:{}\nCompleted:{}\nCommitted:{}\nAborted:{}\nLatency:{}",
            self.client_id, self.completed, self.committed, self.aborted, self.cum_latency
        )
    }
}
