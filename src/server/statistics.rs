use crate::common::error::NonFatalError;
use crate::common::message::{Outcome, Transaction};
use crate::server::scheduler::serialization_graph_testing::error::SerializationGraphTestingError;
use crate::server::scheduler::two_phase_locking::error::TwoPhaseLockingError;
use crate::workloads::tatp::TatpTransaction;

use serde::{Deserialize, Serialize};
use serde_json::json;
// use std::fmt;
// use std::fs::File;
use statrs::statistics::Median;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;
use strum::IntoEnumIterator;

/// Each write handler track statistics in its own instance of `LocalStatisitics`.
/// After the benchmark has completed the statisitics are merged into `GlobalStatistics`.
// #[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Debug)]
pub struct GlobalStatistics {
    /// Time taken to generate data and load into tables (secs).
    data_generation: Option<Duration>,

    /// Time taken to load data into tables fom files (secs).
    load_time: Option<Duration>,

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

    /// Per-transaction metrics.
    workload_breakdown: WorkloadBreakdown,

    /// Abort breakdown.
    abort_breakdown: AbortBreakdown,
}

impl GlobalStatistics {
    /// Create global metrics container.
    pub fn new(workload: &str, protocol: &str) -> GlobalStatistics {
        let workload_breakdown = WorkloadBreakdown::new(workload);
        let abort_breakdown = AbortBreakdown::new(protocol);

        GlobalStatistics {
            data_generation: None,
            load_time: None,
            start: None,
            end: None,
            clients: None,
            protocol: protocol.to_string(),
            workload: workload.to_string(),
            workload_breakdown,
            abort_breakdown,
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

    /// Merge local stats into global stats.
    pub fn merge_into(&mut self, local: LocalStatistics) {
        // 1. merge workload breakdown.
        self.workload_breakdown.merge(local.workload_breakdown);

        // 2. merge abort reasons.
        self.abort_breakdown.merge(local.abort_breakdown);
    }

    pub fn write_to_file(&mut self) {
        // Create directory if does not exist.
        if !Path::new("./results").exists() {
            fs::create_dir("./results").unwrap();
        }

        let file = format!("./results/{}-{}.json", self.protocol, self.workload);

        // Create file.
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&file)
            .expect("cannot open file");

        // TODO: Move
        let mut completed = 0;
        let mut committed = 0;
        let mut aborted = 0;
        let mut raw_latency = vec![];
        for transaction in &mut self.workload_breakdown.transactions {
            completed += transaction.completed;
            committed += transaction.committed;
            aborted += transaction.aborted;
            raw_latency.append(&mut transaction.raw_latency);
        }
        // TODO: double check.
        let med = raw_latency.median() * 1000.0;

        let abort_rate = (aborted as f64 / completed as f64) * 100.0;
        let throughput = completed as f64 / self.end.unwrap().as_secs() as f64;

        let overview = json!({
            "clients": self.clients,
            "protocol": self.protocol,
            "workload": self.workload,
            "completed": completed,
            "committed": committed,
            "aborted": aborted,
            "abort_rate": format!("{:.3}", abort_rate),
            "throughput": format!("{:.3}", throughput),
            "median": med,
        });

        let agg = serde_json::to_string_pretty(&overview).unwrap();
        let aborts = serde_json::to_string_pretty(&self.abort_breakdown).unwrap();
        let workload = serde_json::to_string_pretty(&self.workload_breakdown).unwrap();

        write!(file, "{}", agg).unwrap();
        // write!(file, "{}", aborts).unwrap();
        // write!(file, "{}", workload).unwrap();
    }
}

/// Each write handler track statistics in its own instance of `LocalStatisitics`.
#[derive(Debug, Clone)]
pub struct LocalStatistics {
    /// Client id.
    client_id: u32,

    /// Per-transaction metrics.
    workload_breakdown: WorkloadBreakdown,

    /// Abort breakdown.
    abort_breakdown: AbortBreakdown,
}

impl LocalStatistics {
    /// Create new metrics tracker for a write handler.
    pub fn new(client_id: u32, workload: &str, protocol: &str) -> LocalStatistics {
        let workload_breakdown = WorkloadBreakdown::new(workload);

        let abort_breakdown = AbortBreakdown::new(protocol);

        LocalStatistics {
            client_id,
            workload_breakdown,
            abort_breakdown,
        }
    }

    pub fn get_client_id(&self) -> u32 {
        self.client_id
    }

    /// Record response.
    pub fn record(
        &mut self,
        transaction: Transaction,
        outcome: Outcome,
        latency: Option<Duration>,
    ) {
        // Workload
        self.workload_breakdown
            .record(transaction, outcome.clone(), latency);

        // Protocol
        if let Outcome::Aborted { reason } = outcome {
            if let NonFatalError::RowAlreadyExists(_, _) = reason {
                self.abort_breakdown.row_already_exists += 1;
            } else {
                // workload dependent
                match &mut self.abort_breakdown.protocol_specific {
                    ProtocolAbortBreakdown::HitList(ref mut metric) => match reason {
                        NonFatalError::RowDirty(_, _) => metric.inc_row_dirty(),
                        NonFatalError::RowDeleted(_, _) => metric.inc_row_deleted(),
                        _ => {}
                    },
                    ProtocolAbortBreakdown::SerializationGraph(ref mut metric) => match reason {
                        NonFatalError::RowDirty(_, _) => metric.inc_row_dirty(),
                        NonFatalError::RowDeleted(_, _) => metric.inc_row_deleted(),
                        NonFatalError::SerializationGraphTesting(e) => {
                            if let SerializationGraphTestingError::ParentAborted = e {
                                metric.inc_parent_aborted();
                            }
                        }
                        _ => {}
                    },
                    ProtocolAbortBreakdown::TwoPhaseLocking(ref mut metric) => {
                        if let NonFatalError::TwoPhaseLocking(e) = reason {
                            match e {
                                TwoPhaseLockingError::ReadLockRequestDenied(_) => {
                                    metric.inc_read_lock_denied()
                                }
                                TwoPhaseLockingError::WriteLockRequestDenied(_) => {
                                    metric.inc_write_lock_denied()
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }
}

//////////////////////////////////////////
//// Workload Breakdown ////
/////////////////////////////////////////

#[derive(Serialize, Deserialize, Debug, Clone)]
struct WorkloadBreakdown {
    name: String,
    transactions: Vec<TransactionMetrics>,
}

impl WorkloadBreakdown {
    /// Create new workload breakdown.
    fn new(workload: &str) -> WorkloadBreakdown {
        match workload {
            "tatp" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in TatpTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Tatp(transaction));
                    transactions.push(metrics);
                }
                WorkloadBreakdown { name, transactions }
            }
            _ => unimplemented!(),
        }
    }

    /// Record completed transaction.
    fn record(&mut self, transaction: Transaction, outcome: Outcome, latency: Option<Duration>) {
        let ind = self
            .transactions
            .iter()
            .position(|x| x.transaction == transaction)
            .unwrap();

        match outcome {
            Outcome::Committed { .. } => self.transactions[ind].inc_committed(),
            Outcome::Aborted { .. } => self.transactions[ind].inc_aborted(),
        }

        self.transactions[ind].add_latency(latency.unwrap().as_secs_f64());
    }

    /// Merge two workload breakdowns
    fn merge(&mut self, other: WorkloadBreakdown) {
        // Check the metrics are for the same workload.
        assert!(self.name == other.name);

        for holder in other.transactions {
            // Find matching index.
            let ind = self
                .transactions
                .iter()
                .position(|x| x.transaction == holder.transaction)
                .unwrap();
            self.transactions[ind].merge(holder);
        }
    }
}

/// Per-transaction metrics holder.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct TransactionMetrics {
    transaction: Transaction,
    completed: u32,
    committed: u32,
    aborted: u32,
    raw_latency: Vec<f64>,
}

impl TransactionMetrics {
    /// Create new transaction metrics holder.
    fn new(transaction: Transaction) -> TransactionMetrics {
        TransactionMetrics {
            transaction,
            completed: 0,
            committed: 0,
            aborted: 0,
            raw_latency: vec![],
        }
    }

    /// Increment committed.
    fn inc_committed(&mut self) {
        self.committed += 1;
        self.completed += 1;
    }

    /// Increment aborted.
    fn inc_aborted(&mut self) {
        self.completed += 1;
        self.aborted += 1;
    }

    /// Add latency measurement
    fn add_latency(&mut self, latency: f64) {
        self.raw_latency.push(latency);
    }

    /// Merge transaction metrics.
    fn merge(&mut self, mut other: TransactionMetrics) {
        // Must merge statistics for the same transaction.
        assert!(self.transaction == other.transaction);

        self.completed += other.completed;
        self.committed += other.committed;
        self.aborted += other.aborted;
        self.raw_latency.append(&mut other.raw_latency);
    }
}

//////////////////////////////////
//// Protocol specific ////
//////////////////////////////////

/// Breakdown of reasons transactions were aborted.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct AbortBreakdown {
    /// Attempted to insert a row that already existed in the database.
    row_already_exists: u32,

    /// Protocol specific aborts reasons.
    protocol_specific: ProtocolAbortBreakdown,
}

impl AbortBreakdown {
    /// Create new holder for protocol specific abort reasons.
    fn new(protocol: &str) -> AbortBreakdown {
        let protocol_specific = match protocol {
            "sgt" => ProtocolAbortBreakdown::SerializationGraph(SerializationGraphReasons::new()),
            "2pl" => ProtocolAbortBreakdown::TwoPhaseLocking(TwoPhaseLockingReasons::new()),
            "hit" => ProtocolAbortBreakdown::HitList(HitListReasons::new()),
            _ => unimplemented!(),
        };

        AbortBreakdown {
            row_already_exists: 0,
            protocol_specific,
        }
    }

    fn merge(&mut self, other: AbortBreakdown) {
        self.row_already_exists += other.row_already_exists;

        match self.protocol_specific {
            ProtocolAbortBreakdown::HitList(ref mut reasons) => {
                if let ProtocolAbortBreakdown::HitList(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("abort breakdowns do not match");
                }
            }
            ProtocolAbortBreakdown::SerializationGraph(ref mut reasons) => {
                if let ProtocolAbortBreakdown::SerializationGraph(other_reasons) =
                    other.protocol_specific
                {
                    reasons.merge(other_reasons);
                } else {
                    panic!("abort breakdowns do not match");
                }
            }
            ProtocolAbortBreakdown::TwoPhaseLocking(ref mut reasons) => {
                if let ProtocolAbortBreakdown::TwoPhaseLocking(other_reasons) =
                    other.protocol_specific
                {
                    reasons.merge(other_reasons);
                } else {
                    panic!("abort breakdowns do not match");
                }
            }
        }
    }
}

/// Protocol specific reasons for aborts.
#[derive(Serialize, Deserialize, Debug, Clone)]
enum ProtocolAbortBreakdown {
    TwoPhaseLocking(TwoPhaseLockingReasons),
    SerializationGraph(SerializationGraphReasons),
    HitList(HitListReasons),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TwoPhaseLockingReasons {
    /// Transaction was denied a read lock and aborted.
    read_lock_denied: u32,

    /// Transaction was denied a write lock and aborted.
    write_lock_denied: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SerializationGraphReasons {
    /// Transaction attempted to modify a row already modified.
    row_dirty: u32,

    /// Transaction attempted to read or modify a row already marked for deletion.
    row_deleted: u32,

    /// Transaction aborted as conflicting transaction was aborted (cascading abort).
    parent_aborted: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct HitListReasons {
    /// Transaction attempted to modify a row already modified.
    row_dirty: u32,

    /// Transaction attempted to read or modify a row already marked for deletion.
    row_deleted: u32,
}

impl TwoPhaseLockingReasons {
    /// Create new holder for 2PL abort reasons.
    fn new() -> TwoPhaseLockingReasons {
        TwoPhaseLockingReasons {
            read_lock_denied: 0,
            write_lock_denied: 0,
        }
    }

    /// Increment read lock denied counter.
    fn inc_read_lock_denied(&mut self) {
        self.read_lock_denied += 1;
    }

    /// Increment write lock denied counter.
    fn inc_write_lock_denied(&mut self) {
        self.write_lock_denied += 1;
    }

    fn merge(&mut self, other: TwoPhaseLockingReasons) {
        self.read_lock_denied += other.read_lock_denied;
        self.write_lock_denied += other.write_lock_denied;
    }
}

impl SerializationGraphReasons {
    /// Create new holder for SGT abort reasons.
    fn new() -> SerializationGraphReasons {
        SerializationGraphReasons {
            row_dirty: 0,
            row_deleted: 0,
            parent_aborted: 0,
        }
    }

    /// Increment row dirty counter.
    fn inc_row_dirty(&mut self) {
        self.row_dirty += 1;
    }

    /// Increment row deleted counter.
    fn inc_row_deleted(&mut self) {
        self.row_deleted += 1;
    }

    /// Increment parent aborted counter.
    fn inc_parent_aborted(&mut self) {
        self.parent_aborted += 1;
    }

    fn merge(&mut self, other: SerializationGraphReasons) {
        self.row_dirty += other.row_dirty;
        self.row_deleted += other.row_deleted;
        self.parent_aborted += other.parent_aborted;
    }
}

impl HitListReasons {
    /// Create new holder for HIT abort reasons.
    fn new() -> HitListReasons {
        HitListReasons {
            row_dirty: 0,
            row_deleted: 0,
        }
    }

    /// Increment row dirty counter.
    fn inc_row_dirty(&mut self) {
        self.row_dirty += 1;
    }

    /// Increment row deleted counter.
    fn inc_row_deleted(&mut self) {
        self.row_deleted += 1;
    }

    fn merge(&mut self, other: HitListReasons) {
        self.row_dirty += other.row_dirty;
        self.row_deleted += other.row_deleted;
    }
}
