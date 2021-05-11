use crate::common::error::NonFatalError;
use crate::common::message::{Outcome, Transaction};
use crate::scheduler::basic_sgt::error::BasicSerializationGraphTestingError;
// use crate::scheduler::hit_list::error::HitListError;
// use crate::scheduler::opt_hit_list::error::OptimisedHitListError;
// use crate::scheduler::serialization_graph_testing::error::SerializationGraphTestingError;
// use crate::scheduler::two_phase_locking::error::TwoPhaseLockingError;
// use crate::workloads::acid::AcidTransaction;
use crate::workloads::smallbank::SmallBankTransaction;
// use crate::workloads::tatp::TatpTransaction;

use config::Config;
use serde::{Deserialize, Serialize};
use serde_json::json;
use statrs::statistics::OrderStatistics;
use statrs::statistics::{Max, Mean, Min};
use std::fs::{self, OpenOptions};
use std::path::Path;

use std::time::Duration;
use std::time::Instant;
use strum::IntoEnumIterator;

/// Each write handler track statistics in its own instance of `LocalStatisitics`.
/// After the benchmark has completed the statisitics are merged into `GlobalStatistics`.
#[derive(Debug)]
pub struct GlobalStatistics {
    scale_factor: u64,

    /// Time taken to generate data and load into tables (secs).
    data_generation: Option<Duration>,

    /// Time taken to load data into tables fom files (secs).
    load_time: Option<Duration>,

    /// Number of warmup operations.
    warmup: u32,

    /// Time the server began listening for connections.
    start: Option<Instant>,

    /// Time the server shutdown.
    end: Option<Duration>,

    /// Number of clients.
    clients: Option<u32>,

    /// Number of cores.
    cores: u32,

    cum_time: u128,

    /// Protocol.
    protocol: String,

    /// Workload.
    workload: String,

    /// Anomaly.
    anomaly: Option<String>,

    /// Per-transaction metrics.
    workload_breakdown: WorkloadBreakdown,

    /// Abort breakdown.
    abort_breakdown: AbortBreakdown,
}

impl GlobalStatistics {
    /// Create global metrics container.
    pub fn new(config: &Config) -> GlobalStatistics {
        let scale_factor = config.get_int("scale_factor").unwrap() as u64;
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let anomaly;
        if let Ok(a) = config.get_str("anomaly") {
            anomaly = Some(a);
        } else {
            anomaly = None;
        }

        let warmup = config.get_int("warmup").unwrap() as u32;
        let cores = config.get_int("workers").unwrap() as u32;

        let workload_breakdown = WorkloadBreakdown::new(&workload);

        let abort_breakdown = AbortBreakdown::new(&protocol, &workload);

        GlobalStatistics {
            scale_factor,
            data_generation: None,
            load_time: None,
            warmup,
            start: None,
            end: None,
            clients: None,
            protocol,
            workload,
            cum_time: 0,
            anomaly,
            cores,
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
        self.cum_time += local.end.unwrap().as_millis();

        // 1. merge workload breakdown.
        self.workload_breakdown.merge(local.workload_breakdown);

        // 2. merge abort reasons.
        self.abort_breakdown.merge(local.abort_breakdown);
    }

    pub fn write_to_file(&mut self) {
        let path;
        let file;
        if self.workload.as_str() == "acid" {
            path = format!(
                "./results/{}/{}/",
                self.workload,
                self.anomaly.as_ref().unwrap()
            );
            file = format!(
                "./results/{}/{}/{}-sf{}.json",
                self.workload,
                self.anomaly.as_ref().unwrap(),
                self.protocol,
                self.scale_factor
            );
        } else {
            path = format!("./results/{}", self.workload);
            file = format!(
                "./results/{}/{}-sf{}.json",
                self.workload, self.protocol, self.scale_factor
            );
        }

        if !Path::new(&path).exists() {
            fs::create_dir_all(&path).unwrap();
        }

        // Remove file if already exists.
        if Path::new(&file).exists() {
            fs::remove_file(&file).unwrap();
        }

        // Create new file.
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&file)
            .expect("cannot open file");

        // Compute totals.
        let mut completed = 0;
        let mut committed = 0;
        let mut aborted = 0;
        let mut raw_latency = vec![];
        for transaction in &mut self.workload_breakdown.transactions {
            completed += transaction.completed;
            committed += transaction.committed;
            aborted += transaction.aborted;
            transaction.calculate_latency_summary();
            let mut temp = transaction.raw_latency.clone();
            raw_latency.append(&mut temp);
        }

        let abort_rate;
        let throughput;
        if self.workload == "tatp" {
            // missing data does not contributed to the abort rate.
            let tatp_aborted = aborted - self.abort_breakdown.row_not_found;
            let tatp_success = committed + self.abort_breakdown.row_not_found;

            abort_rate = (tatp_aborted as f64 / completed as f64) * 100.0;
            throughput = tatp_success as f64 / self.end.unwrap().as_secs() as f64;
        } else if self.workload == "smallbank" {
            let wab = self.abort_breakdown.workload_specific.as_mut().unwrap();
            match wab {
                WorkloadAbortBreakdown::SmallBank(reason) => {
                    abort_rate =
                        ((aborted - reason.insufficient_funds) as f64 / completed as f64) * 100.0;
                    let x = (self.cum_time as f64 / self.cores as f64) / 1000.0;
                    throughput = committed as f64 / x;
                }
            }
        } else {
            abort_rate = (aborted as f64 / completed as f64) * 100.0;
            throughput = committed as f64 / self.end.unwrap().as_secs() as f64;
        }
        // Compute latency.
        let min = raw_latency.min() * 1000.0;
        let max = raw_latency.max() * 1000.0;
        let mean = raw_latency.mean() * 1000.0;
        let pc50 = raw_latency.quantile(0.5) * 1000.0;
        let pc90 = raw_latency.quantile(0.9) * 1000.0;
        let pc95 = raw_latency.quantile(0.95) * 1000.0;
        let pc99 = raw_latency.quantile(0.99) * 1000.0;

        let overview = json!({
            "sf": self.scale_factor,
            "load": self.data_generation.unwrap().as_secs(),
            "clients": self.clients,
            "protocol": self.protocol,
            "workload": self.workload,
            "total_duration": self.end.unwrap().as_secs(),
            "warmup": self.warmup,
            "completed": completed,
            "committed": committed,
            "aborted": aborted,
            "abort_rate": format!("{:.3}", abort_rate),
            "throughput": format!("{:.3}", throughput),
            "min": min,
            "max": max,
            "mean": mean,
            "50th_percentile": pc50,
            "90th_percentile": pc90,
            "95th_percentile": pc95,
            "99th_percentile": pc99,
            "abort_breakdown": self.abort_breakdown,
            "workload_breakdown": self.workload_breakdown,
        });

        serde_json::to_writer_pretty(file, &overview).unwrap();

        let pr = json!({
            "sf": self.scale_factor,
            "cores": self.cores,
            "protocol": self.protocol,
            "workload": self.workload,
            "anomaly": self.anomaly,
            "total_duration": self.end.unwrap().as_secs(),
            "completed": completed,
            "throughput": format!("{:.3}", throughput),
            "abort_rate": format!("{:.3}", abort_rate),
            "mean": format!("{:.3}", mean),
        });
        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());
    }
}

/// Each write handler track statistics in its own instance of `LocalStatisitics`.
#[derive(Debug, Clone)]
pub struct LocalStatistics {
    /// Client id.
    client_id: u32,

    start: Option<Instant>,

    end: Option<Duration>,

    /// Per-transaction metrics.
    workload_breakdown: WorkloadBreakdown,

    /// Abort breakdown.
    abort_breakdown: AbortBreakdown,
}

impl LocalStatistics {
    /// Create new metrics tracker for a write handler.
    pub fn new(client_id: u32, workload: &str, protocol: &str) -> LocalStatistics {
        let workload_breakdown = WorkloadBreakdown::new(workload);

        let abort_breakdown = AbortBreakdown::new(protocol, workload);

        LocalStatistics {
            client_id,
            start: None,
            end: None,
            workload_breakdown,
            abort_breakdown,
        }
    }

    pub fn get_client_id(&self) -> u32 {
        self.client_id
    }
    /// Set server start time.
    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    /// Set server end time.
    pub fn end(&mut self) {
        self.end = Some(self.start.unwrap().elapsed());
    }
    /// Record response.
    pub fn record(
        &mut self,
        transaction: Transaction,
        outcome: Outcome,
        latency: Option<Duration>,
    ) {
        self.workload_breakdown
            .record(transaction, outcome.clone(), latency); // workload

        // Abort reasons
        if let Outcome::Aborted { reason } = outcome {
            match reason {
                NonFatalError::RowAlreadyExists(_, _) => {
                    self.abort_breakdown.row_already_exists += 1
                }
                NonFatalError::RowNotFound(_, _) => self.abort_breakdown.row_not_found += 1,
                NonFatalError::SmallBankError(_) => {
                    if let Some(ref mut wab) = self.abort_breakdown.workload_specific {
                        match wab {
                            WorkloadAbortBreakdown::SmallBank(ref mut metric) => {
                                metric.inc_insufficient_funds()
                            }
                        }
                    }
                }
                _ =>
                // protocol dependent
                {
                    match &mut self.abort_breakdown.protocol_specific {
                        ProtocolAbortBreakdown::HitList(ref mut metric) => match reason {
                            NonFatalError::RowDirty(_, _) => metric.inc_row_dirty(),
                            NonFatalError::RowDeleted(_, _) => metric.inc_row_deleted(),
                            // NonFatalError::HitList(e) => match e {
                            //     HitListError::TransactionInHitList(_) => metric.inc_hit(),
                            //     HitListError::PredecessorAborted(_) => metric.inc_pur_aborted(),
                            //     HitListError::PredecessorActive(_) => metric.inc_pur_active(),
                            //     _ => {}
                            // },
                            _ => {}
                        },
                        ProtocolAbortBreakdown::OptimisedHitList(ref mut metric) => match reason {
                            NonFatalError::RowDirty(_, _) => metric.inc_row_dirty(),
                            NonFatalError::RowDeleted(_, _) => metric.inc_row_deleted(),
                            // NonFatalError::OptimisedHitListError(e) => match e {
                            //     OptimisedHitListError::Hit(_) => metric.inc_hit(),
                            //     OptimisedHitListError::PredecessorAborted(_) => {
                            //         metric.inc_pur_aborted()
                            //     }
                            //     OptimisedHitListError::PredecessorActive(_) => {
                            //         metric.inc_pur_active()
                            //     }
                            // },
                            _ => {}
                        },

                        ProtocolAbortBreakdown::SerializationGraph(ref mut metric) => {
                            match reason {
                                NonFatalError::RowDirty(_, _) => metric.inc_row_dirty(),
                                NonFatalError::RowDeleted(_, _) => metric.inc_row_deleted(),
                                // NonFatalError::SerializationGraphTesting(e) => {
                                //     if let SerializationGraphTestingError::ParentAborted = e {
                                //         metric.inc_parent_aborted();
                                //     } else {
                                //         tracing::info!("Other: {:?}", e);
                                //     }
                                // }
                                _ => {}
                            }
                        }
                        ProtocolAbortBreakdown::BasicSerializationGraph(ref mut metric) => {
                            match reason {
                                NonFatalError::RowDeleted(_, _) => metric.inc_row_deleted(),
                                NonFatalError::BasicSerializationGraphTesting(e) => match e {
                                    BasicSerializationGraphTestingError::CascadingAbort => {
                                        metric.inc_cascading_abort();
                                    }
                                    BasicSerializationGraphTestingError::CycleFound => {
                                        metric.inc_cycle_found();
                                    }
                                    _ => tracing::info!("Other: {:?}", e),
                                },
                                _ => {}
                            }
                        }
                        ProtocolAbortBreakdown::TwoPhaseLocking(ref mut metric) => {
                            // if let NonFatalError::TwoPhaseLocking(e) = reason {
                            //     match e {
                            //         TwoPhaseLockingError::ReadLockRequestDenied(_) => {
                            //             metric.inc_read_lock_denied()
                            //         }
                            //         TwoPhaseLockingError::WriteLockRequestDenied(_) => {
                            //             metric.inc_write_lock_denied()
                            //         }
                            //         _ => {}
                            //     }
                            // }
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
            // "tatp" => {
            //     let name = workload.to_string();
            //     let mut transactions = vec![];

            //     for transaction in TatpTransaction::iter() {
            //         let metrics = TransactionMetrics::new(Transaction::Tatp(transaction));
            //         transactions.push(metrics);
            //     }
            //     WorkloadBreakdown { name, transactions }
            // }
            "smallbank" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in SmallBankTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::SmallBank(transaction));
                    transactions.push(metrics);
                }
                WorkloadBreakdown { name, transactions }
            }
            // "acid" => {
            //     let name = workload.to_string();
            //     let mut transactions = vec![];
            //     for transaction in AcidTransaction::iter() {
            //         let metrics = TransactionMetrics::new(Transaction::Acid(transaction));
            //         transactions.push(metrics);
            //     }
            //     WorkloadBreakdown { name, transactions }
            // }
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
    #[serde(skip_serializing)]
    raw_latency: Vec<f64>,
    min: Option<f64>,
    max: Option<f64>,
    mean: Option<f64>,
    pc50: Option<f64>,
    pc90: Option<f64>,
    pc95: Option<f64>,
    pc99: Option<f64>,
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
            min: None,
            max: None,
            mean: None,
            pc50: None,
            pc90: None,
            pc95: None,
            pc99: None,
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

    /// Set median value
    fn calculate_latency_summary(&mut self) {
        if !self.raw_latency.is_empty() {
            self.min = Some(self.raw_latency.min() * 1000.0);
            self.max = Some(self.raw_latency.max() * 1000.0);
            self.mean = Some(self.raw_latency.mean() * 1000.0);
            self.pc50 = Some(self.raw_latency.quantile(0.5) * 1000.0);
            self.pc90 = Some(self.raw_latency.quantile(0.9) * 1000.0);
            self.pc95 = Some(self.raw_latency.quantile(0.95) * 1000.0);
            self.pc99 = Some(self.raw_latency.quantile(0.99) * 1000.0);
        }
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

    /// Row not found in the database,
    row_not_found: u32,

    /// Protocol specific aborts reasons.
    protocol_specific: ProtocolAbortBreakdown,

    /// Workload specific abort reasons -- used for constraint violations, e.g., insufficient funds
    workload_specific: Option<WorkloadAbortBreakdown>,
}

impl AbortBreakdown {
    /// Create new holder for protocol specific abort reasons.
    fn new(protocol: &str, workload: &str) -> AbortBreakdown {
        let protocol_specific = match protocol {
            "sgt" => ProtocolAbortBreakdown::SerializationGraph(SerializationGraphReasons::new()),
            "basic-sgt" => ProtocolAbortBreakdown::BasicSerializationGraph(
                BasicSerializationGraphReasons::new(),
            ),
            "2pl" => ProtocolAbortBreakdown::TwoPhaseLocking(TwoPhaseLockingReasons::new()),
            "hit" => ProtocolAbortBreakdown::HitList(HitListReasons::new()),
            "opt-hit" => ProtocolAbortBreakdown::OptimisedHitList(HitListReasons::new()),
            _ => unimplemented!(),
        };

        let workload_specific;
        if workload == "smallbank" {
            workload_specific = Some(WorkloadAbortBreakdown::SmallBank(
                SmallBankConstraints::new(),
            ));
        } else {
            workload_specific = None;
        }

        AbortBreakdown {
            row_already_exists: 0,
            row_not_found: 0,
            protocol_specific,
            workload_specific,
        }
    }

    /// Merge abort breakdowns.
    fn merge(&mut self, other: AbortBreakdown) {
        self.row_already_exists += other.row_already_exists;
        self.row_not_found += other.row_not_found;

        match self.protocol_specific {
            ProtocolAbortBreakdown::HitList(ref mut reasons) => {
                if let ProtocolAbortBreakdown::HitList(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("abort breakdowns do not match");
                }
            }
            ProtocolAbortBreakdown::OptimisedHitList(ref mut reasons) => {
                if let ProtocolAbortBreakdown::OptimisedHitList(other_reasons) =
                    other.protocol_specific
                {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            ProtocolAbortBreakdown::SerializationGraph(ref mut reasons) => {
                if let ProtocolAbortBreakdown::SerializationGraph(other_reasons) =
                    other.protocol_specific
                {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            ProtocolAbortBreakdown::BasicSerializationGraph(ref mut reasons) => {
                if let ProtocolAbortBreakdown::BasicSerializationGraph(other_reasons) =
                    other.protocol_specific
                {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            ProtocolAbortBreakdown::TwoPhaseLocking(ref mut reasons) => {
                if let ProtocolAbortBreakdown::TwoPhaseLocking(other_reasons) =
                    other.protocol_specific
                {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
        }

        // merging abort breakdowns
        if let Some(ref mut workload_aborts) = self.workload_specific {
            match workload_aborts {
                WorkloadAbortBreakdown::SmallBank(constraints) => {
                    if let Some(other_workload_aborts) = other.workload_specific {
                        match other_workload_aborts {
                            WorkloadAbortBreakdown::SmallBank(other_constraints) => {
                                constraints.merge(other_constraints);
                            }
                        }
                    }
                }
            }
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
enum WorkloadAbortBreakdown {
    SmallBank(SmallBankConstraints),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SmallBankConstraints {
    /// Insufficient funds
    insufficient_funds: u32,
}

impl SmallBankConstraints {
    /// Create new holder for smallbank constraint violations
    fn new() -> Self {
        SmallBankConstraints {
            insufficient_funds: 0,
        }
    }

    /// Increment insufficient funds
    fn inc_insufficient_funds(&mut self) {
        self.insufficient_funds += 1;
    }

    fn merge(&mut self, other: SmallBankConstraints) {
        self.insufficient_funds += other.insufficient_funds;
    }
}

/// Protocol specific reasons for aborts.
#[derive(Serialize, Deserialize, Debug, Clone)]
enum ProtocolAbortBreakdown {
    TwoPhaseLocking(TwoPhaseLockingReasons),
    SerializationGraph(SerializationGraphReasons),
    BasicSerializationGraph(BasicSerializationGraphReasons),
    HitList(HitListReasons),
    OptimisedHitList(HitListReasons),
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
struct BasicSerializationGraphReasons {
    /// Transaction attempted to read or modify a row already marked for deletion.
    row_deleted: u32,

    /// Transaction aborted as conflicting transaction was aborted (cascading abort).
    cascading_abort: u32,

    /// Transaction was in a cycle.
    cycle_found: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct HitListReasons {
    /// Transaction attempted to modify a row already modified.
    row_dirty: u32,

    /// Transaction attempted to read or modify a row already marked for deletion.
    row_deleted: u32,

    /// Transaction was hit.
    hit: u32,

    /// Predecessor upon read was active.
    pur_active: u32,

    /// Predecessor upon read was aborted.
    pur_aborted: u32,
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

impl BasicSerializationGraphReasons {
    /// Create new holder for SGT abort reasons.
    fn new() -> BasicSerializationGraphReasons {
        BasicSerializationGraphReasons {
            row_deleted: 0,
            cascading_abort: 0,
            cycle_found: 0,
        }
    }

    /// Increment row deleted counter.
    fn inc_row_deleted(&mut self) {
        self.row_deleted += 1;
    }

    fn inc_cascading_abort(&mut self) {
        self.cascading_abort += 1;
    }

    fn inc_cycle_found(&mut self) {
        self.cycle_found += 1;
    }

    fn merge(&mut self, other: BasicSerializationGraphReasons) {
        self.row_deleted += other.row_deleted;
        self.cascading_abort += other.cascading_abort;
        self.cycle_found += other.cycle_found;
    }
}

impl HitListReasons {
    /// Create new holder for HIT abort reasons.
    fn new() -> HitListReasons {
        HitListReasons {
            row_dirty: 0,
            row_deleted: 0,
            hit: 0,
            pur_aborted: 0,
            pur_active: 0,
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

    /// Increment hit counter.
    fn inc_hit(&mut self) {
        self.hit += 1;
    }

    /// Increment pur active counter.
    fn inc_pur_active(&mut self) {
        self.pur_active += 1;
    }

    /// Increment pur aborted counter.
    fn inc_pur_aborted(&mut self) {
        self.pur_aborted += 1;
    }

    /// Merge hit list reasons.
    fn merge(&mut self, other: HitListReasons) {
        self.row_dirty += other.row_dirty;
        self.row_deleted += other.row_deleted;
        self.hit += other.hit;
        self.pur_aborted += other.pur_aborted;
        self.pur_active += other.pur_active;
    }
}
