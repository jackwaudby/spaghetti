use crate::common::error::NonFatalError;
use crate::common::message::{Outcome, Transaction};
use crate::scheduler::owh::error::OptimisedWaitHitError;
use crate::scheduler::sgt::error::SerializationGraphError;
use crate::workloads::smallbank::SmallBankTransaction;

use config::Config;
use serde::{Deserialize, Serialize};
use serde_json::json;
//use statrs::statistics::OrderStatistics;
//use statrs::statistics::{Max, Mean, Min};
use std::fs::{self, OpenOptions};
use std::path::Path;

use std::time::Duration;
use std::time::Instant;
use strum::IntoEnumIterator;

#[derive(Debug)]
pub struct GlobalStatistics {
    scale_factor: u64,
    data_generation: Option<Duration>,
    load_time: Option<Duration>,
    warmup: u32,
    start: Option<Instant>,
    end: Option<Duration>,
    cores: u32,
    total_time: u128,
    wait_manager: u128,
    latency: u128,
    protocol: String,
    workload: String,
    transaction_breakdown: TransactionBreakdown,
    abort_breakdown: AbortBreakdown,
}

impl GlobalStatistics {
    pub fn new(config: &Config) -> GlobalStatistics {
        let scale_factor = config.get_int("scale_factor").unwrap() as u64;
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let warmup = config.get_int("warmup").unwrap() as u32;
        let cores = config.get_int("workers").unwrap() as u32;
        let transaction_breakdown = TransactionBreakdown::new(&workload);
        let abort_breakdown = AbortBreakdown::new(&protocol, &workload);

        GlobalStatistics {
            scale_factor,
            data_generation: None,
            load_time: None,
            warmup,
            start: None,
            end: None,
            protocol,
            workload,
            total_time: 0,
            wait_manager: 0,
            latency: 0,
            cores,
            transaction_breakdown,
            abort_breakdown,
        }
    }

    pub fn set_data_generation(&mut self, duration: Duration) {
        self.data_generation = Some(duration);
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

    pub fn merge_into(&mut self, local: LocalStatistics) {
        self.total_time += local.total_time;
        self.wait_manager += local.wait_manager;
        self.latency += local.latency;

        self.transaction_breakdown
            .merge(local.transaction_breakdown);
        self.abort_breakdown.merge(local.abort_breakdown);
    }

    pub fn write_to_file(&mut self) {
        let path;
        let file;
        if self.workload.as_str() == "acid" {
            path = "./results/todo".to_string();
            file = "./results/todo/todo.json".to_string();
            // path = format!(
            //     "./results/{}/{}/",
            //     self.workload,
            //     self.anomaly.as_ref().unwrap()
            // );
            // file = format!(
            //     "./results/{}/{}/{}-sf{}.json",
            //     self.workload,
            //     self.anomaly.as_ref().unwrap(),
            //     self.protocol,
            //     self.scale_factor
            // );
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
        let mut restarted = 0;
        let mut aborted = 0;

        //  let mut raw_latency = vec![];
        for transaction in &mut self.transaction_breakdown.transactions {
            completed += transaction.completed;
            committed += transaction.committed;
            restarted += transaction.restarted;
            aborted += transaction.aborted;

            //   latency += transaction.latency;
            //   transaction.calculate_latency_summary();
            //            let mut temp = transaction.raw_latency.clone();
            //          raw_latency.append(&mut temp);
        }

        let abort_rate;
        let throughput;

        match self.abort_breakdown.workload_specific {
            WorkloadAbortBreakdown::Tatp(ref _reason) => {
                // applicaton_aborts = aborts - reason.row_not_found;
                // protocol_aborts = aborts - applicaton_aborts;
                // // missing data counts as committed
                // let tatp_success = committed + applicaton_aborts;
                abort_rate = 0.0; //(protocol_aborts as f64 / completed as f64) * 100.0;
                throughput = 0.0; //tatp_success as f64 / ((self.total_time as f64 / 1000000.0) / 1000.0)
            }

            WorkloadAbortBreakdown::SmallBank(ref _reason) => {
                let total = (committed + restarted) as f64;
                abort_rate = restarted as f64 / total * 100.0;
                throughput = committed as f64
                    / (((self.total_time as f64 / 1000000.0) / 1000.0) / self.cores as f64)
            } // _ => {
              //     abort_rate = (aborted as f64 / completed as f64) * 100.0;
              //     throughput = committed as f64 / self.end.unwrap().as_secs() as f64
              // }
        }

        let mean = self.latency as f64 / (completed as f64) / 1000000.0;

        // let min = raw_latency.min() * 1000.0;
        // let max = raw_latency.max() * 1000.0;
        // let mean = raw_latency.mean() * 1000.0;
        // let pc50 = raw_latency.quantile(0.5) * 1000.0;
        // let pc90 = raw_latency.quantile(0.9) * 1000.0;
        // let pc95 = raw_latency.quantile(0.95) * 1000.0;
        // let pc99 = raw_latency.quantile(0.99) * 1000.0;

        let overview = json!({
            "sf": self.scale_factor,
            "load": self.data_generation.unwrap().as_secs(),
            "cores": self.cores,
            "protocol": self.protocol,
            "workload": self.workload,
            "total_duration": self.end.unwrap().as_secs(),
            "warmup": self.warmup,
            "completed": completed,
            "committed": committed,
            "restarted": restarted,
            "aborted": aborted,
            "abort_rate": format!("{:.3}", abort_rate),
            "throughput": format!("{:.3}", throughput),
            "latency": mean,
            // "min": min,
            // "max": max,
            // "mean": mean,
            // "50th_percentile": pc50,
            // "90th_percentile": pc90,
            // "95th_percentile": pc95,
            // "99th_percentile": pc99,
            "abort_breakdown": self.abort_breakdown,
            "transaction_breakdown": self.transaction_breakdown,
        });

        serde_json::to_writer_pretty(file, &overview).unwrap();

        let pr = json!({
            "sf": self.scale_factor,
            "cores": self.cores,
            "protocol": self.protocol,
            "workload": self.workload,
            "duration(ms)":  format!("{:.0}",self.end.unwrap().as_nanos() as f64 / 1000000.0),
            "total_time(ms)":  format!("{:.0}", (self.total_time as f64 / 1000000.0)),
            "completed": completed,
            "committed": committed,
            "restarted": restarted,
            "aborted": aborted,
            "throughput": format!("{:.3}", throughput),
            "abort_rate": format!("{:.3}", abort_rate),
            "latency(ms)": format!("{:.0}", (self.latency as f64 / 1000000.0)),
            "av_latency(ms)":format!("{:.3}", mean),
            "wait_manager(ms)": format!("{:.0}", (self.wait_manager as f64 / 1000000.0)),
        });
        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());
    }
}

#[derive(Debug, Clone)]
pub struct LocalStatistics {
    core_id: u32,
    total_time: u128,
    wait_manager: u128,
    latency: u128,
    transaction_breakdown: TransactionBreakdown,
    abort_breakdown: AbortBreakdown,
}

impl LocalStatistics {
    pub fn new(core_id: u32, workload: &str, protocol: &str) -> LocalStatistics {
        let transaction_breakdown = TransactionBreakdown::new(workload);
        let abort_breakdown = AbortBreakdown::new(protocol, workload);

        LocalStatistics {
            core_id,
            total_time: 0,
            wait_manager: 0,
            latency: 0,
            transaction_breakdown,
            abort_breakdown,
        }
    }

    pub fn get_core_id(&self) -> u32 {
        self.core_id
    }

    pub fn stop_worker(&mut self, start: Instant) {
        self.total_time = start.elapsed().as_nanos();
    }

    pub fn stop_latency(&mut self, start: Instant) {
        self.latency += start.elapsed().as_nanos();
    }

    pub fn stop_wait_manager(&mut self, start: Instant) {
        self.wait_manager += start.elapsed().as_nanos();
    }

    pub fn record(&mut self, transaction: Transaction, outcome: Outcome, restart: bool) {
        self.transaction_breakdown
            .record(transaction, outcome.clone(), restart);

        if let Outcome::Aborted { reason } = outcome {
            use WorkloadAbortBreakdown::*;
            match self.abort_breakdown.workload_specific {
                SmallBank(ref mut metric) => {
                    if let NonFatalError::SmallBankError(_) = reason {
                        metric.inc_insufficient_funds();
                    }
                }

                Tatp(ref mut metric) => match reason {
                    NonFatalError::RowNotFound(_, _) => {
                        metric.inc_not_found();
                    }
                    NonFatalError::RowAlreadyExists(_, _) => {
                        metric.inc_already_exists();
                    }
                    _ => unimplemented!(),
                },
            }

            use ProtocolAbortBreakdown::*;
            match self.abort_breakdown.protocol_specific {
                SerializationGraph(ref mut metric) => {
                    if let NonFatalError::SerializationGraph(sge) = reason {
                        match sge {
                            SerializationGraphError::CascadingAbort => metric.inc_cascading_abort(),
                            SerializationGraphError::CycleFound => metric.inc_cycle_found(),
                        }
                    }
                }

                OptimisticWaitHit(ref mut metric) => match reason {
                    NonFatalError::OptimisedWaitHitError(owhe) => match owhe {
                        OptimisedWaitHitError::Hit(_) => metric.inc_hit(),
                        OptimisedWaitHitError::PredecessorAborted(_, _) => metric.inc_pur_aborted(),
                        OptimisedWaitHitError::PredecessorActive(_) => metric.inc_pur_active(),
                    },
                    NonFatalError::RowDirty(_, _) => metric.inc_row_dirty(),
                    _ => {}
                },
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TransactionBreakdown {
    name: String,
    transactions: Vec<TransactionMetrics>,
}

impl TransactionBreakdown {
    fn new(workload: &str) -> Self {
        match workload {
            "smallbank" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in SmallBankTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::SmallBank(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }
            _ => unimplemented!(),
        }
    }

    fn record(&mut self, transaction: Transaction, outcome: Outcome, restarted: bool) {
        let ind = self
            .transactions
            .iter()
            .position(|x| x.transaction == transaction)
            .unwrap();

        if restarted {
            self.transactions[ind].inc_restarted()
        } else {
            match outcome {
                Outcome::Committed { .. } => self.transactions[ind].inc_committed(),
                Outcome::Aborted { .. } => self.transactions[ind].inc_aborted(),
            }
        }

        //  self.transactions[ind].add_latency(latency.unwrap().as_secs_f64());
    }

    fn merge(&mut self, other: TransactionBreakdown) {
        assert!(self.name == other.name);

        for holder in other.transactions {
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
    restarted: u32,
    aborted: u32,
    // latency: f64,
    // #[serde(skip_serializing)]
    // raw_latency: Vec<f64>,
    // min: Option<f64>,
    // max: Option<f64>,
    // mean: Option<f64>,
    // pc50: Option<f64>,
    // pc90: Option<f64>,
    // pc95: Option<f64>,
    // pc99: Option<f64>
}

impl TransactionMetrics {
    fn new(transaction: Transaction) -> Self {
        TransactionMetrics {
            transaction,
            completed: 0,
            committed: 0,
            restarted: 0,
            aborted: 0,
            //       latency: 0.0,
            // raw_latency: vec![],
            // min: None,
            // max: None,
            // mean: None,
            // pc50: None,
            // pc90: None,
            // pc95: None,
            // pc99: None,
        }
    }

    fn inc_committed(&mut self) {
        self.committed += 1;
        self.completed += 1;
    }

    fn inc_aborted(&mut self) {
        self.completed += 1;
        self.aborted += 1;
    }

    fn inc_restarted(&mut self) {
        self.restarted += 1;
    }

    // fn add_latency(&mut self, latency: f64) {
    //     self.latency += latency;
    //     //  self.raw_latency.push(latency);
    // }

    //    fn calculate_latency_summary(&mut self) {
    // if !self.raw_latency.is_empty() {
    //     self.min = Some(self.raw_latency.min() * 1000.0);
    //     self.max = Some(self.raw_latency.max() * 1000.0);
    //     self.mean = Some(self.raw_latency.mean() * 1000.0);
    //     self.pc50 = Some(self.raw_latency.quantile(0.5) * 1000.0);
    //     self.pc90 = Some(self.raw_latency.quantile(0.9) * 1000.0);
    //     self.pc95 = Some(self.raw_latency.quantile(0.95) * 1000.0);
    //     self.pc99 = Some(self.raw_latency.quantile(0.99) * 1000.0);
    // }
    //  }

    fn merge(&mut self, other: TransactionMetrics) {
        assert!(self.transaction == other.transaction);

        self.completed += other.completed;
        self.committed += other.committed;
        self.aborted += other.aborted;
        self.restarted += other.restarted;
        //    self.latency += other.latency;
        //      self.raw_latency.append(&mut other.raw_latency);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AbortBreakdown {
    protocol_specific: ProtocolAbortBreakdown,
    workload_specific: WorkloadAbortBreakdown,
}

impl AbortBreakdown {
    fn new(protocol: &str, workload: &str) -> AbortBreakdown {
        let protocol_specific = match protocol {
            "sgt" => ProtocolAbortBreakdown::SerializationGraph(SerializationGraphReasons::new()),
            "owh" => ProtocolAbortBreakdown::OptimisticWaitHit(HitListReasons::new()),
            _ => unimplemented!(),
        };

        let workload_specific = match workload {
            "smallbank" => WorkloadAbortBreakdown::SmallBank(SmallBankReasons::new()),
            "tatp" => WorkloadAbortBreakdown::Tatp(TatpReasons::new()),
            _ => unimplemented!(),
        };

        AbortBreakdown {
            protocol_specific,
            workload_specific,
        }
    }

    fn merge(&mut self, other: AbortBreakdown) {
        use ProtocolAbortBreakdown::*;
        use WorkloadAbortBreakdown::*;
        match self.protocol_specific {
            SerializationGraph(ref mut reasons) => {
                if let SerializationGraph(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            OptimisticWaitHit(ref mut reasons) => {
                if let OptimisticWaitHit(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
        }

        match self.workload_specific {
            SmallBank(ref mut reasons) => {
                if let SmallBank(other_reasons) = other.workload_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("workload abort breakdowns do not match");
                }
            }
            Tatp(ref mut reasons) => {
                if let Tatp(other_reasons) = other.workload_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("workload abort breakdowns do not match");
                }
            }
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
enum WorkloadAbortBreakdown {
    SmallBank(SmallBankReasons),
    Tatp(TatpReasons),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TatpReasons {
    row_already_exists: u32,
    row_not_found: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SmallBankReasons {
    insufficient_funds: u32,
}

impl TatpReasons {
    fn new() -> Self {
        TatpReasons {
            row_already_exists: 0,
            row_not_found: 0,
        }
    }

    fn inc_already_exists(&mut self) {
        self.row_already_exists += 1;
    }

    fn inc_not_found(&mut self) {
        self.row_not_found += 1;
    }

    fn merge(&mut self, other: TatpReasons) {
        self.row_already_exists += other.row_already_exists;
        self.row_not_found += other.row_not_found;
    }
}

impl SmallBankReasons {
    fn new() -> Self {
        SmallBankReasons {
            insufficient_funds: 0,
        }
    }

    fn inc_insufficient_funds(&mut self) {
        self.insufficient_funds += 1;
    }

    fn merge(&mut self, other: SmallBankReasons) {
        self.insufficient_funds += other.insufficient_funds;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum ProtocolAbortBreakdown {
    SerializationGraph(SerializationGraphReasons),
    OptimisticWaitHit(HitListReasons),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SerializationGraphReasons {
    cascading_abort: u32,
    cycle_found: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct HitListReasons {
    row_dirty: u32,
    hit: u32,
    pur_active: u32,
    pur_aborted: u32,
}

impl SerializationGraphReasons {
    fn new() -> Self {
        SerializationGraphReasons {
            cascading_abort: 0,
            cycle_found: 0,
        }
    }

    fn inc_cascading_abort(&mut self) {
        self.cascading_abort += 1;
    }

    fn inc_cycle_found(&mut self) {
        self.cycle_found += 1;
    }

    fn merge(&mut self, other: SerializationGraphReasons) {
        self.cascading_abort += other.cascading_abort;
        self.cycle_found += other.cycle_found;
    }
}
impl HitListReasons {
    fn new() -> HitListReasons {
        HitListReasons {
            row_dirty: 0,
            hit: 0,
            pur_aborted: 0,
            pur_active: 0,
        }
    }

    fn inc_row_dirty(&mut self) {
        self.row_dirty += 1;
    }

    fn inc_hit(&mut self) {
        self.hit += 1;
    }

    fn inc_pur_active(&mut self) {
        self.pur_active += 1;
    }

    fn inc_pur_aborted(&mut self) {
        self.pur_aborted += 1;
    }

    fn merge(&mut self, other: HitListReasons) {
        self.row_dirty += other.row_dirty;
        self.hit += other.hit;
        self.pur_aborted += other.pur_aborted;
        self.pur_active += other.pur_active;
    }
}
