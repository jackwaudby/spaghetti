use crate::common::error::NonFatalError;
use crate::common::message::{Message, Outcome, Transaction};
use crate::scheduler::error::{MixedSerializationGraphError, SerializationGraphError};
use crate::scheduler::mtpl::error::MixedTwoPhaseLockingError;
use crate::scheduler::owh::error::OptimisedWaitHitError;
use crate::scheduler::tpl::error::TwoPhaseLockingError;
use crate::scheduler::wh::error::WaitHitError;
use crate::workloads::acid::AcidTransaction;
use crate::workloads::dummy::DummyTransaction;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::tatp::TatpTransaction;
use crate::workloads::IsolationLevel;

use config::Config;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::path::Path;

use std::time::Duration;
use std::time::Instant;
use strum::IntoEnumIterator;

#[derive(Debug)]
pub struct GlobalStatistics {
    scale_factor: u64,
    data_generation: Option<Duration>,
    start: Option<Instant>,
    end: Option<Duration>,
    cores: u32,
    total_time: u128,
    latency: u128,
    protocol: String,
    workload: String,
    transaction_breakdown: TransactionBreakdown,
    abort_breakdown: AbortBreakdown,
    anomaly: Option<String>,
}

impl GlobalStatistics {
    pub fn new(config: &Config) -> Self {
        let scale_factor = config.get_int("scale_factor").unwrap() as u64;
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let cores = config.get_int("cores").unwrap() as u32;
        let transaction_breakdown = TransactionBreakdown::new(&workload);
        let abort_breakdown = AbortBreakdown::new(&protocol, &workload);
        let anomaly;
        if let Ok(a) = config.get_str("anomaly") {
            anomaly = Some(a);
        } else {
            anomaly = None;
        }

        GlobalStatistics {
            scale_factor,
            data_generation: None,
            start: None,
            end: None,
            protocol,
            workload,
            total_time: 0,
            latency: 0,
            cores,
            transaction_breakdown,
            abort_breakdown,
            anomaly,
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
        self.latency += local.latency;

        self.transaction_breakdown
            .merge(local.transaction_breakdown);
        self.abort_breakdown.merge(local.abort_breakdown);
    }

    pub fn write_to_file(&mut self) {
        let path; // construct file path
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
                "./results/{}/{}-sf{}-{}.json",
                self.workload, self.protocol, self.scale_factor, self.cores
            );
        }

        if !Path::new(&path).exists() {
            fs::create_dir_all(&path).unwrap(); // create dir if does not exist
        }

        if Path::new(&file).exists() {
            fs::remove_file(&file).unwrap(); // remove file if already exists
        }

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&file)
            .expect("cannot open file"); // create new file

        let mut completed = 0;
        let mut committed = 0;
        let mut aborted = 0; // total aborts

        for transaction in &mut self.transaction_breakdown.transactions {
            completed += transaction.completed;
            committed += transaction.committed;
            aborted += transaction.aborted;
        }

        assert_eq!(completed, committed + aborted);

        let internal_aborts = match self.abort_breakdown.workload_specific {
            WorkloadAbortBreakdown::Tatp(ref reasons) => {
                // row not found is the only internal abort
                // but these count as committed in this workload
                committed += reasons.row_not_found;
                aborted -= reasons.row_not_found;
                0
            }
            WorkloadAbortBreakdown::SmallBank(ref reasons) => reasons.insufficient_funds,
            WorkloadAbortBreakdown::Acid(ref reasons) => reasons.non_serializable,
            WorkloadAbortBreakdown::Dummy(ref reasons) => reasons.non_serializable,
        }; // aborts due to integrity constraints/manual aborts

        let external_aborts = match self.abort_breakdown.protocol_specific {
            ProtocolAbortBreakdown::SerializationGraph(ref reasons) => {
                reasons.cascading_abort + reasons.cycle_found
            }
            ProtocolAbortBreakdown::MixedSerializationGraph(ref reasons) => {
                reasons.cascading_abort + reasons.cycle_found
            }
            ProtocolAbortBreakdown::StdMixedSerializationGraph(ref reasons) => {
                reasons.cascading_abort + reasons.cycle_found
            }
            ProtocolAbortBreakdown::RelMixedSerializationGraph(ref reasons) => {
                reasons.cascading_abort + reasons.cycle_found
            }
            ProtocolAbortBreakdown::EarlyMixedSerializationGraph(ref reasons) => {
                reasons.cascading_abort + reasons.cycle_found
            }
            ProtocolAbortBreakdown::WaitHit(ref reasons) => {
                reasons.hit + reasons.pur_active + reasons.row_dirty + reasons.pur_aborted
            }
            ProtocolAbortBreakdown::OptimisticWaitHit(ref reasons) => {
                reasons.hit + reasons.pur_active + reasons.row_dirty + reasons.pur_aborted
            }
            ProtocolAbortBreakdown::OptimisticWaitHitTransactionTypes(ref reasons) => {
                reasons.hit + reasons.pur_active + reasons.row_dirty + reasons.pur_aborted
            }
            ProtocolAbortBreakdown::TwoPhaseLocking(ref reasons) => {
                reasons.read_lock_denied + reasons.write_lock_denied
            }
            ProtocolAbortBreakdown::MixedTwoPhaseLocking(ref reasons) => {
                reasons.read_lock_denied + reasons.write_lock_denied
            }
            ProtocolAbortBreakdown::NoConcurrencyControl => 0,
        }; // aborts due to system implementation

        assert_eq!(aborted, external_aborts + internal_aborts);

        let abort_rate = external_aborts as f64 / (committed + external_aborts) as f64;
        let throughput = committed as f64
            / (((self.total_time as f64 / 1000000.0) / 1000.0) / self.cores as f64);
        let mean = self.latency as f64 / (completed as f64) / 1000000.0;

        // detailed execution summary
        let overview = json!({
            "workload": self.workload,
            "sf": self.scale_factor,
            "load": self.data_generation.unwrap().as_millis() as u64,
            "cores": self.cores,
            "protocol": self.protocol,
            "total_duration": self.end.unwrap().as_secs(),
            "completed": completed,
            "committed": committed,
            "aborted": aborted,
            "internal_aborts": internal_aborts,
            "external_aborts": external_aborts,
            "abort_rate": format!("{:.3}", abort_rate),
            "throughput": format!("{:.3}", throughput),
            "latency": mean,
            "abort_breakdown": self.abort_breakdown,
            "transaction_breakdown": self.transaction_breakdown,
        });
        serde_json::to_writer_pretty(file, &overview).unwrap();

        // console output
        let pr = json!({
            "workload": self.workload,
            "sf": self.scale_factor,
            "cores": self.cores,
            "protocol": self.protocol,
            "total_time(ms)":  format!("{:.0}", (self.total_time as f64 / 1000000.0)),
            "completed": completed,
            "committed": committed,
            "aborted": aborted,
            "internal_aborts": internal_aborts,
            "external_aborts": external_aborts,
            "throughput": format!("{:.3}", throughput),
            "abort_rate": format!("{:.3}", abort_rate),
            "av_latency(ms)":format!("{:.3}", mean),
        });
        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());

        // results.csv
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open("./results.csv")
            .unwrap();

        let mut wtr = csv::Writer::from_writer(file);

        wtr.serialize((
            self.scale_factor,
            &self.protocol,
            &self.workload,
            self.cores,
            (self.total_time as f64 / 1000000.0), // ms
            committed,
            external_aborts,
            internal_aborts,
            (self.latency as f64 / 1000000.0), // ms
        ))
        .unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct LocalStatistics {
    core_id: u32,
    total_time: u128,
    latency: u128,
    transaction_breakdown: TransactionBreakdown,
    abort_breakdown: AbortBreakdown,
}

impl LocalStatistics {
    pub fn new(core_id: u32, workload: &str, protocol: &str) -> Self {
        let transaction_breakdown = TransactionBreakdown::new(workload);
        let abort_breakdown = AbortBreakdown::new(protocol, workload);

        LocalStatistics {
            core_id,
            total_time: 0,
            latency: 0,
            transaction_breakdown,
            abort_breakdown,
        }
    }

    pub fn get_core_id(&self) -> u32 {
        self.core_id
    }

    /// Per-thread latency
    pub fn stop_worker(&mut self, start: Instant) {
        self.total_time = start.elapsed().as_nanos();
    }

    /// Per-transaction latency
    pub fn stop_latency(&mut self, start: Instant) {
        self.latency += start.elapsed().as_nanos();
    }

    pub fn record(&mut self, response: &Message) {
        if let Message::Response {
            transaction,
            outcome,
            isolation,
            ..
        } = response
        {
            self.transaction_breakdown.record(transaction, outcome);

            if let Outcome::Aborted(reason) = outcome {
                use WorkloadAbortBreakdown::*;
                match self.abort_breakdown.workload_specific {
                    SmallBank(ref mut metric) => {
                        if let NonFatalError::SmallBankError(_) = reason {
                            metric.inc_insufficient_funds();
                        }
                    }

                    Acid(ref mut metric) => {
                        if let NonFatalError::NonSerializable = reason {
                            metric.inc_non_serializable();
                        }
                    }

                    Dummy(ref mut metric) => {
                        if let NonFatalError::NonSerializable = reason {
                            metric.inc_non_serializable();
                        }
                    }

                    Tatp(ref mut metric) => {
                        if let NonFatalError::RowNotFound(_, _) = reason {
                            metric.inc_not_found();
                        }
                    }
                }

                use ProtocolAbortBreakdown::*;
                match self.abort_breakdown.protocol_specific {
                    SerializationGraph(ref mut metric) => {
                        if let NonFatalError::SerializationGraph(sge) = reason {
                            match sge {
                                SerializationGraphError::CascadingAbort => {
                                    metric.inc_cascading_abort()
                                }
                                SerializationGraphError::CycleFound => metric.inc_cycle_found(),
                            }

                            match isolation {
                                IsolationLevel::ReadCommitted => metric.inc_read_committed(),
                                IsolationLevel::ReadUncommitted => metric.inc_read_uncommitted(),
                                IsolationLevel::Serializable => metric.inc_serializable(),
                            }
                        }
                    }

                    MixedSerializationGraph(ref mut metric) => {
                        if let NonFatalError::MixedSerializationGraph(sge) = reason {
                            match sge {
                                MixedSerializationGraphError::CascadingAbort => {
                                    metric.inc_cascading_abort()
                                }
                                MixedSerializationGraphError::CycleFound => {
                                    metric.inc_cycle_found()
                                }
                            }
                            match isolation {
                                IsolationLevel::ReadCommitted => metric.inc_read_committed(),
                                IsolationLevel::ReadUncommitted => metric.inc_read_uncommitted(),
                                IsolationLevel::Serializable => metric.inc_serializable(),
                            }
                        }
                    }

                    StdMixedSerializationGraph(ref mut metric) => {
                        if let NonFatalError::SerializationGraph(sge) = reason {
                            match sge {
                                SerializationGraphError::CascadingAbort => {
                                    metric.inc_cascading_abort()
                                }
                                SerializationGraphError::CycleFound => metric.inc_cycle_found(),
                            }
                            match isolation {
                                IsolationLevel::ReadCommitted => metric.inc_read_committed(),
                                IsolationLevel::ReadUncommitted => metric.inc_read_uncommitted(),
                                IsolationLevel::Serializable => metric.inc_serializable(),
                            }
                        }
                    }

                    WaitHit(ref mut metric) => match reason {
                        NonFatalError::WaitHitError(owhe) => match owhe {
                            WaitHitError::TransactionInHitList(_) => metric.inc_hit(),
                            WaitHitError::PredecessorAborted(_) => metric.inc_pur_aborted(),
                            WaitHitError::PredecessorActive(_) => metric.inc_pur_active(),
                        },
                        NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
                        _ => {}
                    },

                    OptimisticWaitHit(ref mut metric) => match reason {
                        NonFatalError::OptimisedWaitHitError(owhe) => match owhe {
                            OptimisedWaitHitError::Hit => metric.inc_hit(),
                            OptimisedWaitHitError::PredecessorAborted => metric.inc_pur_aborted(),
                            OptimisedWaitHitError::PredecessorActive => metric.inc_pur_active(),
                        },
                        NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
                        _ => {}
                    },

                    OptimisticWaitHitTransactionTypes(ref mut metric) => match reason {
                        NonFatalError::OptimisedWaitHitError(owhe) => match owhe {
                            OptimisedWaitHitError::Hit => metric.inc_hit(),
                            OptimisedWaitHitError::PredecessorAborted => metric.inc_pur_aborted(),
                            OptimisedWaitHitError::PredecessorActive => metric.inc_pur_active(),
                        },
                        NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
                        _ => {}
                    },

                    TwoPhaseLocking(ref mut metric) => match reason {
                        NonFatalError::TwoPhaseLockingError(tple) => match tple {
                            TwoPhaseLockingError::ReadLockRequestDenied(_) => {
                                metric.inc_read_lock_denied()
                            }
                            TwoPhaseLockingError::WriteLockRequestDenied(_) => {
                                metric.inc_write_lock_denied()
                            }
                        },
                        _ => {}
                    },

                    MixedTwoPhaseLocking(ref mut metric) => match reason {
                        NonFatalError::MixedTwoPhaseLockingError(tple) => match tple {
                            MixedTwoPhaseLockingError::ReadLockRequestDenied(_) => {
                                metric.inc_read_lock_denied()
                            }
                            MixedTwoPhaseLockingError::WriteLockRequestDenied(_) => {
                                metric.inc_write_lock_denied()
                            }
                        },
                        _ => {}
                    },

                    _ => {}
                }
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

            "acid" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in AcidTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Acid(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            "dummy" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in DummyTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Dummy(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            "tatp" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in TatpTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Tatp(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            _ => panic!("{} not implemented", workload),
        }
    }

    fn record(&mut self, transaction: &Transaction, outcome: &Outcome) {
        let ind = self
            .transactions
            .iter()
            .position(|x| &x.transaction == transaction)
            .unwrap();

        match outcome {
            Outcome::Committed { .. } => self.transactions[ind].inc_committed(),
            Outcome::Aborted { .. } => self.transactions[ind].inc_aborted(),
        }
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
    aborted: u32, // internal and external aborts
}

impl TransactionMetrics {
    fn new(transaction: Transaction) -> Self {
        TransactionMetrics {
            transaction,
            completed: 0,
            committed: 0,
            aborted: 0,
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

    fn merge(&mut self, other: TransactionMetrics) {
        assert!(self.transaction == other.transaction);

        self.completed += other.completed;
        self.committed += other.committed;
        self.aborted += other.aborted;
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
            "msgt" => {
                ProtocolAbortBreakdown::MixedSerializationGraph(SerializationGraphReasons::new())
            }
            "msgt-std" => {
                ProtocolAbortBreakdown::StdMixedSerializationGraph(SerializationGraphReasons::new())
            }
            "msgt-rel" => {
                ProtocolAbortBreakdown::RelMixedSerializationGraph(SerializationGraphReasons::new())
            }
            "msgt-early" => ProtocolAbortBreakdown::EarlyMixedSerializationGraph(
                SerializationGraphReasons::new(),
            ),
            "wh" => ProtocolAbortBreakdown::WaitHit(HitListReasons::new()),
            "owh" => ProtocolAbortBreakdown::OptimisticWaitHit(HitListReasons::new()),
            "owhtt" => {
                ProtocolAbortBreakdown::OptimisticWaitHitTransactionTypes(HitListReasons::new())
            }
            "nocc" => ProtocolAbortBreakdown::NoConcurrencyControl,
            "tpl" => ProtocolAbortBreakdown::TwoPhaseLocking(TwoPhaseLockingReasons::new()),
            "mtpl" => ProtocolAbortBreakdown::MixedTwoPhaseLocking(TwoPhaseLockingReasons::new()),
            _ => unimplemented!(),
        };

        let workload_specific = match workload {
            "smallbank" => WorkloadAbortBreakdown::SmallBank(SmallBankReasons::new()),
            "tatp" => WorkloadAbortBreakdown::Tatp(TatpReasons::new()),
            "acid" => WorkloadAbortBreakdown::Acid(AcidReasons::new()),
            "dummy" => WorkloadAbortBreakdown::Dummy(DummyReasons::new()),
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
            MixedSerializationGraph(ref mut reasons) => {
                if let MixedSerializationGraph(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            StdMixedSerializationGraph(ref mut reasons) => {
                if let StdMixedSerializationGraph(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            RelMixedSerializationGraph(ref mut reasons) => {
                if let RelMixedSerializationGraph(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            EarlyMixedSerializationGraph(ref mut reasons) => {
                if let EarlyMixedSerializationGraph(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            WaitHit(ref mut reasons) => {
                if let WaitHit(other_reasons) = other.protocol_specific {
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
            OptimisticWaitHitTransactionTypes(ref mut reasons) => {
                if let OptimisticWaitHitTransactionTypes(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            TwoPhaseLocking(ref mut reasons) => {
                if let TwoPhaseLocking(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            MixedTwoPhaseLocking(ref mut reasons) => {
                if let MixedTwoPhaseLocking(other_reasons) = other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }

            _ => {}
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
            Acid(ref mut reasons) => {
                if let Acid(other_reasons) = other.workload_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("workload abort breakdowns do not match");
                }
            }
            Dummy(ref mut reasons) => {
                if let Dummy(other_reasons) = other.workload_specific {
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
    Acid(AcidReasons),
    Dummy(DummyReasons),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TatpReasons {
    row_not_found: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SmallBankReasons {
    insufficient_funds: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AcidReasons {
    non_serializable: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DummyReasons {
    non_serializable: u32,
}

impl TatpReasons {
    fn new() -> Self {
        TatpReasons { row_not_found: 0 }
    }

    fn inc_not_found(&mut self) {
        self.row_not_found += 1;
    }

    fn merge(&mut self, other: TatpReasons) {
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

impl AcidReasons {
    fn new() -> Self {
        AcidReasons {
            non_serializable: 0,
        }
    }

    fn inc_non_serializable(&mut self) {
        self.non_serializable += 1;
    }

    fn merge(&mut self, other: AcidReasons) {
        self.non_serializable += other.non_serializable;
    }
}

impl DummyReasons {
    fn new() -> Self {
        Self {
            non_serializable: 0,
        }
    }

    fn inc_non_serializable(&mut self) {
        self.non_serializable += 1;
    }

    fn merge(&mut self, other: DummyReasons) {
        self.non_serializable += other.non_serializable;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum ProtocolAbortBreakdown {
    SerializationGraph(SerializationGraphReasons),
    MixedSerializationGraph(SerializationGraphReasons),
    StdMixedSerializationGraph(SerializationGraphReasons),
    RelMixedSerializationGraph(SerializationGraphReasons),
    EarlyMixedSerializationGraph(SerializationGraphReasons),
    WaitHit(HitListReasons),
    OptimisticWaitHit(HitListReasons),
    OptimisticWaitHitTransactionTypes(HitListReasons),
    TwoPhaseLocking(TwoPhaseLockingReasons),
    MixedTwoPhaseLocking(TwoPhaseLockingReasons),
    NoConcurrencyControl,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SerializationGraphReasons {
    cascading_abort: u32,
    cycle_found: u32,
    read_uncommitted: u32,
    read_committed: u32,
    serializable: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct HitListReasons {
    row_dirty: u32,
    hit: u32,
    pur_active: u32,
    pur_aborted: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TwoPhaseLockingReasons {
    read_lock_denied: u32,
    write_lock_denied: u32,
}

impl SerializationGraphReasons {
    fn new() -> Self {
        SerializationGraphReasons {
            cascading_abort: 0,
            cycle_found: 0,
            read_uncommitted: 0,
            read_committed: 0,
            serializable: 0,
        }
    }

    fn inc_cascading_abort(&mut self) {
        self.cascading_abort += 1;
    }

    fn inc_cycle_found(&mut self) {
        self.cycle_found += 1;
    }

    fn inc_read_uncommitted(&mut self) {
        self.read_uncommitted += 1;
    }

    fn inc_read_committed(&mut self) {
        self.read_committed += 1;
    }

    fn inc_serializable(&mut self) {
        self.serializable += 1;
    }

    fn merge(&mut self, other: SerializationGraphReasons) {
        self.cascading_abort += other.cascading_abort;
        self.cycle_found += other.cycle_found;
        self.read_uncommitted += other.read_uncommitted;
        self.read_committed += other.read_committed;
        self.serializable += other.serializable;
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

impl TwoPhaseLockingReasons {
    fn new() -> TwoPhaseLockingReasons {
        TwoPhaseLockingReasons {
            read_lock_denied: 0,
            write_lock_denied: 0,
        }
    }

    fn inc_read_lock_denied(&mut self) {
        self.read_lock_denied += 1;
    }

    fn inc_write_lock_denied(&mut self) {
        self.write_lock_denied += 1;
    }

    fn merge(&mut self, other: TwoPhaseLockingReasons) {
        self.read_lock_denied += other.read_lock_denied;
        self.write_lock_denied += other.write_lock_denied;
    }
}
