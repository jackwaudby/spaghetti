use crate::common::message::Response;
use crate::common::statistics::abort_breakdown::AbortBreakdown;
use crate::common::statistics::protocol_diagnostics::ProtocolDiagnostics;
use crate::common::statistics::transaction_breakdown::GlobalSummary;
use crate::common::statistics::transaction_breakdown::TransactionBreakdown;

use config::Config;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::path::Path;

use std::time::Duration;
use std::time::Instant;

pub mod abort_breakdown;
pub mod latency_breakdown;
pub mod protocol_abort_breakdown;
pub mod protocol_diagnostics;
pub mod transaction_breakdown;
pub mod workload_abort_breakdown;

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
    anomaly: Option<String>,
}

impl GlobalStatistics {
    pub fn new(config: &Config) -> Self {
        let scale_factor = config.get_int("scale_factor").unwrap() as u64;
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let cores = config.get_int("cores").unwrap() as u32;

        let transaction_breakdown = TransactionBreakdown::new(&workload, &protocol);

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

        let mut summary = self
            .transaction_breakdown
            .get_global_summary(&self.workload, &self.protocol);

        // sanity check
        let completed = summary.get_completed();
        let committed = summary.get_committed();
        let aborted = summary.get_aborted();
        assert_eq!(completed, committed + aborted);

        let internal = summary.get_internal_aborts();
        let external = summary.get_external_aborts();
        assert_eq!(aborted, external + internal, "{},{}", external, internal);

        // detailed file output
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
            // "internal_aborts": internal_aborts,
            // "external_aborts": external_aborts,
            // "abort_rate": format!("{:.3}", abort_rate),
            // "throughput": format!("{:.3}", throughput),
            // "abort_breakdown": summary.get_aborted_breakdown(),
            // "transaction_breakdown": summary.get_transaction_breakdown(),
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
            "aborted": summary.get_abort_summary(),
            "throughput": format!("{:.2}", summary.get_thpt(self.total_time, self.cores)),
            "av_latency (us)":  summary.get_lat_summary(),
            "diagnostics": summary.get_diagnostics()
        });
        tracing::info!("{}", serde_json::to_string_pretty(&pr).unwrap());

        // summary file output
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
            summary.get_thpt(self.total_time, self.cores),
            summary.get_abort_rate(),
            summary.get_transaction_av_latency(),
            summary.get_write_av_latency(),
        ))
        .unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalStatistics {
    core_id: u32,
    pub total_time: u128,
    pub latency: u128,
    pub transaction_breakdown: TransactionBreakdown,
}

impl LocalStatistics {
    pub fn new(core_id: u32, workload: &str, protocol: &str) -> Self {
        let transaction_breakdown = TransactionBreakdown::new(workload, protocol);

        LocalStatistics {
            core_id,
            total_time: 0,
            latency: 0,
            transaction_breakdown,
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

    pub fn get_global_summary(&mut self, workload: &str, protocol: &str) -> GlobalSummary {
        self.transaction_breakdown
            .get_global_summary(workload, protocol)
    }

    pub fn record(&mut self, response: &Response) {
        self.transaction_breakdown.record(response);

        // if let Outcome::Committed(success) = outcome {
        //     let pd = success.diagnostics.as_ref().unwrap();
        //     if let ProtocolDiagnostics::Attendez(diag) = pd {
        //         if let ProtocolDiagnostics::Attendez(ref mut d) = self.protocol_diagnostics {
        //             d.merge(diag);
        //         }
        //     }
        // }

        // if let Outcome::Aborted(reason) = outcome {
        //     use WorkloadAbortBreakdown::*;
        //     match self.abort_breakdown.get_workload_specific() {
        //         SmallBank(ref mut metric) => {
        //             if let NonFatalError::SmallBankError(_) = reason {
        //                 metric.inc_insufficient_funds();
        //             }
        //         }

        //         Acid(ref mut metric) => {
        //             if let NonFatalError::NonSerializable = reason {
        //                 metric.inc_non_serializable();
        //             }
        //         }

        //         Dummy(ref mut metric) => {
        //             if let NonFatalError::NonSerializable = reason {
        //                 metric.inc_non_serializable();
        //             }
        //         }

        //         Tatp(ref mut metric) => {
        //             if let NonFatalError::RowNotFound(_, _) = reason {
        //                 metric.inc_not_found();
        //             }
        //         }
        //         Ycsb => {}
        //     }

        //     use IsolationLevel::*;
        //     use ProtocolAbortBreakdown::*;
        //     use SerializationGraphError::*;

        //     match self.abort_breakdown.get_protocol_specific() {
        //         SerializationGraph(ref mut metric) => {
        //             if let NonFatalError::SerializationGraphError(err) = reason {
        //                 match err {
        //                     CascadingAbort => metric.inc_cascading_abort(),
        //                     CycleFound => metric.inc_cycle_found(),
        //                 }

        //                 match isolation {
        //                     ReadCommitted => metric.inc_read_committed(),
        //                     ReadUncommitted => metric.inc_read_uncommitted(),
        //                     Serializable => metric.inc_serializable(),
        //                 }
        //             }
        //         }

        //         MixedSerializationGraph(ref mut metric) => {
        //             if let NonFatalError::SerializationGraphError(sge) = reason {
        //                 match sge {
        //                     CascadingAbort => metric.inc_cascading_abort(),
        //                     CycleFound => metric.inc_cycle_found(),
        //                 }
        //                 match isolation {
        //                     ReadCommitted => metric.inc_read_committed(),
        //                     ReadUncommitted => metric.inc_read_uncommitted(),
        //                     Serializable => metric.inc_serializable(),
        //                 }
        //             }
        //         }

        //         Attendez(ref mut metric) => match reason {
        //             NonFatalError::AttendezError(owhe) => match owhe {
        //                 AttendezError::ExceededWatermark => metric.inc_exceeded_watermark(),
        //                 AttendezError::PredecessorAborted => metric.inc_predecessor_aborted(),
        //                 AttendezError::WriteOpExceededWatermark => {
        //                     metric.inc_write_op_exceeded_watermark()
        //                 }
        //             },
        //             NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
        //             _ => {}
        //         },

        //         WaitHit(ref mut metric) => match reason {
        //             NonFatalError::WaitHitError(owhe) => match owhe {
        //                 WaitHitError::Hit => metric.inc_hit(),
        //                 WaitHitError::PredecessorAborted => metric.inc_pur_aborted(),
        //                 WaitHitError::PredecessorActive => metric.inc_pur_active(),
        //             },
        //             NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
        //             _ => {}
        //         },

        //         OptimisticWaitHit(ref mut metric) => match reason {
        //             NonFatalError::WaitHitError(err) => match err {
        //                 WaitHitError::Hit => metric.inc_hit(),
        //                 WaitHitError::PredecessorAborted => metric.inc_pur_aborted(),
        //                 WaitHitError::PredecessorActive => metric.inc_pur_active(),
        //             },
        //             NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
        //             _ => {}
        //         },

        //         _ => {}
        //     }
        // }
    }
}
