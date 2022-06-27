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
    wait: u128,
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
            wait: 0,
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
        self.wait += local.wait;
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
            "diagnostics": summary.get_diagnostics(),
            "wait(ms)":  format!("{:.0}", (self.wait as f64 / 1000000.0)),
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
            summary.get_committed_transaction_av_latency(),
            summary.get_aborted_transaction_av_latency(),
            summary.get_write_operation_av_latency(),
            summary.get_commit_operation_av_latency(),
        ))
        .unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalStatistics {
    core_id: usize,
    pub total_time: u128,
    pub latency: u128,
    pub wait: u128,
    pub transaction_breakdown: TransactionBreakdown,
}

impl LocalStatistics {
    pub fn new(config: &Config, core_id: usize) -> Self {
        let protocol = config.get_str("protocol").unwrap();
        let workload = config.get_str("workload").unwrap();
        let transaction_breakdown = TransactionBreakdown::new(&workload, &protocol);

        LocalStatistics {
            core_id,
            total_time: 0,
            latency: 0,
            wait: 0,
            transaction_breakdown,
        }
    }

    pub fn get_core_id(&self) -> usize {
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

    pub fn stop_wait_manager(&mut self, start: Instant) {
        self.wait += start.elapsed().as_nanos();
    }

    pub fn get_global_summary(&mut self, workload: &str, protocol: &str) -> GlobalSummary {
        self.transaction_breakdown
            .get_global_summary(workload, protocol)
    }

    pub fn record(&mut self, response: &Response) {
        self.transaction_breakdown.record(response);
    }
}
