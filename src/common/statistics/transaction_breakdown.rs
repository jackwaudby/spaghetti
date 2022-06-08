use crate::common::message::{Outcome, Transaction};
// use crate::workloads::acid::AcidTransaction;
// use crate::workloads::dummy::DummyTransaction;
use crate::common::statistics::protocol_abort_breakdown::ProtocolAbortBreakdown;
use crate::workloads::smallbank::SmallBankTransaction;

// use crate::workloads::tatp::TatpTransaction;
// use crate::workloads::ycsb::YcsbTransaction;
use crate::common::message::Response;
use crate::common::statistics::latency_breakdown::LatencyBreakdown;
use crate::common::statistics::workload_abort_breakdown::WorkloadAbortBreakdown;
use crate::common::statistics::AbortBreakdown;
use crate::common::statistics::ProtocolDiagnostics;

use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionBreakdown {
    name: String,
    transactions: Vec<TransactionMetrics>,
}

impl TransactionBreakdown {
    pub fn new(workload: &str, protocol: &str) -> Self {
        match workload {
            "smallbank" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in SmallBankTransaction::iter() {
                    let metrics = TransactionMetrics::new(
                        Transaction::SmallBank(transaction),
                        workload,
                        protocol,
                    );
                    transactions.push(metrics);
                }

                TransactionBreakdown { name, transactions }
            }

            // "acid" => {
            //     let name = workload.to_string();
            //     let mut transactions = vec![];
            //     for transaction in AcidTransaction::iter() {
            //         let metrics = TransactionMetrics::new(Transaction::Acid(transaction));
            //         transactions.push(metrics);
            //     }
            //     TransactionBreakdown { name, transactions }
            // }

            // "dummy" => {
            //     let name = workload.to_string();
            //     let mut transactions = vec![];
            //     for transaction in DummyTransaction::iter() {
            //         let metrics = TransactionMetrics::new(Transaction::Dummy(transaction));
            //         transactions.push(metrics);
            //     }
            //     TransactionBreakdown { name, transactions }
            // }

            // "tatp" => {
            //     let name = workload.to_string();
            //     let mut transactions = vec![];
            //     for transaction in TatpTransaction::iter() {
            //         let metrics = TransactionMetrics::new(Transaction::Tatp(transaction));
            //         transactions.push(metrics);
            //     }
            //     TransactionBreakdown { name, transactions }
            // }

            // "ycsb" => {
            //     let name = workload.to_string();
            //     let mut transactions = vec![];
            //     for transaction in YcsbTransaction::iter() {
            //         let metrics = TransactionMetrics::new(Transaction::Ycsb(transaction));
            //         transactions.push(metrics);
            //     }
            //     TransactionBreakdown { name, transactions }
            // }
            _ => panic!("unknown workload: {}", workload),
        }
    }

    pub fn record(&mut self, response: &Response) {
        let transaction = response.get_transaction();
        let ind = self
            .transactions
            .iter()
            .position(|x| &x.transaction == transaction)
            .unwrap();

        self.transactions[ind].record(response);
    }

    pub fn merge(&mut self, other: TransactionBreakdown) {
        assert!(self.name == other.name);

        for holder in other.transactions {
            let ind = self
                .transactions
                .iter()
                .position(|x| x.transaction == holder.transaction)
                .unwrap();
            self.transactions[ind].merge(&holder);
        }
    }

    pub fn get_global_summary(&self, workload: &str, protocol: &str) -> GlobalSummary {
        let mut summary = GlobalSummary::new(workload, protocol);
        for holder in &self.transactions {
            summary.merge(holder);
        }
        summary
    }

    pub fn get_transactions(&mut self) -> &mut Vec<TransactionMetrics> {
        &mut self.transactions
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionMetrics {
    transaction: Transaction,
    completed: u32,
    committed: u32,
    committed_latency: LatencyBreakdown,
    committed_diagnostics: ProtocolDiagnostics,
    aborted: u32,
    aborted_breakdown: AbortBreakdown,
    aborted_latency: LatencyBreakdown,
    aborted_diagnostics: ProtocolDiagnostics,
}

impl TransactionMetrics {
    pub fn new(transaction: Transaction, workload: &str, protocol: &str) -> Self {
        let committed_latency = LatencyBreakdown::new();
        let committed_diagnostics = ProtocolDiagnostics::new(protocol);
        let aborted_breakdown = AbortBreakdown::new(protocol, workload);
        let aborted_latency = LatencyBreakdown::new();
        let aborted_diagnostics = ProtocolDiagnostics::new(protocol);

        TransactionMetrics {
            transaction,
            completed: 0,
            committed: 0,
            committed_latency,
            committed_diagnostics,
            aborted: 0,
            aborted_breakdown,
            aborted_latency,
            aborted_diagnostics,
        }
    }

    pub fn record(&mut self, response: &Response) {
        self.inc_completed();

        let result = response.get_result();

        match result.get_outcome() {
            Outcome::Committed(_) => {
                self.inc_committed();
                self.committed_latency.merge(result.get_latency());
                self.committed_diagnostics.merge(result.get_diagnostics());
            }
            Outcome::Aborted(reason) => {
                self.inc_aborted();
                self.aborted_latency.merge(result.get_latency());
                self.aborted_diagnostics.merge(result.get_diagnostics());
                self.aborted_breakdown.record(reason);
            }
        }
    }

    fn inc_completed(&mut self) {
        self.completed += 1;
    }

    fn inc_committed(&mut self) {
        self.committed += 1;
    }

    fn inc_aborted(&mut self) {
        self.aborted += 1;
    }

    pub fn get_completed(&self) -> u32 {
        self.completed
    }

    pub fn get_committed(&self) -> u32 {
        self.committed
    }

    pub fn get_aborted(&self) -> u32 {
        self.aborted
    }

    pub fn merge(&mut self, other: &TransactionMetrics) {
        assert!(self.transaction == other.transaction);

        self.completed += other.completed;
        self.committed += other.committed;
        self.committed_latency.merge(&other.committed_latency);
        self.committed_diagnostics
            .merge(&other.committed_diagnostics);
        self.aborted += other.aborted;
        self.aborted_breakdown.merge(&other.aborted_breakdown);
        self.aborted_latency.merge(&other.aborted_latency);
        self.aborted_diagnostics.merge(&other.aborted_diagnostics);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GlobalSummary {
    completed: u32,
    committed: u32,
    committed_latency: LatencyBreakdown,
    committed_diagnostics: ProtocolDiagnostics,
    aborted: u32,
    aborted_breakdown: AbortBreakdown,
    aborted_latency: LatencyBreakdown,
    aborted_diagnostics: ProtocolDiagnostics,
}

impl GlobalSummary {
    pub fn new(workload: &str, protocol: &str) -> Self {
        let committed_latency = LatencyBreakdown::new();
        let committed_diagnostics = ProtocolDiagnostics::new(protocol);
        let aborted_breakdown = AbortBreakdown::new(protocol, workload);
        let aborted_latency = LatencyBreakdown::new();
        let aborted_diagnostics = ProtocolDiagnostics::new(protocol);

        Self {
            completed: 0,
            committed: 0,
            committed_latency,
            committed_diagnostics,
            aborted: 0,
            aborted_breakdown,
            aborted_latency,
            aborted_diagnostics,
        }
    }

    pub fn get_completed(&self) -> u32 {
        self.completed
    }

    pub fn get_committed(&self) -> u32 {
        self.committed
    }

    pub fn get_aborted(&self) -> u32 {
        self.aborted
    }

    pub fn get_write_av_latency(&self) -> f64 {
        let mut cum = self.committed_latency.get_write();
        cum += self.aborted_latency.get_write();
        let mut cnt = self.committed_latency.get_write_cnt();
        cnt += self.aborted_latency.get_write_cnt();

        cum as f64 / cnt as f64 / 1000.0
    }

    pub fn get_committed_write_av_latency(&self) -> f64 {
        let cum = self.committed_latency.get_write();
        let cnt = self.committed_latency.get_write_cnt();

        cum as f64 / cnt as f64 / 1000.0
    }

    pub fn get_aborted_write_av_latency(&self) -> f64 {
        let cum = self.aborted_latency.get_write();
        let cnt = self.aborted_latency.get_write_cnt();

        cum as f64 / cnt as f64 / 1000.0
    }

    pub fn get_aborted_transaction_av_latency(&self) -> f64 {
        let cum = self.aborted_latency.get_total();

        cum as f64 / self.aborted as f64 / 1000.0
    }

    pub fn get_committed_transaction_av_latency(&self) -> f64 {
        let cum = self.committed_latency.get_total();

        cum as f64 / self.committed as f64 / 1000.0
    }

    pub fn get_transaction_av_latency(&self) -> f64 {
        let mut cum = self.committed_latency.get_total();
        cum += self.aborted_latency.get_total();

        cum as f64 / self.completed as f64 / 1000.0
    }

    pub fn get_commit_av_latency(&self) -> f64 {
        let cum = self.committed_latency.get_commit();

        cum as f64 / self.committed as f64 / 1000.0
    }

    pub fn get_aborted_breakdown(&self) -> &AbortBreakdown {
        &self.aborted_breakdown
    }

    pub fn get_mut_aborted_breakdown(&mut self) -> &mut AbortBreakdown {
        &mut self.aborted_breakdown
    }

    pub fn merge(&mut self, other: &TransactionMetrics) {
        self.completed += other.completed;
        self.committed += other.committed;
        self.committed_latency.merge(&other.committed_latency);
        self.committed_diagnostics
            .merge(&other.committed_diagnostics);
        self.aborted += other.aborted;
        self.aborted_breakdown.merge(&other.aborted_breakdown);
        self.aborted_latency.merge(&other.aborted_latency);
        self.aborted_diagnostics.merge(&other.aborted_diagnostics);
    }

    pub fn get_lat_summary(&self) -> LatencySummary {
        LatencySummary {
            completed: format!("{:.2}", self.get_transaction_av_latency()),
            committed: format!("{:.2}", self.get_committed_transaction_av_latency()),
            aborted: format!("{:.2}", self.get_aborted_transaction_av_latency()),
            write_op: format!("{:.2}", self.get_write_av_latency()),
            write_op_in_committed: format!("{:.2}", self.get_committed_write_av_latency()),
            write_op_in_aborted: format!("{:.2}", self.get_aborted_write_av_latency()),
            commit_op_in_committed: format!("{:.2}", self.get_commit_av_latency()),
        }
    }

    fn inc_committed(&mut self, i: u32) {
        self.committed += i;
    }

    fn dec_aborted(&mut self, d: u32) {
        self.aborted -= d;
    }

    pub fn get_internal_aborts(&mut self) -> u32 {
        let w = self.get_mut_aborted_breakdown().get_mut_workload_specific();

        match w {
            WorkloadAbortBreakdown::Tatp(ref reasons) => {
                // row not found is the only internal abort
                // but these count as committed in this workload
                let x = reasons.get_row_not_found();
                self.inc_committed(x);
                self.dec_aborted(x);

                0
            }
            WorkloadAbortBreakdown::SmallBank(ref reasons) => reasons.get_insufficient_funds(),
            WorkloadAbortBreakdown::Acid(ref reasons) => reasons.get_non_serializable(),
            WorkloadAbortBreakdown::Dummy(ref reasons) => reasons.get_non_serializable(),
            WorkloadAbortBreakdown::Ycsb => 0,
        }
    }

    pub fn get_external_aborts(&self) -> u32 {
        match self.get_aborted_breakdown().get_protocol_specific() {
            ProtocolAbortBreakdown::SerializationGraph(ref reasons) => reasons.aggregate(),
            ProtocolAbortBreakdown::MixedSerializationGraph(ref reasons) => reasons.aggregate(),
            ProtocolAbortBreakdown::WaitHit(ref reasons) => reasons.aggregate(),
            ProtocolAbortBreakdown::Attendez(ref reasons) => reasons.aggregate(),
            ProtocolAbortBreakdown::OptimisticWaitHit(ref reasons) => reasons.aggregate(),
            ProtocolAbortBreakdown::NoConcurrencyControl => 0,
        }
    }

    pub fn get_abort_rate(&self) -> f64 {
        (self.get_external_aborts() as f64 / (self.committed + self.get_external_aborts()) as f64)
            * 100.0
    }

    pub fn get_abort_summary(&mut self) -> AbortSummary {
        let internal = self.get_internal_aborts();
        let external = self.get_external_aborts();
        let abort_rate = self.get_abort_rate();
        let breakdown = self.aborted_breakdown.clone();

        AbortSummary {
            aborted: self.aborted,
            internal: format!(
                "{} ({:.2} %)",
                internal,
                (internal as f64 / self.aborted as f64) * 100.0
            ),
            external: format!(
                "{} ({:.2} %)",
                external,
                (external as f64 / self.aborted as f64) * 100.0
            ),
            abort_rate: format!("{:.2} %", abort_rate),
            breakdown,
        }
    }

    pub fn get_thpt(&self, total_time: u128, cores: u32) -> f64 {
        self.committed as f64 / (((total_time as f64 / 1000000.0) / 1000.0) / cores as f64)
    }

    pub fn get_diagnostics(&self) -> &ProtocolDiagnostics {
        &self.aborted_diagnostics
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LatencySummary {
    completed: String,
    committed: String,
    aborted: String,
    write_op: String,
    write_op_in_committed: String,
    write_op_in_aborted: String,
    commit_op_in_committed: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AbortSummary {
    aborted: u32,
    internal: String,
    external: String,
    abort_rate: String,
    breakdown: AbortBreakdown,
}
