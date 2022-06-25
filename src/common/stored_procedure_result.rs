use serde::{Deserialize, Serialize};

use crate::common::error::NonFatalError;
use crate::common::message::{Outcome, Success};
use crate::common::statistics::latency_breakdown::LatencyBreakdown;
use crate::common::statistics::protocol_diagnostics::ProtocolDiagnostics;
use crate::common::stats_bucket::StatsBucket;
use crate::workloads::IsolationLevel;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StoredProcedureResult {
    isolation: IsolationLevel,
    outcome: Outcome,
    diagnostics: ProtocolDiagnostics,
    latency: LatencyBreakdown,
    internal_id: u64,
}

impl StoredProcedureResult {
    pub fn from_error(
        error: NonFatalError,
        isolation: IsolationLevel,
        meta: &mut StatsBucket,
    ) -> StoredProcedureResult {
        let outcome = Outcome::Aborted(error);
        let latency = meta.take_latency_breakdown();
        let diagnostics = meta.take_diagnostics();

        let internal_id = meta.get_transaction_id().extract();

        StoredProcedureResult {
            isolation,
            outcome,
            diagnostics,
            latency,
            internal_id,
        }
    }

    pub fn from_success(
        success: Success,
        isolation: IsolationLevel,
        meta: &mut StatsBucket,
    ) -> StoredProcedureResult {
        let outcome = Outcome::Committed(success);
        let latency = meta.take_latency_breakdown();
        let diagnostics = meta.take_diagnostics();
        let internal_id = meta.get_transaction_id().extract();

        StoredProcedureResult {
            isolation,
            outcome,
            diagnostics,
            latency,
            internal_id,
        }
    }

    pub fn get_internal_id(&self) -> u64 {
        self.internal_id
    }

    pub fn get_isolation_level(&self) -> &IsolationLevel {
        &self.isolation
    }

    pub fn get_outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub fn get_latency(&self) -> &LatencyBreakdown {
        &self.latency
    }

    pub fn get_diagnostics(&self) -> &ProtocolDiagnostics {
        &self.diagnostics
    }

    pub fn set_total_latency(&mut self, dur: u128) {
        self.latency.set_total(dur);
    }
}
