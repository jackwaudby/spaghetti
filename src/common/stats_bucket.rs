use crate::common::statistics::latency_breakdown::LatencyBreakdown;
use crate::common::statistics::protocol_diagnostics::ProtocolDiagnostics;
use crate::storage::access::TransactionId;

pub struct StatsBucket {
    tid: TransactionId,
    latency: LatencyBreakdown,
    diagnostics: ProtocolDiagnostics,
}

impl StatsBucket {
    pub fn new(tid: TransactionId, diagnostics: ProtocolDiagnostics) -> Self {
        Self {
            tid,
            latency: LatencyBreakdown::new(),
            diagnostics,
        }
    }

    pub fn get_transaction_id(&self) -> TransactionId {
        self.tid.clone()
    }

    pub fn get_mut_latency_breakdown(&mut self) -> &mut LatencyBreakdown {
        &mut self.latency
    }

    pub fn take_latency_breakdown(&mut self) -> LatencyBreakdown {
        self.latency.clone()
    }

    pub fn take_diagnostics(&mut self) -> ProtocolDiagnostics {
        self.diagnostics.clone()
    }

    pub fn get_diagnostics(&mut self) -> &mut ProtocolDiagnostics {
        &mut self.diagnostics
    }
}
