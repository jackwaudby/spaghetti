use crate::common::statistics::protocol_abort_breakdown::{
    AttendezReasons, ProtocolAbortBreakdown, SerializationGraphReasons, WaitHitReasons,
};
use crate::common::statistics::workload_abort_breakdown::{
    AcidReasons, DummyReasons, SmallBankReasons, TatpReasons, WorkloadAbortBreakdown,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AbortBreakdown {
    protocol_specific: ProtocolAbortBreakdown,
    workload_specific: WorkloadAbortBreakdown,
}

impl AbortBreakdown {
    pub fn new(protocol: &str, workload: &str) -> AbortBreakdown {
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
            "attendez" => ProtocolAbortBreakdown::Attendez(AttendezReasons::new()),
            "wh" => ProtocolAbortBreakdown::WaitHit(WaitHitReasons::new()),
            "owh" => ProtocolAbortBreakdown::OptimisticWaitHit(WaitHitReasons::new()),
            "owhtt" => {
                ProtocolAbortBreakdown::OptimisticWaitHitTransactionTypes(WaitHitReasons::new())
            }
            "nocc" => ProtocolAbortBreakdown::NoConcurrencyControl,
            _ => unimplemented!(),
        };

        let workload_specific = match workload {
            "smallbank" => WorkloadAbortBreakdown::SmallBank(SmallBankReasons::new()),
            "tatp" => WorkloadAbortBreakdown::Tatp(TatpReasons::new()),
            "acid" => WorkloadAbortBreakdown::Acid(AcidReasons::new()),
            "dummy" => WorkloadAbortBreakdown::Dummy(DummyReasons::new()),
            "ycsb" => WorkloadAbortBreakdown::Ycsb,
            _ => unimplemented!(),
        };

        AbortBreakdown {
            protocol_specific,
            workload_specific,
        }
    }

    pub fn get_workload_specific(&mut self) -> &mut WorkloadAbortBreakdown {
        &mut self.workload_specific
    }

    pub fn get_protocol_specific(&mut self) -> &mut ProtocolAbortBreakdown {
        &mut self.protocol_specific
    }

    pub fn merge(&mut self, other: AbortBreakdown) {
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

            Attendez(ref mut reasons) => {
                if let Attendez(other_reasons) = other.protocol_specific {
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

            Ycsb => {}
        }
    }
}
