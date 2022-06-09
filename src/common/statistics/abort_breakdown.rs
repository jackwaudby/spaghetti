use crate::common::error::{AttendezError, NonFatalError, SerializationGraphError, WaitHitError};
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
            "attendez" => ProtocolAbortBreakdown::Attendez(AttendezReasons::new()),
            "wh" => ProtocolAbortBreakdown::WaitHit(WaitHitReasons::new()),
            "owh" => ProtocolAbortBreakdown::OptimisticWaitHit(WaitHitReasons::new()),
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

    pub fn get_workload_specific(&self) -> &WorkloadAbortBreakdown {
        &self.workload_specific
    }

    pub fn get_protocol_specific(&self) -> &ProtocolAbortBreakdown {
        &self.protocol_specific
    }

    pub fn get_mut_workload_specific(&mut self) -> &mut WorkloadAbortBreakdown {
        &mut self.workload_specific
    }

    pub fn get_mut_protocol_specific(&mut self) -> &mut ProtocolAbortBreakdown {
        &mut self.protocol_specific
    }

    pub fn record(&mut self, reason: &NonFatalError) {
        use WorkloadAbortBreakdown::*;
        match self.workload_specific {
            SmallBank(ref mut metric) => {
                if let NonFatalError::SmallBankError(_) = reason {
                    metric.inc_insufficient_funds();
                }
            } //         Acid(ref mut metric) => {
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
            _ => unimplemented!(),
        }

        use ProtocolAbortBreakdown::*;

        match self.protocol_specific {
            SerializationGraph(ref mut metric) => match reason {
                NonFatalError::SerializationGraphError(err) => match err {
                    SerializationGraphError::CascadingAbort => metric.inc_cascading_abort(),
                    SerializationGraphError::CycleFound => metric.inc_cycle_found(),
                },
                _ => {}
            },

            MixedSerializationGraph(ref mut metric) => match reason {
                NonFatalError::SerializationGraphError(sge) => match sge {
                    SerializationGraphError::CascadingAbort => metric.inc_cascading_abort(),
                    SerializationGraphError::CycleFound => metric.inc_cycle_found(),
                },
                _ => {}
            },

            Attendez(ref mut metric) => match reason {
                NonFatalError::AttendezError(owhe) => match owhe {
                    AttendezError::ExceededWatermark => metric.inc_exceeded_watermark(),
                    AttendezError::PredecessorAborted => metric.inc_predecessor_aborted(),
                    AttendezError::WriteOpExceededWatermark => {
                        metric.inc_write_op_exceeded_watermark()
                    }
                },
                NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
                _ => {}
            },

            WaitHit(ref mut metric) => match reason {
                NonFatalError::WaitHitError(owhe) => match owhe {
                    WaitHitError::Hit => metric.inc_hit(),
                    WaitHitError::PredecessorAborted => metric.inc_pur_aborted(),
                    WaitHitError::PredecessorActive => metric.inc_pur_active(),
                },
                NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
                _ => {}
            },

            OptimisticWaitHit(ref mut metric) => match reason {
                NonFatalError::WaitHitError(err) => match err {
                    WaitHitError::Hit => metric.inc_hit(),
                    WaitHitError::PredecessorAborted => metric.inc_pur_aborted(),
                    WaitHitError::PredecessorActive => metric.inc_pur_active(),
                },
                NonFatalError::RowDirty(_) => metric.inc_row_dirty(),
                _ => {}
            },
            _ => {}
        }
    }

    pub fn merge(&mut self, other: &AbortBreakdown) {
        use ProtocolAbortBreakdown::*;
        use WorkloadAbortBreakdown::*;

        match self.protocol_specific {
            SerializationGraph(ref mut reasons) => {
                if let SerializationGraph(other_reasons) = &other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }

            MixedSerializationGraph(ref mut reasons) => {
                if let MixedSerializationGraph(other_reasons) = &other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }

            Attendez(ref mut reasons) => {
                if let Attendez(other_reasons) = &other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }

            WaitHit(ref mut reasons) => {
                if let WaitHit(other_reasons) = &other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }
            OptimisticWaitHit(ref mut reasons) => {
                if let OptimisticWaitHit(other_reasons) = &other.protocol_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("protocol abort breakdowns do not match");
                }
            }

            _ => {}
        }

        match self.workload_specific {
            SmallBank(ref mut reasons) => {
                if let SmallBank(other_reasons) = &other.workload_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("workload abort breakdowns do not match");
                }
            }

            Tatp(ref mut reasons) => {
                if let Tatp(other_reasons) = &other.workload_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("workload abort breakdowns do not match");
                }
            }

            Acid(ref mut reasons) => {
                if let Acid(other_reasons) = &other.workload_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("workload abort breakdowns do not match");
                }
            }

            Dummy(ref mut reasons) => {
                if let Dummy(other_reasons) = &other.workload_specific {
                    reasons.merge(other_reasons);
                } else {
                    panic!("workload abort breakdowns do not match");
                }
            }

            Ycsb => {}
        }
    }
}
