//! Breakdown of protocol-specific reasons why transactions aborted (external aborts).

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ProtocolAbortBreakdown {
    SerializationGraph(SerializationGraphReasons),
    MixedSerializationGraph(SerializationGraphReasons),
    WaitHit(WaitHitReasons),
    Attendez(AttendezReasons),
    OptimisticWaitHit(WaitHitReasons),
    NoConcurrencyControl,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializationGraphReasons {
    cascading_abort: u32,
    cycle_found: u32,
    read_uncommitted: u32, // TODO: these don't fit here
    read_committed: u32,
    serializable: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WaitHitReasons {
    row_dirty: u32,
    hit: u32,
    pur_active: u32,
    pur_aborted: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AttendezReasons {
    row_dirty: u32,
    predecessor_aborted: u32,
    exceeded_watermark: u32,
    write_op_exceeded_watermark: u32,
}

impl SerializationGraphReasons {
    pub fn new() -> Self {
        SerializationGraphReasons {
            cascading_abort: 0,
            cycle_found: 0,
            read_uncommitted: 0,
            read_committed: 0,
            serializable: 0,
        }
    }

    pub fn inc_cascading_abort(&mut self) {
        self.cascading_abort += 1;
    }

    pub fn inc_cycle_found(&mut self) {
        self.cycle_found += 1;
    }

    pub fn inc_read_uncommitted(&mut self) {
        self.read_uncommitted += 1;
    }

    pub fn inc_read_committed(&mut self) {
        self.read_committed += 1;
    }

    pub fn inc_serializable(&mut self) {
        self.serializable += 1;
    }

    pub fn merge(&mut self, other: &SerializationGraphReasons) {
        self.cascading_abort += other.cascading_abort;
        self.cycle_found += other.cycle_found;
        self.read_uncommitted += other.read_uncommitted;
        self.read_committed += other.read_committed;
        self.serializable += other.serializable;
    }

    pub fn aggregate(&self) -> u32 {
        self.cascading_abort + self.cycle_found
    }
}

impl WaitHitReasons {
    pub fn new() -> Self {
        WaitHitReasons {
            row_dirty: 0,
            hit: 0,
            pur_aborted: 0,
            pur_active: 0,
        }
    }

    pub fn inc_row_dirty(&mut self) {
        self.row_dirty += 1;
    }

    pub fn inc_hit(&mut self) {
        self.hit += 1;
    }

    pub fn inc_pur_active(&mut self) {
        self.pur_active += 1;
    }

    pub fn inc_pur_aborted(&mut self) {
        self.pur_aborted += 1;
    }

    pub fn merge(&mut self, other: &WaitHitReasons) {
        self.row_dirty += other.row_dirty;
        self.hit += other.hit;
        self.pur_aborted += other.pur_aborted;
        self.pur_active += other.pur_active;
    }

    pub fn aggregate(&self) -> u32 {
        self.row_dirty + self.hit + self.pur_aborted + self.pur_active
    }
}

impl AttendezReasons {
    pub fn new() -> Self {
        AttendezReasons {
            row_dirty: 0,
            predecessor_aborted: 0,
            exceeded_watermark: 0,
            write_op_exceeded_watermark: 0,
        }
    }

    pub fn inc_row_dirty(&mut self) {
        self.row_dirty += 1;
    }

    pub fn inc_predecessor_aborted(&mut self) {
        self.predecessor_aborted += 1;
    }

    pub fn inc_exceeded_watermark(&mut self) {
        self.exceeded_watermark += 1;
    }

    pub fn inc_write_op_exceeded_watermark(&mut self) {
        self.write_op_exceeded_watermark += 1;
    }

    pub fn get_row_dirty(&self) -> u32 {
        self.row_dirty
    }

    pub fn get_cascade(&self) -> u32 {
        self.predecessor_aborted
    }

    pub fn get_exceeded_watermark(&self) -> u32 {
        self.exceeded_watermark
    }

    pub fn get_write_op_exceeded_watermark(&self) -> u32 {
        self.write_op_exceeded_watermark
    }

    pub fn merge(&mut self, other: &AttendezReasons) {
        self.row_dirty += other.row_dirty;
        self.predecessor_aborted += other.predecessor_aborted;
        self.exceeded_watermark += other.exceeded_watermark;
        self.write_op_exceeded_watermark += other.write_op_exceeded_watermark;
    }

    pub fn aggregate(&self) -> u32 {
        self.row_dirty
            + self.predecessor_aborted
            + self.exceeded_watermark
            + self.write_op_exceeded_watermark
    }
}
