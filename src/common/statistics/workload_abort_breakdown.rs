//! Breakdown of workload-specific reasons why transactions aborted (internal aborts).

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WorkloadAbortBreakdown {
    SmallBank(SmallBankReasons),
    Tatp(TatpReasons),
    Acid(AcidReasons),
    Dummy(DummyReasons),
    Ycsb,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TatpReasons {
    row_not_found: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SmallBankReasons {
    insufficient_funds: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AcidReasons {
    non_serializable: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DummyReasons {
    non_serializable: u32,
}

impl TatpReasons {
    pub fn new() -> Self {
        TatpReasons { row_not_found: 0 }
    }

    pub fn inc_not_found(&mut self) {
        self.row_not_found += 1;
    }

    pub fn merge(&mut self, other: &TatpReasons) {
        self.row_not_found += other.row_not_found;
    }

    pub fn get_row_not_found(&self) -> u32 {
        self.row_not_found
    }
}

impl SmallBankReasons {
    pub fn new() -> Self {
        SmallBankReasons {
            insufficient_funds: 0,
        }
    }

    pub fn inc_insufficient_funds(&mut self) {
        self.insufficient_funds += 1;
    }

    pub fn merge(&mut self, other: &SmallBankReasons) {
        self.insufficient_funds += other.insufficient_funds;
    }

    pub fn get_insufficient_funds(&self) -> u32 {
        self.insufficient_funds
    }
}

impl AcidReasons {
    pub fn new() -> Self {
        AcidReasons {
            non_serializable: 0,
        }
    }

    pub fn inc_non_serializable(&mut self) {
        self.non_serializable += 1;
    }

    pub fn merge(&mut self, other: &AcidReasons) {
        self.non_serializable += other.non_serializable;
    }

    pub fn get_non_serializable(&self) -> u32 {
        self.non_serializable
    }
}

impl DummyReasons {
    pub fn new() -> Self {
        Self {
            non_serializable: 0,
        }
    }

    pub fn inc_non_serializable(&mut self) {
        self.non_serializable += 1;
    }

    pub fn merge(&mut self, other: &DummyReasons) {
        self.non_serializable += other.non_serializable;
    }

    pub fn get_non_serializable(&self) -> u32 {
        self.non_serializable
    }
}
