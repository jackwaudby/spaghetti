use crate::common::message::Message;
use crate::workloads::acid::paramgen::AcidGenerator;
use crate::workloads::smallbank::paramgen::SmallBankGenerator;
use crate::workloads::tatp::paramgen::TatpGenerator;
use crate::workloads::tpcc::generator::TpccGenerator;

/// Parameter generator.
pub enum ParameterGenerator {
    Acid(AcidGenerator),
    Tatp(TatpGenerator),
    Tpcc(TpccGenerator),
    SmallBank(SmallBankGenerator),
}

impl ParameterGenerator {
    /// Get next transaction (async).
    pub async fn get_transaction(&mut self) -> Message {
        use ParameterGenerator::*;
        match self {
            Acid(ref mut gen) => gen.generate(),
            Tatp(ref mut gen) => gen.generate(),
            Tpcc(ref mut gen) => gen.generate(),
            SmallBank(ref mut gen) => gen.generate(),
        }
    }

    /// Get next transaction (sync).
    pub fn get_next(&mut self) -> Message {
        use ParameterGenerator::*;
        match self {
            Acid(ref mut gen) => gen.generate(),
            Tatp(ref mut gen) => gen.generate(),
            Tpcc(ref mut gen) => gen.generate(),
            SmallBank(ref mut gen) => gen.generate(),
        }
    }

    /// Get number of transactions generated.
    pub fn get_generated(&mut self) -> u32 {
        use ParameterGenerator::*;
        match self {
            Acid(ref mut gen) => gen.get_generated(),
            Tatp(ref mut gen) => gen.get_generated(),
            Tpcc(ref mut gen) => gen.get_generated(),
            SmallBank(ref mut gen) => gen.get_generated(),
        }
    }
}

pub trait Generator {
    /// Generate transaction wrapped in as a message.
    fn generate(&mut self) -> Message;

    /// Get the number of transactions generated.
    fn get_generated(&self) -> u32;
}
