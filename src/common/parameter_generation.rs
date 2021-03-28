use crate::common::message::Message;
use crate::workloads::tatp::paramgen::TatpGenerator;
use crate::workloads::tpcc::generator::TpccGenerator;

/// Parameter generator.
pub enum ParameterGenerator {
    Tatp(TatpGenerator),
    Tpcc(TpccGenerator),
}

impl ParameterGenerator {
    pub async fn get_transaction(&mut self) -> Message {
        use ParameterGenerator::*;
        match self {
            Tatp(ref mut gen) => gen.generate(),
            Tpcc(ref mut gen) => gen.generate(),
        }
    }

    pub fn get_generated(&mut self) -> u32 {
        use ParameterGenerator::*;
        match self {
            Tatp(ref mut gen) => gen.get_generated(),
            Tpcc(ref mut gen) => gen.get_generated(),
        }
    }
}

pub trait Generator {
    fn generate(&mut self) -> Message;
    fn get_generated(&self) -> u32;
}
