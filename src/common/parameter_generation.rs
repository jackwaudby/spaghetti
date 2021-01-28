use crate::common::message::Message;
use crate::workloads::tatp::TatpGenerator;
use crate::workloads::tpcc::TpccGenerator;

/// Parameter generator.
pub enum ParameterGenerator {
    Tatp(TatpGenerator),
    Tpcc(TpccGenerator),
}

impl ParameterGenerator {
    pub fn get_transaction(&mut self) -> Message {
        use ParameterGenerator::*;
        match self {
            Tatp(ref mut gen) => gen.generate(),
            Tpcc(ref mut gen) => gen.generate(),
        }
    }
}

pub trait Generator {
    fn generate(&mut self) -> Message;
}
