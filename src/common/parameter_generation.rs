use crate::common::message::Message;
use crate::workloads::tatp::generator::TatpGenerator;
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
}

pub trait Generator {
    fn generate(&mut self) -> Message;
}
