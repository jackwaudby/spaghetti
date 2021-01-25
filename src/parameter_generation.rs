//! Used by client to generate transaction parameters.

use crate::transaction::Transaction;
use crate::workloads::tatp::TatpGenerator;
use crate::workloads::tpcc::TpccGenerator;

use crate::message::Message;

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

pub trait Generator<T> {
    fn generate(&mut self) -> Box<T>
    where
        T: Transaction;
}
