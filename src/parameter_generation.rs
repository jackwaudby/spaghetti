//! Used by client to generate transaction parameters.

use crate::transaction::Transaction;
use crate::workloads::tatp::TatpGenerator;
use crate::workloads::tpcc::TpccGenerator;

/// Parameter generator.
pub enum ParameterGenerator {
    Tatp(TatpGenerator),
    Tpcc(TpccGenerator),
}

impl ParameterGenerator {
    pub fn get_transaction(&self) -> Box<dyn Transaction + Send> {
        use ParameterGenerator::*;
        match self {
            Tatp(ref gen) => gen.generate(),
            Tpcc(ref gen) => gen.generate(),
        }
    }
}

pub trait Generator<T> {
    fn generate(&self) -> Box<T>
    where
        T: Transaction;
}
