use crate::common::message::Request;
use crate::workloads::{
    smallbank::paramgen::SmallBankGenerator, tatp::paramgen::TatpGenerator,
    ycsb::paramgen::YcsbGenerator,
};

pub enum ParameterGenerator {
    SmallBank(SmallBankGenerator),
    Tatp(TatpGenerator),
    Ycsb(YcsbGenerator),
}

impl ParameterGenerator {
    pub fn get_next(&mut self) -> Request {
        use ParameterGenerator::*;
        match self {
            SmallBank(ref mut gen) => gen.generate(),
            Tatp(ref mut gen) => gen.generate(),
            Ycsb(ref mut gen) => gen.generate(),
        }
    }

    pub fn get_generated(&mut self) -> u32 {
        use ParameterGenerator::*;
        match self {
            SmallBank(ref mut gen) => gen.get_generated(),
            Tatp(ref mut gen) => gen.get_generated(),
            Ycsb(ref mut gen) => gen.get_generated(),
        }
    }
}

pub trait Generator {
    fn generate(&mut self) -> Request;

    fn get_generated(&self) -> u32;
}
