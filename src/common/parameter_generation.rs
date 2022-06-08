use crate::common::message::Request;
// use crate::workloads::acid::paramgen::AcidGenerator;
// use crate::workloads::dummy::paramgen::DummyGenerator;
use crate::workloads::smallbank::paramgen::SmallBankGenerator;
// use crate::workloads::tatp::paramgen::TatpGenerator;
// use crate::workloads::ycsb::paramgen::YcsbGenerator;

pub enum ParameterGenerator {
    // Acid(AcidGenerator),
    // Dummy(DummyGenerator),
    SmallBank(SmallBankGenerator),
    // Tatp(TatpGenerator),
    // Ycsb(YcsbGenerator),
}

impl ParameterGenerator {
    pub fn get_next(&mut self) -> Request {
        use ParameterGenerator::*;
        match self {
            // Acid(ref mut gen) => gen.generate(),
            SmallBank(ref mut gen) => gen.generate(),
            // Tatp(ref mut gen) => gen.generate(),
            // Dummy(ref mut gen) => gen.generate(),
            // Ycsb(ref mut gen) => gen.generate(),
        }
    }

    pub fn get_generated(&mut self) -> u32 {
        use ParameterGenerator::*;
        match self {
            // Acid(ref mut gen) => gen.get_generated(),
            SmallBank(ref mut gen) => gen.get_generated(),
            // Tatp(ref mut gen) => gen.get_generated(),
            // Dummy(ref mut gen) => gen.get_generated(),
            // Ycsb(ref mut gen) => gen.get_generated(),
        }
    }
}

pub trait Generator {
    fn generate(&mut self) -> Request;

    fn get_generated(&self) -> u32;
}
