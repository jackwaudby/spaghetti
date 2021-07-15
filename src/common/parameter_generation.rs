use crate::common::message::Message;
use crate::workloads::acid::paramgen::AcidGenerator;
use crate::workloads::smallbank::paramgen::SmallBankGenerator;
use crate::workloads::tatp::paramgen::TatpGenerator;

pub enum ParameterGenerator {
    Acid(AcidGenerator),
    SmallBank(SmallBankGenerator),
    Tatp(TatpGenerator),
}

impl ParameterGenerator {
    pub fn get_next(&mut self) -> Message {
        use ParameterGenerator::*;
        match self {
            Acid(ref mut gen) => gen.generate(),
            SmallBank(ref mut gen) => gen.generate(),
            Tatp(ref mut gen) => gen.generate(),
        }
    }

    pub fn get_generated(&mut self) -> u32 {
        use ParameterGenerator::*;
        match self {
            Acid(ref mut gen) => gen.get_generated(),
            SmallBank(ref mut gen) => gen.get_generated(),
            Tatp(ref mut gen) => gen.get_generated(),
        }
    }
}

pub trait Generator {
    fn generate(&mut self) -> Message;

    fn get_generated(&self) -> u32;
}
