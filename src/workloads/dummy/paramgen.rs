use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::dummy::DummyTransaction;
use crate::workloads::dummy::*;
use crate::workloads::IsolationLevel;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt;

pub struct DummyGenerator {
    thread_id: u32,
    rng: StdRng,
    generated: u32,
    cardinality: usize,
}

impl DummyGenerator {
    pub fn new(thread_id: u32, sf: u64, set_seed: bool, seed: Option<u64>) -> Self {
        let rng: StdRng;

        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        let cardinality = *DUMMY_SF_MAP.get(&sf).unwrap();

        Self {
            thread_id,
            rng,
            generated: 0,
            cardinality,
        }
    }
}

impl Generator for DummyGenerator {
    fn generate(&mut self) -> Message {
        let n: f32 = self.rng.gen();
        let (transaction, parameters) = self.get_params(n);

        let m: f32 = self.rng.gen();
        let isolation = match m {
            x if x < 0.2 => IsolationLevel::ReadUncommitted,
            x if x < 0.6 => IsolationLevel::ReadCommitted,
            _ => IsolationLevel::Serializable,
        };

        Message::Request {
            request_no: (self.thread_id, self.generated),
            transaction: Transaction::Dummy(transaction),
            parameters: Parameters::Dummy(parameters),
            isolation,
        }
    }

    fn get_generated(&self) -> u32 {
        self.generated
    }
}

impl DummyGenerator {
    fn get_params(&mut self, n: f32) -> (DummyTransaction, DummyTransactionProfile) {
        self.generated += 1;

        self.write_only(n)
    }

    pub fn get_offset(&mut self) -> usize {
        self.rng.gen_range(0..self.cardinality)
    }

    pub fn get_value(&mut self) -> u64 {
        self.rng.gen_range(0..=100)
    }

    fn write_only(&mut self, n: f32) -> (DummyTransaction, DummyTransactionProfile) {
        let mut offset1 = self.get_offset();
        let offset2 = self.get_offset();

        if offset1 == offset2 {
            if offset1 == 0 {
                offset1 += 1;
            } else {
                offset1 -= 1;
            }
        }

        let value = self.get_value();
        let payload = Write {
            offset1,
            offset2,
            value,
        };

        if n < 1.0 {
            (
                DummyTransaction::Write,
                DummyTransactionProfile::Write(payload),
            )
        } else {
            (
                DummyTransaction::WriteAbort,
                DummyTransactionProfile::WriteAbort(payload),
            )
        }
    }
}

/// Represents parameters for each transaction.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DummyTransactionProfile {
    Write(Write),
    WriteAbort(Write),
    Read(Read),
    ReadWrite(ReadWrite),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Write {
    pub offset1: usize,
    pub offset2: usize,
    pub value: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Read {
    pub offset: usize,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReadWrite {
    pub offset1: usize,
    pub offset2: usize,
    pub value: u64,
}

impl fmt::Display for DummyTransactionProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            DummyTransactionProfile::Read(params) => {
                let Read { offset } = params;
                write!(f, "0,{}", offset)
            }
            DummyTransactionProfile::Write(params) => {
                let Write {
                    offset1,
                    offset2,
                    value,
                } = params;
                write!(f, "1,{},{},{}", offset1, offset2, value)
            }
            DummyTransactionProfile::WriteAbort(params) => {
                let Write {
                    offset1,
                    offset2,
                    value,
                } = params;
                write!(f, "1,{},{},{}", offset1, offset2, value)
            }
            DummyTransactionProfile::ReadWrite(params) => {
                let ReadWrite {
                    offset1,
                    offset2,
                    value,
                } = params;
                write!(f, "2,{},{},{}", offset1, offset2, value)
            }
        }
    }
}
