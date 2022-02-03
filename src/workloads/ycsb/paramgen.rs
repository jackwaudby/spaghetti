use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::ycsb::helper;
use crate::workloads::ycsb::YcsbTransaction;
use crate::workloads::ycsb::*;
use crate::workloads::IsolationLevel;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

pub struct YcsbGenerator {
    thread_id: u32,
    rng: StdRng,
    generated: u32,
    cardinality: usize,
    theta: f64,
    update_rate: f64,
}

impl YcsbGenerator {
    pub fn new(
        thread_id: u32,
        sf: u64,
        set_seed: bool,
        seed: Option<u64>,
        theta: f64,
        update_rate: f64,
    ) -> Self {
        let rng: StdRng;

        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        let cardinality = *YCSB_SF_MAP.get(&sf).unwrap();

        Self {
            thread_id,
            rng,
            generated: 0,
            cardinality,
            theta,
            update_rate,
        }
    }
}

impl Generator for YcsbGenerator {
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
            transaction: Transaction::Ycsb(transaction),
            parameters: Parameters::Ycsb(parameters),
            isolation,
        }
    }

    fn get_generated(&self) -> u32 {
        self.generated
    }
}

impl YcsbGenerator {
    fn get_params(&mut self, n: f32) -> (YcsbTransaction, YcsbTransactionProfile) {
        self.generated += 1;
        let mut operations = Vec::new();

        let mut unique = HashSet::new();

        if n < self.update_rate as f32 {
            // update txn
            for _ in 0..10 {
                let mut offset;
                loop {
                    offset = helper::zipf(&mut self.rng, self.cardinality, self.theta);
                    if unique.contains(&offset) {
                        continue;
                    } else {
                        unique.insert(offset);
                        break;
                    }
                }

                let x: f32 = self.rng.gen();
                if x < 0.5 {
                    operations.push(Operation::Read(offset - 1));
                } else {
                    let value = helper::generate_random_string(&mut self.rng);

                    operations.push(Operation::Update(offset - 1, value));
                }
            }
        } else {
            // read txn
            for _ in 0..10 {
                let mut offset;
                loop {
                    offset = helper::zipf(&mut self.rng, self.cardinality, self.theta);
                    if unique.contains(&offset) {
                        continue;
                    } else {
                        unique.insert(offset);
                        break;
                    }
                }

                operations.push(Operation::Read(offset - 1));
            }
        }

        (
            YcsbTransaction::General,
            YcsbTransactionProfile::General(Operations(operations)),
        )
    }
}

/// Represents parameters for each transaction.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum YcsbTransactionProfile {
    General(Operations),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Operations(pub Vec<Operation>);

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Operation {
    // offset
    Read(usize),

    // offset, value
    Update(usize, String),
}

impl fmt::Display for YcsbTransactionProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            YcsbTransactionProfile::General(_) => {
                write!(f, "TODO")
            }
        }
    }
}
