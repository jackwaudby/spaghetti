use crate::common::isolation_level::IsolationLevel;
use crate::common::message::{Parameters, Request, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::ycsb::{helper, YcsbTransaction, *};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub struct YcsbGenerator {
    core_id: usize,
    rng: StdRng,
    generated: u32,
    cardinality: usize,
    theta: f64,
    update_rate: f64,
    serializable_rate: f64,
    read_uncommitted_rate: f64,
    queries: u64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl YcsbGenerator {
    pub fn new(
        core_id: usize,
        sf: u64,
        set_seed: bool,
        seed: Option<u64>,
        theta: f64,
        update_rate: f64,
        serializable_rate: f64,
        queries: u64,
    ) -> Self {
        let rng: StdRng;

        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        let cardinality = *YCSB_SF_MAP.get(&sf).unwrap();

        let alpha = helper::alpha(theta);
        let zetan = helper::zeta(cardinality, theta);
        let zeta_2_thetan = helper::zeta_2_theta(theta);
        let eta = helper::eta(cardinality, theta, zeta_2_thetan, zetan);

        let weak_rate = 1.0 - serializable_rate;
        let read_uncommitted_rate = serializable_rate + (weak_rate / 10.0);

        Self {
            core_id,
            rng,
            generated: 0,
            cardinality,
            theta,
            update_rate,
            serializable_rate,
            read_uncommitted_rate,
            alpha,
            zetan,
            eta,
            queries,
        }
    }
}

impl Generator for YcsbGenerator {
    fn generate(&mut self) -> Request {
        self.inc_generated();

        let parameters = self.get_parameters();
        let isolation = self.get_isolation_level();

        Request::new(
            (self.core_id, self.generated),
            Transaction::Ycsb(YcsbTransaction::General),
            Parameters::Ycsb(parameters),
            isolation,
        )
    }

    fn get_generated(&self) -> u32 {
        self.generated
    }
}

impl YcsbGenerator {
    fn inc_generated(&mut self) {
        self.generated += 1;
    }

    fn get_isolation_level(&mut self) -> IsolationLevel {
        let n: f64 = self.rng.gen();

        if n < self.serializable_rate {
            IsolationLevel::Serializable
        } else if n < self.read_uncommitted_rate {
            IsolationLevel::ReadUncommitted
        } else {
            IsolationLevel::ReadCommitted
        }
    }

    fn is_update_transaction(&mut self) -> bool {
        let n: f64 = self.rng.gen();
        n < self.update_rate
    }

    fn is_update_operation(&mut self) -> bool {
        let n: f64 = self.rng.gen();
        n < 0.5
    }

    fn get_parameters(&mut self) -> YcsbTransactionProfile {
        let mut operations = Vec::new();
        let mut unique = HashSet::new();

        if self.is_update_transaction() {
            let mut updates = 0;

            for _ in 0..self.queries {
                let offset = self.get_unique_offset(&mut unique);

                if self.is_update_operation() && (updates < (self.queries / 2)) {
                    let value = helper::generate_random_string(&mut self.rng);
                    operations.push(Operation::Update(offset - 1, value));
                    updates += 1;
                } else {
                    operations.push(Operation::Read(offset - 1));
                }
            }
        } else {
            for _ in 0..self.queries {
                let offset = self.get_unique_offset(&mut unique);
                operations.push(Operation::Read(offset - 1));
            }
        }

        YcsbTransactionProfile::General(Operations(operations))
    }

    fn get_unique_offset(&mut self, unique: &mut HashSet<usize>) -> usize {
        let mut offset;

        loop {
            offset = helper::zipf2(
                &mut self.rng,
                self.cardinality,
                self.theta,
                self.alpha,
                self.zetan,
                self.eta,
            );

            if unique.contains(&offset) {
                continue;
            } else {
                unique.insert(offset);
                break;
            }
        }

        offset
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum YcsbTransactionProfile {
    General(Operations),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Operations(Vec<Operation>);

impl Operations {
    pub fn get_operations(self) -> Vec<Operation> {
        self.0
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Operation {
    Read(usize),
    Update(usize, String),
}
