use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::acid::{AcidTransaction, ACID_SF_MAP};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::info;

/////////////////////////////////////////
/// Parameter Generator. ///
////////////////////////////////////////

/// ACID workload transaction generator.
pub struct AcidGenerator {
    /// Persons.
    persons: u64,

    /// Anomaly.
    anomaly: String,

    /// Rng.
    rng: StdRng,

    /// Number of transactions generated.
    pub generated: u32,
}

impl AcidGenerator {
    /// Create new `TatpGenerator`.
    pub fn new(sf: u64, set_seed: bool, seed: Option<u64>, anomaly: &str) -> Self {
        info!("Parameter generator set seed: {}", set_seed);
        let persons = *ACID_SF_MAP.get(&sf).unwrap();

        let rng: StdRng;
        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        AcidGenerator {
            persons,
            rng,
            generated: 0,
            anomaly: anomaly.to_string(),
        }
    }
}

impl Generator for AcidGenerator {
    /// Generate a transaction request.
    fn generate(&mut self) -> Message {
        let n: f32 = self.rng.gen();
        let (transaction, parameters) = self.get_params(n);

        Message::Request {
            request_no: self.generated,
            transaction: Transaction::Acid(transaction),
            parameters: Parameters::Acid(parameters),
        }
    }

    fn get_generated(&self) -> u32 {
        self.generated
    }
}

impl AcidGenerator {
    /// Get a random transaction profile (type, params)
    fn get_params(&mut self, n: f32) -> (AcidTransaction, AcidTransactionProfile) {
        self.generated += 1;

        match self.anomaly.as_str() {
            "g1a" => self.get_g1a_params(n),
            "g1c" => self.get_g1c_params(),

            _ => panic!("anomaly: {} not recognised", self.anomaly),
        }
    }

    /// Get a transaction profile for g1a test.
    fn get_g1a_params(&mut self, n: f32) -> (AcidTransaction, AcidTransactionProfile) {
        match n {
            x if x < 0.5 => {
                // G1A_READ
                let p_id = self.rng.gen_range(0..self.persons);

                let payload = G1aRead { p_id };
                (
                    AcidTransaction::G1aRead,
                    AcidTransactionProfile::G1aRead(payload),
                )
            }

            _ => {
                // G1A_WRITE
                let p_id = self.rng.gen_range(0..self.persons);

                let payload = G1aWrite { p_id, version: 2 };
                (
                    AcidTransaction::G1aWrite,
                    AcidTransactionProfile::G1aWrite(payload),
                )
            }
        }
    }

    /// Get a transaction profile for g1c test.
    fn get_g1c_params(&mut self) -> (AcidTransaction, AcidTransactionProfile) {
        let p1_id = self.rng.gen_range(0..self.persons); // person1 id
        let mut p2_id = p1_id;

        while p1_id == p2_id {
            p2_id = self.rng.gen_range(0..self.persons); // person2 id
        }

        // unique tid; ok as transaction generation is single-threaded
        let transaction_id = self.generated;
        let payload = G1cReadWrite {
            p1_id,
            p2_id,
            transaction_id,
        };
        (
            AcidTransaction::G1cReadWrite,
            AcidTransactionProfile::G1cReadWrite(payload),
        )
    }
}

///////////////////////////////////////
/// Transaction Profiles. ///
//////////////////////////////////////

/// Represents parameters for each transaction.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum AcidTransactionProfile {
    G1aWrite(G1aWrite),
    G1aRead(G1aRead),
    G1cReadWrite(G1cReadWrite),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G1aWrite {
    pub p_id: u64,
    pub version: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G1aRead {
    pub p_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G1cReadWrite {
    pub p1_id: u64,
    pub p2_id: u64,
    pub transaction_id: u32,
}

impl fmt::Display for AcidTransactionProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            AcidTransactionProfile::G1aRead(params) => {
                let G1aRead { p_id } = params;
                write!(f, "0,{}", p_id)
            }
            AcidTransactionProfile::G1aWrite(params) => {
                let G1aWrite { p_id, version } = params;
                write!(f, "1,{},{}", p_id, version)
            }
            AcidTransactionProfile::G1cReadWrite(params) => {
                let G1cReadWrite {
                    p1_id,
                    p2_id,
                    transaction_id,
                } = params;
                write!(f, "2,{},{},{}", p1_id, p2_id, transaction_id)
            }
        }
    }
}
