use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::acid::{AcidTransaction, ACID_SF_MAP};

use math::round;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt;

/////////////////////////////////////////
/// Parameter Generator. ///
////////////////////////////////////////

/// ACID workload transaction generator.
pub struct AcidGenerator {
    thread_id: u64,
    persons: u64,
    anomaly: String,
    delay: u64,
    rng: StdRng,
    pub generated: u32,
}

impl AcidGenerator {
    pub fn new(sf: u64, set_seed: bool, seed: Option<u64>, anomaly: &str, delay: u64) -> Self {
        let handle = std::thread::current();
        let thread_id: u64 = handle.name().unwrap().parse().unwrap();
        let persons = *ACID_SF_MAP.get(&sf).unwrap();

        let rng: StdRng;
        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        AcidGenerator {
            thread_id,
            persons,
            rng,
            generated: 0,
            anomaly: anomaly.to_string(),
            delay: delay * 1000,
        }
    }
}

impl Generator for AcidGenerator {
    /// Generate a transaction request.
    fn generate(&mut self) -> Message {
        let n: f32 = self.rng.gen();
        let (transaction, parameters) = self.get_params(n);

        let request_no = format!("{}{}", self.thread_id + 1, self.generated);
        let request_no: u32 = request_no.parse().unwrap();

        Message::Request {
            request_no,
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
        match self.anomaly.as_str() {
            "g0" => self.get_g0_params(n),
            "g1a" => self.get_g1a_params(n),
            "g1c" => self.get_g1c_params(),
            "imp" => self.get_imp_params(n),
            "otv" => self.get_otv_params(n),
            "fr" => self.get_otv_params(n), // uses same profiles as otv
            "lu" => self.get_lu_params(),
            "g2item" => self.get_g2item_params(n),
            _ => panic!("anomaly: {} not recognised", self.anomaly),
        }
    }

    /// Get the parameters for a Dirty Write (G0) write transaction.
    ///
    /// Select a unique person pair: (p1) -- [knows] --> (p2).
    /// Transactions are required to have a unique id.
    fn get_g0_params(&mut self, n: f32) -> (AcidTransaction, AcidTransactionProfile) {
        // --- transaction id
        self.generated += 1;
        let request_no = format!("{}{}", self.thread_id + 1, self.generated);
        let transaction_id: u32 = request_no.parse().unwrap();

        // --- person pair
        let mut p1_id;
        let mut p2_id;

        let p_id = self.rng.gen_range(0..self.persons); // person id

        // person pairs go from even id to odd id
        if p_id % 2 == 0 {
            p1_id = p_id;
            p2_id = p_id + 1;
        } else {
            p1_id = p_id - 1;
            p2_id = p_id;
        }

        if n < 0.5 {
            std::mem::swap(&mut p1_id, &mut p2_id)
            // flip so they get accessed in different orders
            // let temp = p1_id;
            // p1_id = p2_id;
            // p2_id = temp;
        }

        let payload = G0Write {
            p1_id,
            p2_id,
            transaction_id,
        };

        (
            AcidTransaction::G0Write,
            AcidTransactionProfile::G0Write(payload),
        )
    }

    /// Get the parameters for a Aborted Read (G1a) read/write transaction.
    ///
    /// Read transaction; selects a random person.
    /// Write transaction; selects a random person and an even version number.
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

                let payload = G1aWrite {
                    p_id,
                    version: 2,
                    delay: self.delay,
                };
                (
                    AcidTransaction::G1aWrite,
                    AcidTransactionProfile::G1aWrite(payload),
                )
            }
        }
    }

    /// Get the parameters for a Circular Info Flow (G1c) read-write transaction.
    ///
    /// Select two different person and unique transaction id.
    fn get_g1c_params(&mut self) -> (AcidTransaction, AcidTransactionProfile) {
        // --- transaction id
        self.generated += 1;
        let request_no = format!("{}{}", self.thread_id + 1, self.generated);
        let transaction_id: u32 = request_no.parse().unwrap();

        // --- person ids
        let p1_id = self.rng.gen_range(0..self.persons); // person1 id
        let mut p2_id = p1_id;
        while p1_id == p2_id {
            p2_id = self.rng.gen_range(0..self.persons); // person2 id
        }

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

    /// Get the parameters for a Item-many-preceders (IMP) transaction.
    ///
    /// Read transaction; selects a random person and reads the version numbers.
    /// Write transaction; selects a random person and incremenet version number.
    fn get_imp_params(&mut self, n: f32) -> (AcidTransaction, AcidTransactionProfile) {
        let p_id = self.rng.gen_range(0..self.persons - 1);
        match n {
            x if x < 0.5 => {
                let payload = ImpRead {
                    p_id,
                    delay: self.delay,
                };
                (
                    AcidTransaction::ImpRead,
                    AcidTransactionProfile::ImpRead(payload),
                )
            }

            _ => {
                let payload = ImpWrite { p_id };
                (
                    AcidTransaction::ImpWrite,
                    AcidTransactionProfile::ImpWrite(payload),
                )
            }
        }
    }

    /// Get a transaction profile for OTV test.
    ///
    /// OTV Read/Write transactions are generated with equal chance.
    /// The parameters for both transaction are a disjoint person sequence of length 4.
    fn get_otv_params(&mut self, n: f32) -> (AcidTransaction, AcidTransactionProfile) {
        let p = self.persons;

        let id = self.rng.gen_range(0..p) as f64;
        let id = id / 4.0;
        let rounded = (round::floor(id, 0) * 4.0) as u64;
        let p1_id = rounded;
        let p2_id = rounded + 1;
        let p3_id = rounded + 2;
        let p4_id = rounded + 3;

        let payload = Otv {
            p1_id,
            p2_id,
            p3_id,
            p4_id,
        };

        match n {
            x if x < 0.5 => (
                AcidTransaction::OtvRead,
                AcidTransactionProfile::OtvRead(payload),
            ),

            _ => (
                AcidTransaction::OtvWrite,
                AcidTransactionProfile::OtvWrite(payload),
            ),
        }
    }

    /// Get a transaction profile for LU test.
    fn get_lu_params(&mut self) -> (AcidTransaction, AcidTransactionProfile) {
        let p_id = self.rng.gen_range(0..self.persons); // person id

        let payload = LostUpdateWrite { p_id };
        (
            AcidTransaction::LostUpdateWrite,
            AcidTransactionProfile::LostUpdateWrite(payload),
        )
    }

    /// Get a transaction profile for the Write Skew (G2-item) test.
    ///
    /// Returns a G2-item Write transactions, as G2-item Read transactions are issued after execution.
    /// Selects a person pair and an update person from the pair.
    fn get_g2item_params(&mut self, n: f32) -> (AcidTransaction, AcidTransactionProfile) {
        let p = self.persons;

        let id = self.rng.gen_range(0..p) as f64;
        let id = id / 2.0;
        let rounded = (round::floor(id, 0) * 2.0) as u64;
        let p1_id = rounded;
        let p2_id = rounded + 1;

        let p_id_update = match n {
            x if x < 0.5 => p1_id,

            _ => p2_id,
        };

        let payload = G2itemWrite {
            p1_id,
            p2_id,
            p_id_update,
        };

        (
            AcidTransaction::G2itemWrite,
            AcidTransactionProfile::G2itemWrite(payload),
        )
    }
}

///////////////////////////////////////
/// Transaction Profiles. ///
//////////////////////////////////////

/// Represents parameters for each transaction.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum AcidTransactionProfile {
    G0Write(G0Write),
    G0Read(G0Read),
    G1aWrite(G1aWrite),
    G1aRead(G1aRead),
    G1cReadWrite(G1cReadWrite),
    ImpRead(ImpRead),
    ImpWrite(ImpWrite),
    OtvRead(Otv),
    OtvWrite(Otv),
    LostUpdateRead(LostUpdateRead),
    LostUpdateWrite(LostUpdateWrite),
    G2itemWrite(G2itemWrite),
    G2itemRead(G2itemRead),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G0Write {
    pub p1_id: u64,
    pub p2_id: u64,
    pub transaction_id: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G0Read {
    pub p1_id: u64,
    pub p2_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G1aWrite {
    pub p_id: u64,
    pub version: u64,
    pub delay: u64,
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct ImpRead {
    pub p_id: u64,
    pub delay: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct ImpWrite {
    pub p_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct Otv {
    pub p1_id: u64,
    pub p2_id: u64,
    pub p3_id: u64,
    pub p4_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct LostUpdateWrite {
    pub p_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct LostUpdateRead {
    pub p_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G2itemWrite {
    pub p1_id: u64,
    pub p2_id: u64,
    pub p_id_update: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct G2itemRead {
    pub p1_id: u64,
    pub p2_id: u64,
}

impl fmt::Display for AcidTransactionProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            AcidTransactionProfile::G0Read(params) => {
                let G0Read { p1_id, p2_id } = params;
                write!(f, "0,{},{}", p1_id, p2_id)
            }
            AcidTransactionProfile::G0Write(params) => {
                let G0Write {
                    p1_id,
                    p2_id,
                    transaction_id,
                } = params;
                write!(f, "1,{},{},{}", p1_id, p2_id, transaction_id)
            }
            AcidTransactionProfile::G1aRead(params) => {
                let G1aRead { p_id } = params;
                write!(f, "2,{}", p_id)
            }
            AcidTransactionProfile::G1aWrite(params) => {
                let G1aWrite { p_id, version, .. } = params;
                write!(f, "3,{},{}", p_id, version)
            }
            AcidTransactionProfile::G1cReadWrite(params) => {
                let G1cReadWrite {
                    p1_id,
                    p2_id,
                    transaction_id,
                } = params;
                write!(f, "6,{},{},{}", p1_id, p2_id, transaction_id)
            }
            AcidTransactionProfile::ImpRead(params) => {
                let ImpRead { p_id, .. } = params;
                write!(f, "7,{}", p_id)
            }
            AcidTransactionProfile::ImpWrite(params) => {
                let ImpWrite { p_id } = params;
                write!(f, "8,{}", p_id)
            }
            AcidTransactionProfile::OtvRead(params) => {
                let Otv {
                    p1_id,
                    p2_id,
                    p3_id,
                    p4_id,
                } = params;
                write!(f, "9,{},{},{},{}", p1_id, p2_id, p3_id, p4_id)
            }
            AcidTransactionProfile::OtvWrite(params) => {
                let Otv {
                    p1_id,
                    p2_id,
                    p3_id,
                    p4_id,
                } = params;
                write!(f, "10,{},{},{},{}", p1_id, p2_id, p3_id, p4_id)
            }
            AcidTransactionProfile::LostUpdateRead(params) => {
                let LostUpdateRead { p_id } = params;
                write!(f, "11,{}", p_id)
            }
            AcidTransactionProfile::LostUpdateWrite(params) => {
                let LostUpdateWrite { p_id } = params;
                write!(f, "12,{}", p_id)
            }
            AcidTransactionProfile::G2itemWrite(params) => {
                let G2itemWrite {
                    p1_id,
                    p2_id,
                    p_id_update,
                } = params;
                write!(f, "13,{},{},{}", p1_id, p2_id, p_id_update)
            }
            AcidTransactionProfile::G2itemRead(params) => {
                let G2itemRead { p1_id, p2_id } = params;
                write!(f, "14,{},{}", p1_id, p2_id)
            }
        }
    }
}
