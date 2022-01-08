use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::tatp::helper;
use crate::workloads::tatp::{TatpTransaction, TATP_SF_MAP};
use crate::workloads::IsolationLevel;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt;

/// TATP workload transaction generator.
pub struct TatpGenerator {
    thread_id: u32,
    subscribers: u64,
    rng: StdRng,
    use_nurand: bool,
    generated: u32,
}

impl TatpGenerator {
    pub fn new(
        thread_id: u32,
        sf: u64,
        set_seed: bool,
        seed: Option<u64>,
        use_nurand: bool,
    ) -> Self {
        let rng: StdRng;

        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        let subscribers = *TATP_SF_MAP.get(&sf).unwrap();

        TatpGenerator {
            thread_id,
            subscribers,
            rng,
            generated: 0,
            use_nurand,
        }
    }
}

impl Generator for TatpGenerator {
    /// Generate a transaction request.
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
            transaction: Transaction::Tatp(transaction),
            parameters: Parameters::Tatp(parameters),
            isolation,
        }
    }

    fn get_generated(&self) -> u32 {
        self.generated
    }
}

impl TatpGenerator {
    /// Get a random transaction profile (type, params)
    fn get_params(&mut self, n: f32) -> (TatpTransaction, TatpTransactionProfile) {
        self.generated += 1;
        match n {
            x if x < 0.35 => {
                // GET_SUBSCRIBER_DATA
                let s_id;
                if self.use_nurand {
                    s_id = helper::nurand_sid(&mut self.rng, self.subscribers, 1);
                } else {
                    s_id = self.rng.gen_range(0..self.subscribers);
                }
                let payload = GetSubscriberData { s_id };
                (
                    TatpTransaction::GetSubscriberData,
                    TatpTransactionProfile::GetSubscriberData(payload),
                )
            }
            x if x < 0.45 => {
                // GET_NEW_DESTINATION
                let s_id = self.rng.gen_range(0..self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let end_time = start_time + self.rng.gen_range(1..=8);
                let payload = GetNewDestination {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                };
                (
                    TatpTransaction::GetNewDestination,
                    TatpTransactionProfile::GetNewDestination(payload),
                )
            }
            x if x < 0.8 => {
                // GET_ACCESS_DATA
                let s_id = self.rng.gen_range(0..self.subscribers);
                let ai_type = self.rng.gen_range(1..=4);
                let payload = GetAccessData { s_id, ai_type };
                (
                    TatpTransaction::GetAccessData,
                    TatpTransactionProfile::GetAccessData(payload),
                )
            }
            x if x < 0.82 => {
                // UPDATE_SUBSCRIBER_DATA
                let s_id = self.rng.gen_range(0..self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let bit_1 = self.rng.gen_range(0..=1);
                let data_a = self.rng.gen_range(0..=255);
                let payload = UpdateSubscriberData {
                    s_id,
                    sf_type,
                    bit_1,
                    data_a,
                };
                (
                    TatpTransaction::UpdateSubscriberData,
                    TatpTransactionProfile::UpdateSubscriberData(payload),
                )
            }

            _ => {
                // UPDATE_LOCATION
                let s_id = self.rng.gen_range(0..self.subscribers);
                let vlr_location = self.rng.gen_range(1..(2 ^ 32));
                let payload = UpdateLocationData { s_id, vlr_location };
                (
                    TatpTransaction::UpdateLocationData,
                    TatpTransactionProfile::UpdateLocationData(payload),
                )
            }
        }
    }
}

/// Represents parameters for each transaction.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TatpTransactionProfile {
    GetSubscriberData(GetSubscriberData),
    GetNewDestination(GetNewDestination),
    GetAccessData(GetAccessData),
    UpdateSubscriberData(UpdateSubscriberData),
    UpdateLocationData(UpdateLocationData),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct GetSubscriberData {
    pub s_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct GetNewDestination {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct GetAccessData {
    pub s_id: u64,
    pub ai_type: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct UpdateSubscriberData {
    pub s_id: u64,
    pub sf_type: u8,
    pub bit_1: u8,
    pub data_a: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct UpdateLocationData {
    pub s_id: u64,
    pub vlr_location: u8,
}

impl fmt::Display for TatpTransactionProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            TatpTransactionProfile::GetSubscriberData(params) => {
                let GetSubscriberData { s_id } = params;
                write!(f, "0,{}", s_id)
            }
            TatpTransactionProfile::GetNewDestination(params) => {
                let GetNewDestination {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                } = params;
                write!(f, "1,{},{},{},{}", s_id, sf_type, start_time, end_time)
            }
            TatpTransactionProfile::GetAccessData(params) => {
                let GetAccessData { s_id, ai_type } = params;
                write!(f, "2,{},{}", s_id, ai_type)
            }
            TatpTransactionProfile::UpdateLocationData(params) => {
                let UpdateLocationData { s_id, vlr_location } = params;
                write!(f, "3,{},{}", s_id, vlr_location)
            }
            TatpTransactionProfile::UpdateSubscriberData(params) => {
                let UpdateSubscriberData {
                    s_id,
                    sf_type,
                    bit_1,
                    data_a,
                } = params;
                write!(f, "4,{},{},{},{}", s_id, sf_type, bit_1, data_a)
            }
        }
    }
}
