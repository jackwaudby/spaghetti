use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::tatp::helper;
use crate::workloads::tatp::{TatpTransaction, TATP_SF_MAP};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::debug;

/////////////////////////////////////////
/// Parameter Generator. ///
////////////////////////////////////////

/// TATP workload transaction generator.
pub struct TatpGenerator {
    /// Subscribers in the workload.
    subscribers: u64,

    /// Rng.
    rng: StdRng,

    /// Use non-uniform distribution.
    use_nurand: bool,

    /// Number of transactions generated.
    pub generated: u32,
}

impl TatpGenerator {
    /// Create new `TatpGenerator`.
    pub fn new(sf: u64, set_seed: bool, seed: Option<u64>, use_nurand: bool) -> TatpGenerator {
        debug!("Parameter generator set seed: {}", set_seed);
        debug!("Non-uniform parameter generator: {}", use_nurand);
        let subscribers = *TATP_SF_MAP.get(&sf).unwrap(); // get subscribers for sf
        let rng: StdRng; // rng
        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        TatpGenerator {
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

        Message::Request {
            request_no: self.generated,
            transaction: Transaction::Tatp(transaction),
            parameters: Parameters::Tatp(parameters),
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
                    s_id = self.rng.gen_range(1..=self.subscribers);
                }
                let payload = GetSubscriberData { s_id };
                (
                    TatpTransaction::GetSubscriberData,
                    TatpTransactionProfile::GetSubscriberData(payload),
                )
            }

            x if x < 0.45 => {
                // GET_NEW_DESTINATION
                let s_id = self.rng.gen_range(1..=self.subscribers);
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
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let ai_type = self.rng.gen_range(1..=4);
                let payload = GetAccessData { s_id, ai_type };
                (
                    TatpTransaction::GetAccessData,
                    TatpTransactionProfile::GetAccessData(payload),
                )
            }
            x if x < 0.82 => {
                // UPDATE_SUBSCRIBER_DATA
                let s_id = self.rng.gen_range(1..=self.subscribers);
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
            x if x < 0.96 => {
                // UPDATE_LOCATION
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let vlr_location = self.rng.gen_range(1..(2 ^ 32));
                let payload = UpdateLocationData { s_id, vlr_location };
                (
                    TatpTransaction::UpdateLocationData,
                    TatpTransactionProfile::UpdateLocationData(payload),
                )
            }
            x if x < 0.98 => {
                // INSERT CALL_FORWARDING
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let end_time = start_time + self.rng.gen_range(1..=8);
                let number_x = helper::get_number_x(&mut self.rng);
                let payload = InsertCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                    number_x,
                };
                (
                    TatpTransaction::InsertCallForwarding,
                    TatpTransactionProfile::InsertCallForwarding(payload),
                )
            }
            _ => {
                // DELETE_CALL_FORWARDING
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let payload = DeleteCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                };
                (
                    TatpTransaction::DeleteCallForwarding,
                    TatpTransactionProfile::DeleteCallForwarding(payload),
                )
            }
        }
    }
}

///////////////////////////////////////
/// Transaction Profiles. ///
//////////////////////////////////////

/// Represents parameters for each transaction.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TatpTransactionProfile {
    GetSubscriberData(GetSubscriberData),
    GetNewDestination(GetNewDestination),
    GetAccessData(GetAccessData),
    UpdateSubscriberData(UpdateSubscriberData),
    UpdateLocationData(UpdateLocationData),
    InsertCallForwarding(InsertCallForwarding),
    DeleteCallForwarding(DeleteCallForwarding),
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct InsertCallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
    pub number_x: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct DeleteCallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
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
            TatpTransactionProfile::InsertCallForwarding(params) => {
                let InsertCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                    number_x,
                } = params;
                write!(
                    f,
                    "5,{},{},{},{},{}",
                    s_id, sf_type, start_time, end_time, number_x
                )
            }
            TatpTransactionProfile::DeleteCallForwarding(params) => {
                let DeleteCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                } = params;
                write!(f, "6,{},{},{}", s_id, sf_type, start_time)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_env_log::test;

    #[test]
    fn generate_test() {
        let mut gen = TatpGenerator::new(1, true, Some(42), false);
        assert_eq!(
            (
                TatpTransaction::GetSubscriberData,
                TatpTransactionProfile::GetSubscriberData(GetSubscriberData { s_id: 52656 })
            ),
            gen.get_params(0.1)
        );
        assert_eq!(
            (
                TatpTransaction::GetNewDestination,
                TatpTransactionProfile::GetNewDestination(GetNewDestination {
                    s_id: 54273,
                    sf_type: 4,
                    start_time: 8,
                    end_time: 16
                })
            ),
            gen.get_params(0.4)
        );
        assert_eq!(
            (
                TatpTransaction::GetAccessData,
                TatpTransactionProfile::GetAccessData(GetAccessData {
                    s_id: 61743,
                    ai_type: 2
                })
            ),
            gen.get_params(0.7)
        );
        assert_eq!(
            (
                TatpTransaction::UpdateSubscriberData,
                TatpTransactionProfile::UpdateSubscriberData(UpdateSubscriberData {
                    s_id: 73743,
                    sf_type: 1,
                    bit_1: 1,
                    data_a: 94
                })
            ),
            gen.get_params(0.81)
        );
        assert_eq!(
            (
                TatpTransaction::UpdateLocationData,
                TatpTransactionProfile::UpdateLocationData(UpdateLocationData {
                    s_id: 47941,
                    vlr_location: 1
                })
            ),
            gen.get_params(0.93)
        );
        assert_eq!(
            (
                TatpTransaction::InsertCallForwarding,
                TatpTransactionProfile::InsertCallForwarding(InsertCallForwarding {
                    s_id: 93215,
                    sf_type: 3,
                    start_time: 8,
                    end_time: 15,
                    number_x: "788614624315946".to_string()
                })
            ),
            gen.get_params(0.97)
        );
        assert_eq!(
            (
                TatpTransaction::DeleteCallForwarding,
                TatpTransactionProfile::DeleteCallForwarding(DeleteCallForwarding {
                    s_id: 80873,
                    sf_type: 1,
                    start_time: 8
                })
            ),
            gen.get_params(0.99)
        );
    }
}
