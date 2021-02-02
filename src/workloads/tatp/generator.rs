use crate::common::message::Message;
use crate::common::parameter_generation::Generator;
use crate::workloads::tatp::helper;
use crate::workloads::tatp::profiles::{
    DeleteCallForwarding, GetAccessData, GetNewDestination, GetSubscriberData,
    InsertCallForwarding, TatpTransaction, UpdateLocationData, UpdateSubscriberData,
};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/////////////////////////////////////////
/// Parameter Generator. ///
////////////////////////////////////////

pub struct TatpGenerator {
    subscribers: u64,
    rng: StdRng,
}

impl TatpGenerator {
    pub fn new(subscribers: u64) -> TatpGenerator {
        // Initialise the thread local rng.
        let rng: StdRng = SeedableRng::from_entropy();

        TatpGenerator { subscribers, rng }
    }
}

impl Generator for TatpGenerator {
    fn generate(&mut self) -> Message {
        let n: f32 = self.rng.gen();

        let transaction = match n {
            x if x < 0.35 => {
                // GET_SUBSCRIBER_DATA
                // TODO: implement non-uniform distribution.
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let payload = GetSubscriberData { s_id };
                TatpTransaction::GetSubscriberData(payload)
            }

            x if x < 0.45 => {
                // GET_NEW_DESTINATION
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let end_time = self.rng.gen_range(1..=24);
                let payload = GetNewDestination {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                };
                TatpTransaction::GetNewDestination(payload)
            }
            x if x < 0.8 => {
                // GET_ACCESS_DATA
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let ai_type = self.rng.gen_range(1..=4);
                let payload = GetAccessData { s_id, ai_type };
                TatpTransaction::GetAccessData(payload)
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
                TatpTransaction::UpdateSubscriberData(payload)
            }
            x if x < 0.96 => {
                // UPDATE_LOCATION
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let vlr_location = self.rng.gen_range(1..=2 ^ 32 - 1);
                let payload = UpdateLocationData { s_id, vlr_location };
                TatpTransaction::UpdateLocationData(payload)
            }
            x if x < 0.98 => {
                // INSERT CALL_FORWARDING
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let end_time = self.rng.gen_range(1..=24);
                let number_x = helper::get_number_x(&mut self.rng);
                let payload = InsertCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                    number_x,
                };
                TatpTransaction::InsertCallForwarding(payload)
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
                TatpTransaction::DeleteCallForwarding(payload)
            }
        };

        Message::TatpTransaction(transaction)
    }
}
