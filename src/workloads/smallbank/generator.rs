//use crate::common::message::{Message, Parameters, Transaction};
// use crate::common::parameter_generation::Generator;
//use crate::workloads::smallbank::profiles::Amalgamate;
//use crate::workloads::smallbank::SmallBankTransaction;

use config::Config;
use rand::rngs::StdRng;
//use rand::{Rng, SeedableRng};
use rand::SeedableRng;
use std::sync::Arc;

/// SmallBank workload transaction generator.
pub struct SmallBankGenerator {
    /// Rng.
    rng: StdRng,

    /// Number of transactions generated.
    pub generated: u32,

    /// Configuration.
    config: Arc<Config>,
}

impl SmallBankGenerator {
    /// Create new `SmallBankGenerator`.
    pub fn new(config: Arc<Config>) -> SmallBankGenerator {
        let set_seed = config.get_bool("set_seed").unwrap();

        let rng: StdRng;
        if set_seed {
            rng = SeedableRng::seed_from_u64(1);
        } else {
            rng = SeedableRng::from_entropy();
        }
        SmallBankGenerator {
            rng,
            generated: 0,
            config,
        }
    }
}

// impl Generator for SmallBankGenerator {
//     /// Generate a transaction request.
//     fn generate(&mut self) -> Message {
//         let n: f32 = self.rng.gen();
//         let (transaction, parameters) = self.get_params(n);

//         Message::Request {
//             request_no: self.generated,
//             transaction: Transaction::SmallBank(transaction),
//             parameters: Parameters::SmallBank(parameters),
//         }
//     }

//     /// Get number of transactions generated.
//     fn get_generated(&self) -> u32 {
//         self.generated
//     }
// }

// impl SmallBankGenerator {
//     fn create_hotspot(&self) {
//         let hotspot_ids = vec![];
//         let use_fixed_size = self.config.get_bool("hotspot_use_fixed_size").unwrap();

//         if use_fixed_size {
//             let fixed_size = self.config.get_int("hotspot_fixed_size").unwrap() as u64;

//         } else {
//         }
//     }

//     // Get customer id
//     // create hotsopt
//     // if hotspot_use_fixed_size = true
//     // then select 100 random ids from the num_accounts
//     // else calculate hotspot size using percentage
// }
//     /// Get a random transaction profile (type, params)
//     fn get_params(&mut self, n: f32) -> (TatpTransaction, TatpTransactionProfile) {
//         self.generated += 1;
//         match n {
//             x if x < 0.35 => {
//                 // GET_SUBSCRIBER_DATA
//                 let s_id = self.rng.gen_range(1..=self.subscribers);
//                 let payload = GetSubscriberData { s_id };
//                 (
//                     TatpTransaction::GetSubscriberData,
//                     TatpTransactionProfile::GetSubscriberData(payload),
//                 )
//             }

//             x if x < 0.45 => {
//                 // GET_NEW_DESTINATION
//                 let s_id = self.rng.gen_range(1..=self.subscribers);
//                 let sf_type = self.rng.gen_range(1..=4);
//                 let start_time = helper::get_start_time(&mut self.rng);
//                 let end_time = start_time + self.rng.gen_range(1..=8);
//                 let payload = GetNewDestination {
//                     s_id,
//                     sf_type,
//                     start_time,
//                     end_time,
//                 };
//                 (
//                     TatpTransaction::GetNewDestination,
//                     TatpTransactionProfile::GetNewDestination(payload),
//                 )
//             }
//             x if x < 0.8 => {
//                 // GET_ACCESS_DATA
//                 let s_id = self.rng.gen_range(1..=self.subscribers);
//                 let ai_type = self.rng.gen_range(1..=4);
//                 let payload = GetAccessData { s_id, ai_type };
//                 (
//                     TatpTransaction::GetAccessData,
//                     TatpTransactionProfile::GetAccessData(payload),
//                 )
//             }
//             x if x < 0.82 => {
//                 // UPDATE_SUBSCRIBER_DATA
//                 let s_id = self.rng.gen_range(1..=self.subscribers);
//                 let sf_type = self.rng.gen_range(1..=4);
//                 let bit_1 = self.rng.gen_range(0..=1);
//                 let data_a = self.rng.gen_range(0..=255);
//                 let payload = UpdateSubscriberData {
//                     s_id,
//                     sf_type,
//                     bit_1,
//                     data_a,
//                 };
//                 (
//                     TatpTransaction::UpdateSubscriberData,
//                     TatpTransactionProfile::UpdateSubscriberData(payload),
//                 )
//             }
//             x if x < 0.96 => {
//                 // UPDATE_LOCATION
//                 let s_id = self.rng.gen_range(1..=self.subscribers);
//                 let vlr_location = self.rng.gen_range(1..=2 ^ 32 - 1);
//                 let payload = UpdateLocationData { s_id, vlr_location };
//                 (
//                     TatpTransaction::UpdateLocationData,
//                     TatpTransactionProfile::UpdateLocationData(payload),
//                 )
//             }
//             x if x < 0.98 => {
//                 // INSERT CALL_FORWARDING
//                 let s_id = self.rng.gen_range(1..=self.subscribers);
//                 let sf_type = self.rng.gen_range(1..=4);
//                 let start_time = helper::get_start_time(&mut self.rng);
//                 let end_time = start_time + self.rng.gen_range(1..=8);
//                 let number_x = helper::get_number_x(&mut self.rng);
//                 let payload = InsertCallForwarding {
//                     s_id,
//                     sf_type,
//                     start_time,
//                     end_time,
//                     number_x,
//                 };
//                 (
//                     TatpTransaction::InsertCallForwarding,
//                     TatpTransactionProfile::InsertCallForwarding(payload),
//                 )
//             }
//             _ => {
//                 // DELETE_CALL_FORWARDING
//                 let s_id = self.rng.gen_range(1..=self.subscribers);
//                 let sf_type = self.rng.gen_range(1..=4);
//                 let start_time = helper::get_start_time(&mut self.rng);
//                 let payload = DeleteCallForwarding {
//                     s_id,
//                     sf_type,
//                     start_time,
//                 };
//                 (
//                     TatpTransaction::DeleteCallForwarding,
//                     TatpTransactionProfile::DeleteCallForwarding(payload),
//                 )
//             }
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::Once;
//     use tracing::Level;
//     use tracing_subscriber::FmtSubscriber;

//     static LOG: Once = Once::new();

//     fn logging(on: bool) {
//         if on {
//             LOG.call_once(|| {
//                 let subscriber = FmtSubscriber::builder()
//                     .with_max_level(Level::DEBUG)
//                     .finish();
//                 tracing::subscriber::set_global_default(subscriber)
//                     .expect("setting default subscriber failed");
//             });
//         }
//     }

//     #[test]
//     fn generate_test() {
//         logging(false);
//         let mut gen = TatpGenerator::new(10, true);
//         assert_eq!(
//             (
//                 TatpTransaction::GetSubscriberData,
//                 TatpTransactionProfile::GetSubscriberData(GetSubscriberData { s_id: 5 })
//             ),
//             gen.get_params(0.1)
//         );
//         assert_eq!(
//             (
//                 TatpTransaction::GetNewDestination,
//                 TatpTransactionProfile::GetNewDestination(GetNewDestination {
//                     s_id: 3,
//                     sf_type: 3,
//                     start_time: 16,
//                     end_time: 23
//                 })
//             ),
//             gen.get_params(0.4)
//         );
//         assert_eq!(
//             (
//                 TatpTransaction::GetAccessData,
//                 TatpTransactionProfile::GetAccessData(GetAccessData {
//                     s_id: 2,
//                     ai_type: 3
//                 })
//             ),
//             gen.get_params(0.7)
//         );
//         assert_eq!(
//             (
//                 TatpTransaction::UpdateSubscriberData,
//                 TatpTransactionProfile::UpdateSubscriberData(UpdateSubscriberData {
//                     s_id: 3,
//                     sf_type: 2,
//                     bit_1: 0,
//                     data_a: 241
//                 })
//             ),
//             gen.get_params(0.81)
//         );
//         assert_eq!(
//             (
//                 TatpTransaction::UpdateLocationData,
//                 TatpTransactionProfile::UpdateLocationData(UpdateLocationData {
//                     s_id: 9,
//                     vlr_location: 13
//                 })
//             ),
//             gen.get_params(0.93)
//         );
//         assert_eq!(
//             (
//                 TatpTransaction::InsertCallForwarding,
//                 TatpTransactionProfile::InsertCallForwarding(InsertCallForwarding {
//                     s_id: 2,
//                     sf_type: 1,
//                     start_time: 16,
//                     end_time: 20,
//                     number_x: "333269051490038".to_string()
//                 })
//             ),
//             gen.get_params(0.97)
//         );
//         assert_eq!(
//             (
//                 TatpTransaction::DeleteCallForwarding,
//                 TatpTransactionProfile::DeleteCallForwarding(DeleteCallForwarding {
//                     s_id: 3,
//                     sf_type: 3,
//                     start_time: 16
//                 })
//             ),
//             gen.get_params(0.99)
//         );
//     }
// }