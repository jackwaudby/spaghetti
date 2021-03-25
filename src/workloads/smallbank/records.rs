// use crate::workloads::tatp::helper;
// use rand::rngs::StdRng;
// use rand::Rng;
use serde::{Deserialize, Serialize};

/// Represent a record in the Account table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Account {
    pub name: String,
    pub customer_id: String,
}

/// Represent a record in the Savings table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Savings {
    pub customer_id: String,
    pub balance: String,
}

/// Represent a record in the Checking table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Checking {
    pub customer_id: String,
    pub balance: String,
}

// impl Subscriber {
//     /// Create new `AccessInfo` record.
//     pub fn new(s_id: u64, rng: &mut StdRng) -> Subscriber {
//         Subscriber {
//             s_id: s_id.to_string(),
//             sub_nbr: helper::to_sub_nbr(s_id),
//             bit_1: rng.gen_range(0..=1).to_string(),
//             bit_2: rng.gen_range(0..=1).to_string(),
//             bit_3: rng.gen_range(0..=1).to_string(),
//             bit_4: rng.gen_range(0..=1).to_string(),
//             bit_5: rng.gen_range(0..=1).to_string(),
//             bit_6: rng.gen_range(0..=1).to_string(),
//             bit_7: rng.gen_range(0..=1).to_string(),
//             bit_8: rng.gen_range(0..=1).to_string(),
//             bit_9: rng.gen_range(0..=1).to_string(),
//             bit_10: rng.gen_range(0..=1).to_string(),
//             hex_1: rng.gen_range(0..=15).to_string(),
//             hex_2: rng.gen_range(0..=15).to_string(),
//             hex_3: rng.gen_range(0..=15).to_string(),
//             hex_4: rng.gen_range(0..=15).to_string(),
//             hex_5: rng.gen_range(0..=15).to_string(),
//             hex_6: rng.gen_range(0..=15).to_string(),
//             hex_7: rng.gen_range(0..=15).to_string(),
//             hex_8: rng.gen_range(0..=15).to_string(),
//             hex_9: rng.gen_range(0..=15).to_string(),
//             hex_10: rng.gen_range(0..=15).to_string(),
//             byte_2_1: rng.gen_range(0..=255).to_string(),
//             byte_2_2: rng.gen_range(0..=255).to_string(),
//             byte_2_3: rng.gen_range(0..=255).to_string(),
//             byte_2_4: rng.gen_range(0..=255).to_string(),
//             byte_2_5: rng.gen_range(0..=255).to_string(),
//             byte_2_6: rng.gen_range(0..=255).to_string(),
//             byte_2_7: rng.gen_range(0..=255).to_string(),
//             byte_2_8: rng.gen_range(0..=255).to_string(),
//             byte_2_9: rng.gen_range(0..=255).to_string(),
//             byte_2_10: rng.gen_range(0..=255).to_string(),
//             msc_location: rng.gen_range(1..=2 ^ 32 - 1).to_string(),
//             vlr_location: rng.gen_range(1..=2 ^ 32 - 1).to_string(),
//         }
//     }
// }

// impl AccessInfo {
//     /// Create new `AccessInfo` record.
//     pub fn new(s_id: String, ai_type: String, rng: &mut StdRng) -> AccessInfo {
//         let data_1 = rng.gen_range(0..=255).to_string();
//         let data_2 = rng.gen_range(0..=255).to_string();
//         let data_3 = helper::get_data_x(3, rng);
//         let data_4 = helper::get_data_x(5, rng);

//         AccessInfo {
//             s_id,
//             ai_type,
//             data_1,
//             data_2,
//             data_3,
//             data_4,
//         }
//     }
// }

// impl SpecialFacility {
//     /// Create new `AccessInfo` record.
//     pub fn new(s_id: String, sf_type: String, rng: &mut StdRng) -> SpecialFacility {
//         let is_active = helper::is_active(rng).to_string();
//         let error_cntrl = rng.gen_range(0..=255).to_string();
//         let data_a = rng.gen_range(0..=255).to_string();
//         let data_b = helper::get_data_x(5, rng);
//         SpecialFacility {
//             s_id,
//             sf_type,
//             is_active,
//             error_cntrl,
//             data_a,
//             data_b,
//         }
//     }
// }

// impl CallForwarding {
//     /// Create new `AccessInfo` record.
//     pub fn new(
//         s_id: String,
//         sf_type: String,
//         start_time: String,
//         end_time: String,
//         number_x: String,
//     ) -> CallForwarding {
//         CallForwarding {
//             s_id,
//             sf_type,
//             start_time,
//             end_time,
//             number_x,
//         }
//     }
// }
