use crate::common::message::{Message, Parameters};
use crate::common::parameter_generation::Generator;
use crate::workloads::tatp::helper;
use crate::workloads::tatp::paramgen::TatpGenerator;
use crate::Result;

use csv::Writer;
use rand::prelude::IteratorRandom;
use rand::rngs::StdRng;
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Generate parameters for TATP stored procedures.
pub fn params(
    sf: u64,
    set_seed: bool,
    seed: Option<u64>,
    use_nurand: bool,
    transactions: u64,
) -> Result<()> {
    let mut wtr = Writer::from_path(format!("data/tatp/sf-{}/params.csv", sf))?;
    let mut gen = TatpGenerator::new(sf, set_seed, seed, use_nurand);

    for _ in 1..=transactions {
        let message = gen.generate();

        if let Message::Request { parameters, .. } = message {
            if let Parameters::Tatp(params) = parameters {
                let s = format!("{}", params);
                wtr.write_record(&[s])?;
            }
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate records for subscriber table.
pub fn subscribers(subscribers: u64, rng: &mut StdRng, sf: u64) -> Result<()> {
    let mut wtr = Writer::from_path(format!("data/tatp/sf-{}/subscribers.csv", sf))?;

    for s_id in 1..=subscribers {
        wtr.serialize(Subscriber::new(s_id, rng))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Generate records for access info table.
pub fn access_info(subscribers: u64, rng: &mut StdRng, sf: u64) -> Result<()> {
    let mut wtr = Writer::from_path(format!("data/tatp/sf-{}/access_info.csv", sf))?;

    let ai_type_values = vec![1, 2, 3, 4]; // range of values for ai_type records

    for s_id in 1..=subscribers {
        let n_ai = rng.gen_range(1..=4); // generate number of records for a given s_id

        let sample = ai_type_values.iter().choose_multiple(rng, n_ai); // randomly sample w.o. replacement from range of ai_type values
        for record in 1..=n_ai {
            let ai_type = sample[record - 1];
            wtr.serialize(AccessInfo::new(s_id, *ai_type, rng))?;
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate records for special facility and call forwarding tables.
pub fn special_facility_call_forwarding(subscribers: u64, rng: &mut StdRng, sf: u64) -> Result<()> {
    let mut cfr = Writer::from_path(format!("data/tatp/sf-{}/call_forwarding.csv", sf))?;
    let mut sfr = Writer::from_path(format!("data/tatp/sf-{}/special_facility.csv", sf))?;

    let sf_type_values = vec![1, 2, 3, 4]; // range of values for ai_type records
    let start_time_values = vec![0, 8, 16]; // range of values for start_time

    for s_id in 1..=subscribers {
        let n_sf = rng.gen_range(1..=4); // generate number of records for a given s_id

        let sample = sf_type_values.iter().choose_multiple(rng, n_sf); // randomly sample w.o. replacement from range of ai_type values
        for record in 1..=n_sf {
            let sf_type = sample[record - 1];
            sfr.serialize(SpecialFacility::new(s_id, *sf_type, rng))?;

            // For each row, insert [0,3] into call forwarding table
            let n_cf = rng.gen_range(0..=3); // generate the number to insert
            let start_times = start_time_values.iter().choose_multiple(rng, n_cf); // randomly sample w.o. replacement from range
            if n_cf != 0 {
                for i in 1..=n_cf {
                    let st = *start_times[i - 1];
                    let et = st + rng.gen_range(1..=8);
                    let nx = helper::get_number_x(rng);

                    cfr.serialize(CallForwarding::new(s_id, *sf_type, st, et, nx))?;
                }
            }
        }
    }
    cfr.flush()?;
    sfr.flush()?;
    Ok(())
}

/// Represent a record in the subscriber table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Subscriber {
    pub s_id: u64,
    pub sub_nbr: String,
    pub bit_1: u64,
    pub bit_2: u64,
    pub bit_3: u64,
    pub bit_4: u64,
    pub bit_5: u64,
    pub bit_6: u64,
    pub bit_7: u64,
    pub bit_8: u64,
    pub bit_9: u64,
    pub bit_10: u64,
    pub hex_1: u64,
    pub hex_2: u64,
    pub hex_3: u64,
    pub hex_4: u64,
    pub hex_5: u64,
    pub hex_6: u64,
    pub hex_7: u64,
    pub hex_8: u64,
    pub hex_9: u64,
    pub hex_10: u64,
    pub byte_2_1: u64,
    pub byte_2_2: u64,
    pub byte_2_3: u64,
    pub byte_2_4: u64,
    pub byte_2_5: u64,
    pub byte_2_6: u64,
    pub byte_2_7: u64,
    pub byte_2_8: u64,
    pub byte_2_9: u64,
    pub byte_2_10: u64,
    pub msc_location: u64,
    pub vlr_location: u64,
}

/// Represent a record in the access info table.
#[derive(Debug, Deserialize, Serialize)]
pub struct AccessInfo {
    pub s_id: u64,
    pub ai_type: u64,
    pub data_1: u64,
    pub data_2: u64,
    pub data_3: String,
    pub data_4: String,
}

/// Represent a record in the call forwarding table.
#[derive(Debug, Deserialize, Serialize)]
pub struct CallForwarding {
    pub s_id: u64,
    pub sf_type: u64,
    pub start_time: u64,
    pub end_time: u64,
    pub number_x: String,
}

/// Represent a record in the special facility table.
#[derive(Debug, Deserialize, Serialize)]
pub struct SpecialFacility {
    pub s_id: u64,
    pub sf_type: u64,
    pub is_active: u64,
    pub error_cntrl: u64,
    pub data_a: u64,
    pub data_b: String,
}

impl Subscriber {
    /// Create new subscriber record.
    pub fn new(s_id: u64, rng: &mut StdRng) -> Subscriber {
        Subscriber {
            s_id: s_id,
            sub_nbr: helper::to_sub_nbr(s_id),
            bit_1: rng.gen_range(0..=1),
            bit_2: rng.gen_range(0..=1),
            bit_3: rng.gen_range(0..=1),
            bit_4: rng.gen_range(0..=1),
            bit_5: rng.gen_range(0..=1),
            bit_6: rng.gen_range(0..=1),
            bit_7: rng.gen_range(0..=1),
            bit_8: rng.gen_range(0..=1),
            bit_9: rng.gen_range(0..=1),
            bit_10: rng.gen_range(0..=1),
            hex_1: rng.gen_range(0..=15),
            hex_2: rng.gen_range(0..=15),
            hex_3: rng.gen_range(0..=15),
            hex_4: rng.gen_range(0..=15),
            hex_5: rng.gen_range(0..=15),
            hex_6: rng.gen_range(0..=15),
            hex_7: rng.gen_range(0..=15),
            hex_8: rng.gen_range(0..=15),
            hex_9: rng.gen_range(0..=15),
            hex_10: rng.gen_range(0..=15),
            byte_2_1: rng.gen_range(0..=255),
            byte_2_2: rng.gen_range(0..=255),
            byte_2_3: rng.gen_range(0..=255),
            byte_2_4: rng.gen_range(0..=255),
            byte_2_5: rng.gen_range(0..=255),
            byte_2_6: rng.gen_range(0..=255),
            byte_2_7: rng.gen_range(0..=255),
            byte_2_8: rng.gen_range(0..=255),
            byte_2_9: rng.gen_range(0..=255),
            byte_2_10: rng.gen_range(0..=255),
            msc_location: rng.gen_range(1..(2 ^ 32)),
            vlr_location: rng.gen_range(1..(2 ^ 32)),
        }
    }
}

impl AccessInfo {
    /// Create new access info record.
    pub fn new(s_id: u64, ai_type: u64, rng: &mut StdRng) -> AccessInfo {
        let data_1 = rng.gen_range(0..=255);
        let data_2 = rng.gen_range(0..=255);
        let data_3 = helper::get_data_x(3, rng);
        let data_4 = helper::get_data_x(5, rng);

        AccessInfo {
            s_id,
            ai_type,
            data_1,
            data_2,
            data_3,
            data_4,
        }
    }
}

impl SpecialFacility {
    /// Create new special facility record.
    pub fn new(s_id: u64, sf_type: u64, rng: &mut StdRng) -> SpecialFacility {
        let is_active = helper::is_active(rng);
        let error_cntrl = rng.gen_range(0..=255);
        let data_a = rng.gen_range(0..=255);
        let data_b = helper::get_data_x(5, rng);

        SpecialFacility {
            s_id,
            sf_type,
            is_active,
            error_cntrl,
            data_a,
            data_b,
        }
    }
}

impl CallForwarding {
    /// Create new call forwarding record.
    pub fn new(
        s_id: u64,
        sf_type: u64,
        start_time: u64,
        end_time: u64,
        number_x: String,
    ) -> CallForwarding {
        CallForwarding {
            s_id,
            sf_type,
            start_time,
            end_time,
            number_x,
        }
    }
}
