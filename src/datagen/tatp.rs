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

pub fn params(transactions: u64, subscribers: u64, use_nurand: bool) -> Result<()> {
    // Init writer.
    let mut wtr = Writer::from_path("data/tatp/params.csv")?;
    // Init generator.
    let mut gen = TatpGenerator::new(subscribers, false, use_nurand);

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

pub fn subscribers(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("data/tatp/subscribers.csv")?;

    for s_id in 1..=subscribers {
        wtr.serialize(Subscriber::new(s_id, rng))?;
    }

    wtr.flush()?;
    Ok(())
}

pub fn access_info(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("data/tatp/access_info.csv")?;

    // Range of values for ai_type records.
    let ai_type_values = vec![1, 2, 3, 4];

    for s_id in 1..=subscribers {
        // Generate number of records for a given s_id.
        let n_ai = rng.gen_range(1..=4);
        // Randomly sample w.o. replacement from range of ai_type values.
        let sample = ai_type_values.iter().choose_multiple(rng, n_ai);
        for record in 1..=n_ai {
            let ai_type = sample[record - 1].to_string();
            wtr.serialize(AccessInfo::new(s_id.to_string(), ai_type, rng))?;
        }
    }

    wtr.flush()?;
    Ok(())
}

pub fn special_facility_call_forwarding(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    // Writer to files
    let mut cfr = Writer::from_path("data/tatp/call_forwarding.csv")?;
    let mut sfr = Writer::from_path("data/tatp/special_facility.csv")?;
    // Range of values for ai_type records.
    let sf_type_values = vec![1, 2, 3, 4];

    // Range of values for start_time.
    let start_time_values = vec![0, 8, 16];
    for s_id in 1..=subscribers {
        // Generate number of records for a given s_id.
        let n_sf = rng.gen_range(1..=4);
        // Randomly sample w.o. replacement from range of ai_type values.
        let sample = sf_type_values.iter().choose_multiple(rng, n_sf);
        for record in 1..=n_sf {
            // SPECIALFACILITY
            let sf_type = sample[record - 1];
            sfr.serialize(SpecialFacility::new(
                s_id.to_string(),
                sf_type.to_string(),
                rng,
            ))?;

            // For each row, insert [0,3] into call forwarding table
            // Generate the number to insert
            let n_cf = rng.gen_range(0..=3);
            // Randomly sample w.o. replacement from range of ai_type values.
            let start_times = start_time_values.iter().choose_multiple(rng, n_cf);
            if n_cf != 0 {
                for i in 1..=n_cf {
                    // s_id from above
                    // sf_type from above
                    let st = *start_times[i - 1];
                    let et = st + rng.gen_range(1..=8);
                    let nx = helper::get_number_x(rng);
                    // CALLFORWARDING
                    cfr.serialize(CallForwarding::new(
                        s_id.to_string(),
                        sf_type.to_string(),
                        st.to_string(),
                        et.to_string(),
                        nx,
                    ))?;
                }
            }
        }
    }
    cfr.flush()?;
    sfr.flush()?;
    Ok(())
}

/// Represent a record in the Subscriber table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Subscriber {
    pub s_id: String,
    pub sub_nbr: String,
    pub bit_1: String,
    pub bit_2: String,
    pub bit_3: String,
    pub bit_4: String,
    pub bit_5: String,
    pub bit_6: String,
    pub bit_7: String,
    pub bit_8: String,
    pub bit_9: String,
    pub bit_10: String,
    pub hex_1: String,
    pub hex_2: String,
    pub hex_3: String,
    pub hex_4: String,
    pub hex_5: String,
    pub hex_6: String,
    pub hex_7: String,
    pub hex_8: String,
    pub hex_9: String,
    pub hex_10: String,
    pub byte_2_1: String,
    pub byte_2_2: String,
    pub byte_2_3: String,
    pub byte_2_4: String,
    pub byte_2_5: String,
    pub byte_2_6: String,
    pub byte_2_7: String,
    pub byte_2_8: String,
    pub byte_2_9: String,
    pub byte_2_10: String,
    pub msc_location: String,
    pub vlr_location: String,
}

/// Represent a record in the AccessInfo table.
#[derive(Debug, Deserialize, Serialize)]
pub struct AccessInfo {
    pub s_id: String,
    pub ai_type: String,
    pub data_1: String,
    pub data_2: String,
    pub data_3: String,
    pub data_4: String,
}

/// Represent a record in the CallForwarding table.
#[derive(Debug, Deserialize, Serialize)]
pub struct CallForwarding {
    pub s_id: String,
    pub sf_type: String,
    pub start_time: String,
    pub end_time: String,
    pub number_x: String,
}

/// Represent a record in the SpecialFacility table.
#[derive(Debug, Deserialize, Serialize)]
pub struct SpecialFacility {
    pub s_id: String,
    pub sf_type: String,
    pub is_active: String,
    pub error_cntrl: String,
    pub data_a: String,
    pub data_b: String,
}

impl Subscriber {
    /// Create new `AccessInfo` record.
    pub fn new(s_id: u64, rng: &mut StdRng) -> Subscriber {
        Subscriber {
            s_id: s_id.to_string(),
            sub_nbr: helper::to_sub_nbr(s_id),
            bit_1: rng.gen_range(0..=1).to_string(),
            bit_2: rng.gen_range(0..=1).to_string(),
            bit_3: rng.gen_range(0..=1).to_string(),
            bit_4: rng.gen_range(0..=1).to_string(),
            bit_5: rng.gen_range(0..=1).to_string(),
            bit_6: rng.gen_range(0..=1).to_string(),
            bit_7: rng.gen_range(0..=1).to_string(),
            bit_8: rng.gen_range(0..=1).to_string(),
            bit_9: rng.gen_range(0..=1).to_string(),
            bit_10: rng.gen_range(0..=1).to_string(),
            hex_1: rng.gen_range(0..=15).to_string(),
            hex_2: rng.gen_range(0..=15).to_string(),
            hex_3: rng.gen_range(0..=15).to_string(),
            hex_4: rng.gen_range(0..=15).to_string(),
            hex_5: rng.gen_range(0..=15).to_string(),
            hex_6: rng.gen_range(0..=15).to_string(),
            hex_7: rng.gen_range(0..=15).to_string(),
            hex_8: rng.gen_range(0..=15).to_string(),
            hex_9: rng.gen_range(0..=15).to_string(),
            hex_10: rng.gen_range(0..=15).to_string(),
            byte_2_1: rng.gen_range(0..=255).to_string(),
            byte_2_2: rng.gen_range(0..=255).to_string(),
            byte_2_3: rng.gen_range(0..=255).to_string(),
            byte_2_4: rng.gen_range(0..=255).to_string(),
            byte_2_5: rng.gen_range(0..=255).to_string(),
            byte_2_6: rng.gen_range(0..=255).to_string(),
            byte_2_7: rng.gen_range(0..=255).to_string(),
            byte_2_8: rng.gen_range(0..=255).to_string(),
            byte_2_9: rng.gen_range(0..=255).to_string(),
            byte_2_10: rng.gen_range(0..=255).to_string(),
            msc_location: rng.gen_range(1..=2 ^ 32 - 1).to_string(),
            vlr_location: rng.gen_range(1..=2 ^ 32 - 1).to_string(),
        }
    }
}

impl AccessInfo {
    /// Create new `AccessInfo` record.
    pub fn new(s_id: String, ai_type: String, rng: &mut StdRng) -> AccessInfo {
        let data_1 = rng.gen_range(0..=255).to_string();
        let data_2 = rng.gen_range(0..=255).to_string();
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
    /// Create new `AccessInfo` record.
    pub fn new(s_id: String, sf_type: String, rng: &mut StdRng) -> SpecialFacility {
        let is_active = helper::is_active(rng).to_string();
        let error_cntrl = rng.gen_range(0..=255).to_string();
        let data_a = rng.gen_range(0..=255).to_string();
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
    /// Create new `AccessInfo` record.
    pub fn new(
        s_id: String,
        sf_type: String,
        start_time: String,
        end_time: String,
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
