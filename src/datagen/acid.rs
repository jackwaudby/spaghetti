//use crate::common::message::{Message, Parameters};
//use crate::common::parameter_generation::Generator;
//use crate::workloads::smallbank::paramgen::SmallBankGenerator;
use crate::Result;

use csv::Writer;
// use rand::rngs::StdRng;
// use rand::Rng;
use serde::{Deserialize, Serialize};

// TODO
// /// Generate parameters for SmallBank stored procedures.
// pub fn params(
//     sf: u64,
//     set_seed: bool,
//     seed: Option<u64>,

//     use_balance_mix: bool,
//     hotspot_use_fixed_size: bool,
//     transactions: u64,
// ) -> Result<()> {
//     let mut wtr = Writer::from_path(format!("./data/smallbank/sf-{}/params.csv", sf))?;
//     let mut gen =
//         SmallBankGenerator::new(sf, set_seed, seed, use_balance_mix, hotspot_use_fixed_size);

//     for _ in 1..=transactions {
//         let message = gen.generate();
//         if let Message::Request { parameters, .. } = message {
//             if let Parameters::SmallBank(params) = parameters {
//                 let s = format!("{}", params);
//                 wtr.write_record(&[s])?;
//             }
//         }
//     }

//     wtr.flush()?;
//     Ok(())
// }

/// Generate `Person` records.
pub fn persons(persons: u64, sf: u64) -> Result<()> {
    let mut wtr = Writer::from_path(format!("./data/acid/sf-{}/persons.csv", sf))?;

    for p_id in 0..persons {
        wtr.serialize(Person::new(p_id, 1, 0))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Represents a record in the Person table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Person {
    pub p_id: u64,
    pub version: u64,
    pub num_friends: u64,
}

impl Person {
    /// Create new `Person` record.
    pub fn new(p_id: u64, version: u64, num_friends: u64) -> Self {
        Person {
            p_id,
            version,
            num_friends,
        }
    }
}
