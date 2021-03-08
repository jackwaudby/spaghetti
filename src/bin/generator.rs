// fn main() {
//     let header = "s_id,sub_nbr,bit_1,bit_2,bit_3,bit_4,bit_5,bit_6,bit_7,bit_8,bit_9,bit_10,hex_1,hex_2,hex_3,hex_4,hex_5,hex_6,hex_7,hex_8,hex_9,hex_10,byte_2_1,byte_2_2,byte_2_3,byte_2_4,byte_2_5,byte_2_6,byte_2_7,byte_2_8,byte_2_9,byte_2_10,msc_location,vlr_location";
//     let file = "subscriber.csv";

//     let subs = 1;

//     for s_id in 1..=subs {
//         let mut row = Row::new(Arc::clone(&t), &protocol);
//         let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(s_id));
//         row.set_primary_key(pk);
//         row.init_value("s_id", &s_id.to_string())?;
//         row.init_value("sub_nbr", &helper::to_sub_nbr(s_id))?;
//         for i in 1..=10 {
//             row.init_value(
//                 format!("bit_{}", i).as_str(),
//                 &rng.gen_range(0..=1).to_string(),
//             )?;
//             row.init_value(
//                 format!("hex_{}", i).as_str(),
//                 &rng.gen_range(0..=15).to_string(),
//             )?;
//             row.init_value(
//                 format!("byte_2_{}", i).as_str(),
//                 &rng.gen_range(0..=255).to_string(),
//             )?;
//         }
//         row.init_value("msc_location", &rng.gen_range(1..=2 ^ 32 - 1).to_string())?;
//         row.init_value("vlr_location", &rng.gen_range(1..=2 ^ 32 - 1).to_string())?;

//         i.insert(pk, row)?;
//     }
// }

use config::Config;
use csv::Writer;
use rand::prelude::IteratorRandom;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::Serialize;
use spaghetti::workloads::tatp::helper;
use spaghetti::Result;
use std::process;

fn tatp(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("access_info.csv")?;

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

fn main() -> Result<()> {
    // Initialise configuration.
    let file = "Generator.toml";
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file))?;

    // Get workload.
    let workload = settings.get_str("workload")?;

    // Initialise rand.
    let seed = settings.get_int("seed");
    let mut rng: StdRng;
    match seed {
        Ok(seed) => {
            rng = SeedableRng::seed_from_u64(seed as u64);
        }
        Err(_) => {
            rng = SeedableRng::from_entropy();
        }
    }

    // Generate
    match workload.as_str() {
        "tatp" => {
            let subscribers = settings.get_int("subscribers")? as u64;
            if let Err(err) = tatp(subscribers, &mut rng) {
                println!("error running example: {}", err);
                process::exit(1);
            }
        }
        _ => panic!("workload not recognised"),
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct AccessInfo {
    s_id: String,
    ai_type: String,
    data_1: String,
    data_2: String,
    data_3: String,
    data_4: String,
}

impl AccessInfo {
    fn new(s_id: String, ai_type: String, rng: &mut StdRng) -> AccessInfo {
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

// Range of values for ai_type records.
// let ai_type_values = vec![1, 2, 3, 4];
// let subscribers = data.config.get_int("subscribers")? as u64;
// for s_id in 1..=subscribers {
// Generate number of records for a given s_id.
// let n_ai = rng.gen_range(1..=4);
// Randomly sample w.o. replacement from range of ai_type values.
// let sample = ai_type_values.iter().choose_multiple(rng, n_ai);
// for record in 1..=n_ai {
// row.init_value("s_id", &s_id.to_string())?;
// row.init_value("ai_type", &sample[record - 1].to_string())?;
// row.init_value("data_1", &rng.gen_range(0..=255).to_string())?;
// row.init_value("data_2", &rng.gen_range(0..=255).to_string())?;
// row.init_value("data_3", &helper::get_data_x(3, rng))?;
// row.init_value("data_4", &helper::get_data_x(5, rng))?;
// debug!("{}", row);
// i.insert(pk, row)?;
// }
// }
