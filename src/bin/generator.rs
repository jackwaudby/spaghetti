use config::Config;
use csv::Writer;
use rand::prelude::IteratorRandom;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::Serialize;
use spaghetti::workloads::tatp::helper;
use spaghetti::Result;

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
            let s = settings.get_int("subscribers")? as u64;
            subscribers(s, &mut rng)?;
            access_info(s, &mut rng)?;
        }
        _ => panic!("workload not recognised"),
    }

    Ok(())
}

/// Represent a record in the Subscriber table.
#[derive(Debug, Serialize)]
struct Subscriber {
    s_id: String,
    sub_nbr: String,
    bit_1: String,
    bit_2: String,
    bit_3: String,
    bit_4: String,
    bit_5: String,
    bit_6: String,
    bit_7: String,
    bit_8: String,
    bit_9: String,
    bit_10: String,
    hex_1: String,
    hex_2: String,
    hex_3: String,
    hex_4: String,
    hex_5: String,
    hex_6: String,
    hex_7: String,
    hex_8: String,
    hex_9: String,
    hex_10: String,
    byte_2_1: String,
    byte_2_2: String,
    byte_2_3: String,
    byte_2_4: String,
    byte_2_5: String,
    byte_2_6: String,
    byte_2_7: String,
    byte_2_8: String,
    byte_2_9: String,
    byte_2_10: String,
    msc_location: String,
    vlr_location: String,
}

/// Represent a record in the AccessInfo table.
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
    /// Create new `AccessInfo` record.
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

impl Subscriber {
    /// Create new `AccessInfo` record.
    fn new(s_id: u64, rng: &mut StdRng) -> Subscriber {
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

fn subscribers(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("subscribers.csv")?;

    for s_id in 1..=subscribers {
        wtr.serialize(Subscriber::new(s_id, rng))?;
    }

    wtr.flush()?;
    Ok(())
}

fn access_info(subscribers: u64, rng: &mut StdRng) -> Result<()> {
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
