use config::Config;
use csv::Writer;
use rand::prelude::IteratorRandom;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use spaghetti::workloads::tatp::helper;
use spaghetti::workloads::tatp::records::{
    AccessInfo, CallForwarding, SpecialFacility, Subscriber,
};
use spaghetti::Result;
use std::fs;
use std::path::Path;
use std::time::Instant;

fn main() -> Result<()> {
    let start = Instant::now();

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

    // Remove directory.
    if Path::new("./data").exists() {
        fs::remove_dir_all("./data")?;
    }
    // Create directory
    fs::create_dir("./data")?;

    // Generate
    match workload.as_str() {
        "tatp" => {
            let s = settings.get_int("subscribers")? as u64;
            subscribers(s, &mut rng)?;
            access_info(s, &mut rng)?;
            special_facility_call_forwarding(s, &mut rng)?;
        }
        _ => panic!("workload not recognised"),
    }

    let duration = start.elapsed();

    println!("Time taken to generate data is: {:?}", duration);

    Ok(())
}

fn subscribers(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("data/subscribers.csv")?;

    for s_id in 1..=subscribers {
        wtr.serialize(Subscriber::new(s_id, rng))?;
    }

    wtr.flush()?;
    Ok(())
}

fn access_info(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("data/access_info.csv")?;

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

fn special_facility_call_forwarding(subscribers: u64, rng: &mut StdRng) -> Result<()> {
    // Writer to files
    let mut cfr = Writer::from_path("data/call_forwarding.csv")?;
    let mut sfr = Writer::from_path("data/special_facility.csv")?;
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
