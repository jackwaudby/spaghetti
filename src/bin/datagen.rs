use spaghetti::datagen::smallbank;
use spaghetti::datagen::tatp::{self, TATP_SF_MAP};
use spaghetti::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() -> Result<()> {
    let start = Instant::now();

    // Initialise configuration.
    let file = "Generator.toml";
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file))?;

    // Get workload.
    let workload = settings.get_str("workload")?;

    // Initialise rng.
    let set_seed = settings.get_bool("set_seed").unwrap();
    let seed = settings.get_int("seed").unwrap() as u64;
    let mut rng: StdRng;
    if set_seed {
        rng = SeedableRng::seed_from_u64(seed);
    } else {
        rng = SeedableRng::from_entropy();
    }

    // Generate
    match workload.as_str() {
        "tatp" => {
            let sf = settings.get_int("scale_factor")? as u64; // scale factor
            let dir = format!("./data/tatp/sf-{}", sf); // dir

            if Path::new(&dir).exists() {
                fs::remove_dir_all(&dir)?; // remove directory
            }

            fs::create_dir(&dir)?; // create directory
            let s = *TATP_SF_MAP.get(&sf).unwrap(); // get subscribers

            // generate subscribers
            let sub = thread::spawn(move || {
                // Initialise rng.
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                tatp::subscribers(s, &mut rng, sf).unwrap();
            });

            // generate access info
            let ai = thread::spawn(move || {
                // Initialise rng.
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                tatp::access_info(s, &mut rng, sf).unwrap();
            });

            // generate special facility and call forwarding
            let sfcf = thread::spawn(move || {
                // Initialise rng.
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                tatp::special_facility_call_forwarding(s, &mut rng, sf).unwrap();
            });

            sub.join().unwrap();
            ai.join().unwrap();
            sfcf.join().unwrap();

            let create_params = settings.get_bool("params")?;
            if create_params {
                let t = settings.get_int("transactions")? as u64; // parameters
                let use_nurand = settings.get_bool("use_nurand")?; // use non-uniform sid distribution
                tatp::params(10, t, use_nurand)?; //  generate parameters
            }
        }
        "smallbank" => {
            // Remove directory.
            if Path::new("./data/smallbank").exists() {
                fs::remove_dir_all("./data/smallbank")?;
            }
            // Create directory
            fs::create_dir("./data/smallbank")?;

            // Data
            let accounts = settings.get_int("accounts")? as u64;
            let min = settings.get_int("min_balance")?;
            let max = settings.get_int("max_balance")?;

            smallbank::accounts(accounts)?;
            smallbank::savings(accounts, min, max, &mut rng)?;
            smallbank::checking(accounts, min, max, &mut rng)?;

            // Params
            let config = Arc::new(settings);
            smallbank::params(config)?;
        }
        _ => panic!("workload not recognised"),
    }

    let duration = start.elapsed();

    println!("Time taken to generate data is: {:?}", duration);

    Ok(())
}
