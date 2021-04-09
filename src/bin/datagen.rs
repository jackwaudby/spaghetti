use spaghetti::datagen::smallbank;
use spaghetti::datagen::tatp::{self, TATP_SF_MAP};
use spaghetti::Result;

use config::Config;
use env_logger::Env;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn run() -> Result<()> {
    let start = Instant::now(); // start timing script
    let file = "Generator.toml";
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file))?; // load config

    let workload = settings.get_str("workload")?; // workload
    let sf = settings.get_int("scale_factor")? as u64; // scale factor

    log::info!(
        "Generating scale factor {} for {} workload",
        sf,
        workload.to_uppercase()
    );

    let set_seed = settings.get_bool("set_seed")?;
    let seed = settings.get_int("seed")? as u64;
    let create_params = settings.get_bool("params").unwrap();
    log::info!("Seed set: {}", set_seed);
    log::info!("Generater parameters: {}", create_params);

    match workload.as_str() {
        "tatp" => {
            let dir = format!("./data/tatp/sf-{}", sf); // dir

            if Path::new(&dir).exists() {
                fs::remove_dir_all(&dir)?; // remove directory
            }

            fs::create_dir(&dir).unwrap(); // create directory
            let s = *TATP_SF_MAP.get(&sf).unwrap(); // get subscribers

            let sub = thread::spawn(move || {
                log::info!("Generating subscribers.csv");
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                tatp::subscribers(s, &mut rng, sf).unwrap();
                log::info!("Generated subscribers.csv");
            });

            let ai = thread::spawn(move || {
                log::info!("Generating access_info.csv");
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                tatp::access_info(s, &mut rng, sf).unwrap();
                log::info!("Generated access_info.csv");
            });

            let sfcf = thread::spawn(move || {
                log::info!("Generating special_facility.csv and call_forwarding.csv");
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                tatp::special_facility_call_forwarding(s, &mut rng, sf).unwrap();
                log::info!("Generated special_facility.csv and call_forwarding.csv");
            });

            sub.join().unwrap();
            ai.join().unwrap();
            sfcf.join().unwrap();

            // generate parameters
            if create_params {
                let t = settings.get_int("transactions")? as u64;
                let use_nurand = settings.get_bool("use_nurand")?; // use non-uniform sid distribution
                log::info!("Generating {} parameters", t);
                log::info!("Use nurand {}", use_nurand);
                tatp::params(10, t, use_nurand).unwrap();
                log::info!("Generated {} parameters", t);
            }
        }
        "smallbank" => {
            let mut rng: StdRng;
            if set_seed {
                rng = SeedableRng::seed_from_u64(seed);
            } else {
                rng = SeedableRng::from_entropy();
            }
            // Remove directory.
            if Path::new("./data/smallbank").exists() {
                fs::remove_dir_all("./data/smallbank").unwrap();
            }
            // Create directory
            fs::create_dir("./data/smallbank").unwrap();

            // Data
            let accounts = settings.get_int("accounts").unwrap() as u64;
            let min = settings.get_int("min_balance").unwrap();
            let max = settings.get_int("max_balance").unwrap();

            smallbank::accounts(accounts).unwrap();
            smallbank::savings(accounts, min, max, &mut rng).unwrap();
            smallbank::checking(accounts, min, max, &mut rng).unwrap();

            // Params
            let config = Arc::new(settings);
            smallbank::params(config).unwrap();
        }
        _ => panic!("workload not recognised"),
    }

    let duration = start.elapsed();

    log::info!("Time taken to generate data is: {:?}", duration);
    Ok(())
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    if let Err(err) = run() {
        log::error!("{}", err);
    }
}
