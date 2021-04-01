use spaghetti::datagen::smallbank;
use spaghetti::datagen::tatp;
use spaghetti::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fs;
use std::path::Path;
use std::sync::Arc;
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
            // Remove directory.
            if Path::new("./data/tatp").exists() {
                fs::remove_dir_all("./data/tatp")?;
            }
            // Create directory
            fs::create_dir("./data/tatp")?;
            // Data
            let s = settings.get_int("subscribers")? as u64;
            tatp::subscribers(s, &mut rng)?;
            tatp::access_info(s, &mut rng)?;
            tatp::special_facility_call_forwarding(s, &mut rng)?;

            // Params
            let t = settings.get_int("transactions")? as u64;
            let use_nurand = settings.get_bool("use_nurand")?;

            tatp::params(10, t, use_nurand)?;
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
