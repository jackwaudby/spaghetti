use spaghetti::datagen::tatp;
use spaghetti::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
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
            tatp::params(10, t)?;
        }
        _ => panic!("workload not recognised"),
    }

    let duration = start.elapsed();

    println!("Time taken to generate data is: {:?}", duration);

    Ok(())
}
