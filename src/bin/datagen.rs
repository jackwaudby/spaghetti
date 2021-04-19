use spaghetti::datagen::acid;
use spaghetti::datagen::smallbank;
use spaghetti::datagen::tatp;
use spaghetti::workloads::acid::ACID_SF_MAP;
use spaghetti::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE, SB_SF_MAP};
use spaghetti::workloads::tatp::TATP_SF_MAP;

use spaghetti::Result;

use config::Config;
use env_logger::Env;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fs;
use std::path::Path;
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

            fs::create_dir_all(&dir)?; // create directory
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

            if create_params {
                let transactions = settings.get_int("transactions")? as u64;
                let use_nurand = settings.get_bool("use_nurand")?; // use non-uniform sid distribution
                log::info!("Generating {} parameters", transactions);
                let wrapped_seed;
                if set_seed {
                    wrapped_seed = Some(seed);
                } else {
                    wrapped_seed = None;
                }

                tatp::params(sf, set_seed, wrapped_seed, use_nurand, transactions).unwrap();
                log::info!("Generated {} parameters", transactions);
            }
        }
        "smallbank" => {
            let dir = format!("./data/smallbank/sf-{}", sf);

            if Path::new(&dir).exists() {
                fs::remove_dir_all(&dir)?;
            }

            fs::create_dir_all(&dir)?;
            let accounts = *SB_SF_MAP.get(&sf).unwrap(); // get account

            let a = thread::spawn(move || {
                log::info!("Generating accounts.csv");
                smallbank::accounts(accounts, sf).unwrap();
                log::info!("Generated accounts.csv");
            });

            let s = thread::spawn(move || {
                log::info!("Generating savings.csv");
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                let min = MIN_BALANCE;
                let max = MAX_BALANCE;
                smallbank::savings(accounts, min, max, &mut rng, sf).unwrap();
                log::info!("Generated savings.csv");
            });

            let c = thread::spawn(move || {
                log::info!("Generating checking.csv");
                let mut rng: StdRng;
                if set_seed {
                    rng = SeedableRng::seed_from_u64(seed);
                } else {
                    rng = SeedableRng::from_entropy();
                }
                let min = MIN_BALANCE;
                let max = MAX_BALANCE;

                smallbank::checking(accounts, min, max, &mut rng, sf).unwrap();
                log::info!("Generated checking.csv");
            });

            a.join().unwrap();
            s.join().unwrap();
            c.join().unwrap();

            if create_params {
                let transactions = settings.get_int("transactions")? as u64;
                let wrapped_seed;
                if set_seed {
                    wrapped_seed = Some(seed);
                } else {
                    wrapped_seed = None;
                }
                let use_balance_mix = settings.get_bool("use_balance_mix")?;
                let hotspot_use_fixed_size = settings.get_bool("hotspot_use_fixed_size")?;
                log::info!("Generating {} parameters", transactions);
                smallbank::params(
                    sf,
                    set_seed,
                    wrapped_seed,
                    use_balance_mix,
                    hotspot_use_fixed_size,
                    transactions,
                )
                .unwrap();
                log::info!("Generated {} parameters", transactions);
            }
        }
        "acid" => {
            let dir = format!("./data/acid/sf-{}", sf);

            if Path::new(&dir).exists() {
                fs::remove_dir_all(&dir)?;
            }

            fs::create_dir_all(&dir)?;
            let persons = *ACID_SF_MAP.get(&sf).unwrap(); // get account

            let pjh = thread::spawn(move || {
                log::info!("Generating persons.csv");
                acid::persons(persons, sf).unwrap();
                log::info!("Generated persons.csv");
            });

            let pkp = thread::spawn(move || {
                log::info!("Generating person_knows_person.csv");
                acid::person_knows_person(persons, sf).unwrap();
                log::info!("Generated person_knows_person.csv");
            });

            pjh.join().unwrap();
            pkp.join().unwrap();
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
