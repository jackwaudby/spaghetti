use crate::storage::table::Table;
use crate::workloads::smallbank::{self, SmallBankDatabase, *};
use crate::workloads::tatp::{self, keys::TatpPrimaryKey, TatpDatabase, *};
use crate::workloads::ycsb::{self, YcsbDatabase, *};

use config::Config;
use rand::{rngs::StdRng, SeedableRng};
use tracing::info;

pub mod datatype;

pub mod tuple;

pub mod table;

pub mod access;

#[derive(Debug)]
pub enum Database {
    SmallBank(SmallBankDatabase),
    Ycsb(YcsbDatabase),
    Tatp(TatpDatabase),
}

#[derive(Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Clone)]
pub enum PrimaryKey {
    SmallBank,
    Ycsb,
    Tatp(TatpPrimaryKey),
}

impl Database {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let workload = config.get_str("workload")?;
        let sf = config.get_int("scale_factor")? as u64; // scale factor
        let set_seed = config.get_bool("set_seed")?; // set seed

        match workload.as_str() {
            "smallbank" => {
                let population = *SB_SF_MAP.get(&sf).unwrap() as usize; // population size
                let mut database = SmallBankDatabase::new(population); // create database
                let mut rng: StdRng = SeedableRng::from_entropy();

                info!("Generate SmallBank SF-{}", sf);
                smallbank::loader::populate_tables(population, &mut database, &mut rng)?; // generate data

                info!("Parameter generator set seed: {}", set_seed);
                info!("Balance mix: {}", config.get_bool("use_balance_mix")?); // balance mix
                let contention = match sf {
                    0 => "NA",
                    1 => "high",
                    2 => "mid",
                    3 => "low",
                    4 => "very low",
                    5 => "very very low",
                    _ => panic!("invalid scale factor"),
                };
                info!("Contention: {}", contention);

                let isolation_mix = config.get_str("isolation_mix").unwrap();
                info!("Isolation mix: {}", isolation_mix);

                Ok(Database::SmallBank(database))
            }

            "ycsb" => {
                let population = *YCSB_SF_MAP.get(&sf).unwrap() as usize;
                let mut database = YcsbDatabase::new(population);
                let mut rng: StdRng = SeedableRng::from_entropy();

                info!("Generate YCSB SF-{}", sf);
                ycsb::loader::populate_tables(population, &mut database, &mut rng)?;

                info!("Theta: {}", config.get_float("theta")?);
                info!("Update rate: {}", config.get_float("update_rate")?);
                info!(
                    "Serializable rate: {}",
                    config.get_float("serializable_rate")?
                );

                Ok(Database::Ycsb(database))
            }

            "tatp" => {
                let population = *TATP_SF_MAP.get(&sf).unwrap() as usize;
                let mut database = TatpDatabase::new(population);
                let mut rng: StdRng = SeedableRng::from_entropy();

                info!("Generate TATP SF-{}", sf);
                tatp::loader::populate_tables(population, &mut database, &mut rng)?;

                info!("Parameter generator set seed: {}", set_seed);
                info!("Nurand: {}", config.get_bool("nurand")?); // balance mix

                Ok(Database::Tatp(database))
            }

            _ => panic!("unknown workload: {}", workload),
        }
    }

    pub fn get_table(&self, id: usize) -> &Table {
        match self {
            Database::SmallBank(ref db) => db.get_table(id),
            Database::Ycsb(ref db) => db.get_table(id),
            Database::Tatp(ref db) => db.get_table(id),
        }
    }
}
