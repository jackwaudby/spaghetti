use crate::storage::table::Table;
use crate::workloads::smallbank::{self, SmallBankDatabase, *};

use config::Config;
use rand::{rngs::StdRng, SeedableRng};
use tracing::info;

pub mod datatype;

pub mod tuple;

pub mod table;

pub mod access;

#[derive(Debug)]
pub struct Database(SmallBankDatabase);

#[derive(Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Clone)]
pub enum PrimaryKey {
    SmallBank,
    Acid,
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

                Ok(Database(database))
            }

            _ => panic!("unknown workload: {}", workload),
        }
    }

    pub fn get_table(&self, id: usize) -> &Table {
        self.0.get_table(id)
    }
}
