use crate::common::error::FatalError;
use crate::storage::table::Table;
use crate::workloads::acid::*;
use crate::workloads::acid::{self, AcidDatabase};
use crate::workloads::smallbank::*;
use crate::workloads::smallbank::{self, SmallBankDatabase};

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fmt;
use tracing::info;

pub mod datatype;

pub mod tuple;

pub mod table;

pub mod access;

#[derive(Debug)]
pub enum Database {
    SmallBank(SmallBankDatabase),
    Tatp,
    Acid(AcidDatabase),
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

                Ok(Database::SmallBank(database))
            }
            "acid" => {
                let population = *ACID_SF_MAP.get(&sf).unwrap() as usize; // population size
                let mut database = AcidDatabase::new(population); // create database

                info!("Generate ACID SF-{}", sf);
                acid::loader::populate_tables(population, &mut database)?; // generate data
                info!("Parameter generator set seed: {}", set_seed);

                Ok(Database::Acid(database))
            }
            "tatp" => unimplemented!(),
            _ => return Err(Box::new(FatalError::IncorrectWorkload(workload))),
        }
    }

    pub fn get_table(&self, id: usize) -> &Table {
        match self {
            Database::SmallBank(ref db) => db.get_table(id),
            Database::Acid(ref db) => db.get_table(id),
            Database::Tatp => unimplemented!(),
        }
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO").unwrap();
        Ok(())
    }
}
