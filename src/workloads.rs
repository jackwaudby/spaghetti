use crate::common::error::FatalError;
use crate::storage::{SmallBankDatabase, Table};
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::*;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::convert::From;
use std::fmt;
use tracing::info;

pub mod smallbank;

#[derive(Debug)]
pub enum Database {
    SmallBank(SmallBankDatabase),
}

#[derive(PartialEq, Debug, Clone, Eq)]
pub enum PrimaryKey {
    SmallBank(SmallBankPrimaryKey),
}

impl Database {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let workload = config.get_str("workload")?;
        match workload.as_str() {
            "smallbank" => {
                let sf = config.get_int("scale_factor")? as u64; // scale factor
                let set_seed = config.get_bool("set_seed")?; // set seed
                let use_balance_mix = config.get_bool("use_balance_mix")?; // balance mix
                let population = *SB_SF_MAP.get(&sf).unwrap() as usize; // population size
                let mut database = SmallBankDatabase::new(population); // create database
                let mut rng: StdRng = SeedableRng::from_entropy();
                let contention = match sf {
                    0 => "NA",
                    1 => "high",
                    2 => "mid",
                    3 => "low",
                    4 => "very low",
                    5 => "very very low",
                    _ => panic!("invalid scale factor"),
                };
                info!("Generate SmallBank SF-{}", sf);
                smallbank::loader::populate_tables(population, &mut database, &mut rng)?; // generate data
                info!("Parameter generator set seed: {}", set_seed);
                info!("Balance mix: {}", use_balance_mix);
                info!("Contention: {}", contention);

                Ok(Database::SmallBank(database))
            }
            _ => return Err(Box::new(FatalError::IncorrectWorkload(workload))),
        }
    }

    pub fn get_table(&self, id: usize) -> &Table {
        match self {
            Database::SmallBank(ref db) => db.get_table(id),
        }
    }
}

impl From<&PrimaryKey> for usize {
    fn from(item: &PrimaryKey) -> Self {
        use PrimaryKey::*;
        use SmallBankPrimaryKey::*;
        match item {
            SmallBank(pk) => match pk {
                Account(id) => *id as usize,
                Savings(id) => *id as usize,
                Checking(id) => *id as usize,
            },
        }
    }
}

impl From<PrimaryKey> for usize {
    fn from(item: PrimaryKey) -> Self {
        use PrimaryKey::*;
        use SmallBankPrimaryKey::*;
        match item {
            SmallBank(pk) => match pk {
                Account(id) => id as usize,
                Savings(id) => id as usize,
                Checking(id) => id as usize,
            },
        }
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO").unwrap();
        Ok(())
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PrimaryKey::*;
        match &self {
            SmallBank(pk) => write!(f, "{:?}", pk),
        }
    }
}
