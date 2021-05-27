use crate::common::error::FatalError;
use crate::storage::catalog::Catalog;
use crate::storage::{Database, Table};
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::*;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::convert::From;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use tracing::info;

pub mod smallbank;

#[derive(Debug)]
pub struct Workload {
    database: Database,
    config: Config,
}

#[derive(PartialEq, Debug, Clone, Eq)]
pub enum PrimaryKey {
    SmallBank(SmallBankPrimaryKey),
}

impl Workload {
    pub fn new(config: Config) -> crate::Result<Self> {
        let workload = config.get_str("workload")?; // determine workload
        let filename = match workload.as_str() {
            "acid" => "./schema/acid_schema.txt",
            "tatp" => "./schema/tatp_schema.txt",
            "smallbank" => "./schema/smallbank_schema.txt",
            _ => return Err(Box::new(FatalError::IncorrectWorkload(workload))),
        };

        let path = Path::new(filename); // load schema file
        let mut file = File::open(&path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut lines = contents.lines();

        let sf = config.get_int("scale_factor")? as u64; // TODO: determine scale factor
        let population = *SB_SF_MAP.get(&sf).unwrap() as usize;

        let mut database = Database::new(3); // TODO: determine table count

        while let Some(line) = lines.next() {
            if line.starts_with("TABLE") {
                let table_name: String = match line.strip_prefix("TABLE=") {
                    Some(name) => name.to_lowercase(),
                    None => panic!("invalid table assignment"),
                };

                let mut catalog = Catalog::new(&table_name); // create schema

                while let Some(line) = lines.next() {
                    if line.is_empty() {
                        break;
                    }
                    let column: Vec<&str> = line.split(',').collect();
                    let c_name: String = column[2].to_lowercase();
                    let c_type: &str = column[1];

                    catalog.add_column((&c_name, c_type))?;
                }

                let table = Table::new(population, catalog); // create table
                database.add(table); // add to database
            }
        }

        let set_seed = config.get_bool("set_seed")?;
        let mut rng: StdRng = SeedableRng::from_entropy();

        match workload.as_str() {
            "smallbank" => {
                let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
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
                smallbank::loader::populate_tables(&config, &mut database, &mut rng)?; // generate data
                info!("Parameter generator set seed: {}", set_seed);
                info!("Balance mix: {}", use_balance_mix);
                info!("Contention: {}", contention);
            }
            _ => unimplemented!(),
        }

        Ok(Workload { database, config })
    }

    pub fn get_db(&self) -> &Database {
        &self.database
    }

    pub fn get_config(&self) -> &Config {
        &self.config
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

impl fmt::Display for Workload {
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
