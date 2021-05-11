use crate::common::error::{FatalError, NonFatalError};
use crate::storage::catalog::Catalog;
use crate::storage::index::Index;
use crate::storage::table::Table;
// use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
// use crate::workloads::tatp::keys::TatpPrimaryKey;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

// pub mod acid;

// pub mod tatp;

pub mod smallbank;

/// Represents the data for a given workload.
#[derive(Debug)]
pub struct Workload {
    /// Hashmap of tables.
    tables: HashMap<String, Arc<Table>>,

    /// Hashmap of indexes; data is owned by the index.
    indexes: HashMap<String, Index>,

    /// Configuration.
    config: Config,
}

/// Primary keys of workloads.
#[derive(PartialEq, Debug, Clone, Eq)]
pub enum PrimaryKey {
    // Acid(AcidPrimaryKey),
    // Tatp(TatpPrimaryKey)
    SmallBank(SmallBankPrimaryKey),
}

impl Workload {
    /// Initialise workload.
    pub fn init(config: Config) -> crate::Result<Self> {
        let workload = config.get_str("workload")?;
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

        let mut tables = HashMap::new(); // initialise tables and indexes
        let mut indexes = HashMap::new();

        let mut next_table_id = 0;

        while let Some(line) = lines.next() {
            if line.starts_with("TABLE") {
                let table_name: String = match line.strip_prefix("TABLE=") {
                    Some(name) => name.to_lowercase(),
                    None => panic!("invalid table assignment"),
                };

                let mut catalog = Catalog::init(&table_name, next_table_id);
                next_table_id += 1;
                while let Some(line) = lines.next() {
                    if line.is_empty() {
                        break;
                    }
                    let column: Vec<&str> = line.split(',').collect();
                    let c_name: String = column[2].to_lowercase();
                    let c_type: &str = column[1];

                    catalog.add_column((&c_name, c_type))?;
                }
                let table = Table::init(catalog);

                tables.insert(table_name, Arc::new(table));
            } else if line.starts_with("INDEX") {
                let index_name: String = match line.strip_prefix("INDEX=") {
                    Some(name) => name.to_lowercase(),
                    None => panic!("invalid index assignment"),
                };

                let attributes: Vec<&str> = match lines.next() {
                    Some(a) => a.split(',').collect(),
                    None => break,
                };

                let index = Index::init(&index_name);

                indexes.insert(index_name, index);
            }
        }

        let sf = config.get_int("scale_factor")?; // populate index
        let set_seed = config.get_bool("set_seed")?;
        let mut rng: StdRng = SeedableRng::from_entropy();
        match workload.as_str() {
            // "acid" => {
            //     info!("Parameter generator set seed: {}", set_seed);
            //     info!("Generate ACID SF-{} ", sf);
            //     acid::loader::populate_person_table(&config, &mut tables, &mut indexes)?;
            //     acid::loader::populate_person_knows_person_table(
            //         &config,
            //         &mut tables,
            //         &mut indexes,
            //     )?;
            // }
            // "tatp" => {
            //     let use_nurand = config.get_bool("nurand")?;
            //     info!("Generate TATP SF-{}", sf);
            //     tatp::loader::populate_tables(&config, &mut tables, &mut indexes, &mut rng)?;
            //     info!("Generator set seed: {}", set_seed);
            //     info!("Generator nurand: {}", use_nurand);
            // }
            "smallbank" => {
                let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
                let contention = match sf {
                    0 => "NA",
                    1 => "high",
                    2 => "mid",
                    3 => "low",
                    _ => panic!("invalid scale factor"),
                };

                info!("Generate SmallBank SF-{}", sf);
                smallbank::loader::populate_tables(&config, &tables, &mut indexes, &mut rng)?;
                info!("Parameter generator set seed: {}", set_seed);
                info!("Balance mix: {}", use_balance_mix);
                info!("Contention: {}", contention);
            }
            _ => unimplemented!(),
        }

        Ok(Workload {
            tables,
            indexes,
            config,
        })
    }

    /// Get shared reference to table.
    pub fn get_table(&self, name: &str) -> Result<&Table, NonFatalError> {
        match self.tables.get(name) {
            Some(table) => Ok(&table),
            None => Err(NonFatalError::TableNotFound(name.to_string())),
        }
    }

    /// Get shared reference to index.
    pub fn get_index(&self, name: &str) -> Result<&Index, NonFatalError> {
        match self.indexes.get(name) {
            Some(index) => Ok(&index),
            None => Err(NonFatalError::IndexNotFound("test".to_string())),
        }
    }

    /// Get shared reference to config
    pub fn get_config(&self) -> &Config {
        &self.config
    }
}

impl std::hash::Hash for PrimaryKey {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        use PrimaryKey::*;
        use SmallBankPrimaryKey::*;
        match &self {
            // Acid(_) => hasher.write_u64(0), // TODO
            // Tatp(_) => hasher.write_u64(1),
            SmallBank(pk) => match pk {
                Account(id) => hasher.write_u64(*id),
                Savings(id) => hasher.write_u64(*id),
                Checking(id) => hasher.write_u64(*id),
            },
        }
    }
}

impl nohash_hasher::IsEnabled for PrimaryKey {}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for index in self.indexes.values() {
            write!(f, "{}", index).unwrap();
        }
        Ok(())
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PrimaryKey::*;
        match &self {
            // Acid(pk) => write!(f, "{:?}", pk),
            // Tatp(pk) => write!(f, "{:?}", pk),
            SmallBank(pk) => write!(f, "{:?}", pk),
        }
    }
}
