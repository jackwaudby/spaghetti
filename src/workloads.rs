use crate::common::error::{FatalError, NonFatalError};
use crate::storage::catalog::Catalog;
use crate::storage::index::Index;
use crate::storage::table::Table;
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::tatp::keys::TatpPrimaryKey;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

pub mod acid;

pub mod tatp;

pub mod smallbank;

/// Represent the workload.
#[derive(Debug)]
pub enum Workload {
    Acid(Internal),
    Tatp(Internal),
    SmallBank(Internal),
}

/// Represents the data for a given workload.
#[derive(Debug)]
pub struct Internal {
    /// Hashmap of tables.
    tables: Arc<HashMap<String, Arc<Table>>>,

    /// Hashmap of indexes; data is owned by the index.
    indexes: Arc<HashMap<String, Arc<Index>>>,

    /// Reference to configuration.
    config: Arc<Config>,
}

/// Primary keys of workloads.
#[derive(PartialEq, Debug, Clone, Eq, Hash)]
pub enum PrimaryKey {
    Acid(AcidPrimaryKey),
    Tatp(TatpPrimaryKey),
    SmallBank(SmallBankPrimaryKey),
}

impl Workload {
    /// Create new `Workload`.
    pub fn new(config: Arc<Config>) -> crate::Result<Workload> {
        let workload = config.get_str("workload")?;
        match workload.as_str() {
            "acid" => {
                let internals = Internal::new("./schema/acid_schema.txt", config)?;
                Ok(Workload::Acid(internals))
            }
            "tatp" => {
                let internals = Internal::new("./schema/tatp_schema.txt", config)?;
                Ok(Workload::Tatp(internals))
            }
            "smallbank" => {
                let internals = Internal::new("./schema/smallbank_schema.txt", config)?;
                Ok(Workload::SmallBank(internals))
            }
            _ => Err(Box::new(FatalError::IncorrectWorkload(workload))),
        }
    }

    // /// Populate indexes with data.
    // pub fn populate_tables(&self, rng: &mut StdRng) -> crate::Result<()> {
    //     use Workload::*;
    //     match *self {
    //         Acid(ref i) => {
    //             let config = self.get_internals().get_config();
    //             let sf = config.get_int("scale_factor")?;
    //             let set_seed = config.get_bool("set_seed")?;
    //             info!("Parameter generator set seed: {}", set_seed);
    //             info!("Generate ACID SF-{} ", sf);
    //             acid::loader::populate_person_table(i)?;
    //             acid::loader::populate_person_knows_person_table(i, rng)?;
    //         }
    //         Tatp(ref i) => {
    //             let config = self.get_internals().get_config();
    //             let sf = config.get_int("scale_factor")?;
    //             let set_seed = config.get_bool("set_seed")?;
    //             let use_nurand = config.get_bool("nurand")?;
    //             info!("Generate TATP SF-{}", sf);
    //             tatp::loader::populate_tables(i, rng)?;
    //             info!("Generator set seed: {}", set_seed);
    //             info!("Generator nurand: {}", use_nurand);
    //         }
    //         SmallBank(ref i) => {
    //             let config = self.get_internals().get_config();
    //             let sf = config.get_int("scale_factor")?;
    //             let set_seed = config.get_bool("set_seed")?;
    //             let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
    //             let contention = match sf {
    //                 0 => "NA",
    //                 1 => "high",
    //                 2 => "mid",
    //                 3 => "low",
    //                 _ => panic!("invalid scale factor"),
    //             };

    //             info!("Generate SmallBank SF-{}", sf);
    //             smallbank::loader::populate_tables(i, rng)?;
    //             info!("Parameter generator set seed: {}", set_seed);
    //             info!("Balance mix: {}", use_balance_mix);
    //             info!("Contention: {}", contention);
    //         }
    //     }
    //     Ok(())
    // }

    /// Get reference to internals of workload.
    pub fn get_internals(&self) -> &Internal {
        use Workload::*;
        match *self {
            Acid(ref i) => &i,
            Tatp(ref i) => &i,
            SmallBank(ref i) => &i,
        }
    }
}

impl Internal {
    /// Create tables and initialise empty indexes.
    pub fn new(filename: &str, config: Arc<Config>) -> crate::Result<Internal> {
        // Load schema file.
        let path = Path::new(filename);
        let mut file = File::open(&path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut lines = contents.lines();

        // Initialise tables and indexes.
        let mut tables = HashMap::new(); // String: table_name, table: Table
        let mut indexes = HashMap::new(); // String: table_name, index: Index

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
                //      let table = Arc::new(table);
                debug!("Initialise table: {}", table.get_table_name());
                tables.insert(table_name, table);
            } else if line.starts_with("INDEX") {
                let index_name: String = match line.strip_prefix("INDEX=") {
                    Some(name) => name.to_lowercase(),
                    None => panic!("invalid index assignment"),
                };

                let attributes: Vec<&str> = match lines.next() {
                    Some(a) => a.split(',').collect(),
                    None => break,
                };

                let table_name: String = attributes[0].trim().to_lowercase();

                match tables.get_mut(&table_name) {
                    Some(table) => table.set_primary_index(&index_name),
                    None => panic!("table does not exist: {:?}", &table_name),
                }

                let index = Index::init(&index_name);
                debug!("Initialise index: {}", index);

                indexes.insert(index_name, index);
            }
        }

        let mut atomic_tables = HashMap::new();
        for (key, val) in tables.drain() {
            atomic_tables.insert(key, Arc::new(val));
        }

        let workload = config.get_str("workload")?;
        let sf = config.get_int("scale_factor")?;
        let set_seed = config.get_bool("set_seed")?;
        let mut rng: StdRng = SeedableRng::from_entropy();
        match workload.as_str() {
            "acid" => {
                info!("Parameter generator set seed: {}", set_seed);
                info!("Generate ACID SF-{} ", sf);
                acid::loader::populate_person_table(
                    Arc::clone(&config),
                    &mut atomic_tables,
                    &mut indexes,
                )?;
                acid::loader::populate_person_knows_person_table(
                    Arc::clone(&config),
                    &mut atomic_tables,
                    &mut indexes,
                )?;
            }
            "tatp" => {
                let use_nurand = config.get_bool("nurand")?;
                info!("Generate TATP SF-{}", sf);
                tatp::loader::populate_tables(
                    Arc::clone(&config),
                    &mut atomic_tables,
                    &mut indexes,
                    &mut rng,
                )?;
                info!("Generator set seed: {}", set_seed);
                info!("Generator nurand: {}", use_nurand);
            }
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
                smallbank::loader::populate_tables(
                    Arc::clone(&config),
                    &mut atomic_tables,
                    &mut indexes,
                    &mut rng,
                )?;
                info!("Parameter generator set seed: {}", set_seed);
                info!("Balance mix: {}", use_balance_mix);
                info!("Contention: {}", contention);
            }
            _ => unimplemented!(),
        }

        let mut atomic_indexes = HashMap::new();
        for (key, val) in indexes.drain() {
            atomic_indexes.insert(key, Arc::new(val));
        }

        Ok(Internal {
            tables: Arc::new(atomic_tables),
            indexes: Arc::new(atomic_indexes),
            config,
        })
    }

    /// Get atomic shared reference to `Table`.
    pub fn get_table(&self, name: &str) -> Result<Arc<Table>, NonFatalError> {
        match self.tables.get(name) {
            Some(table) => Ok(Arc::clone(table)),
            None => Err(NonFatalError::TableNotFound(name.to_string())),
        }
    }

    /// Get atomic shared reference to `Index`.
    pub fn get_index(&self, name: &str) -> Result<Arc<Index>, NonFatalError> {
        match self.indexes.get(name) {
            Some(index) => Ok(Arc::clone(index)),
            None => Err(NonFatalError::IndexNotFound(name.to_string())),
        }
    }

    /// Get atomic shared reference to `Config`.
    pub fn get_config(&self) -> Arc<Config> {
        Arc::clone(&self.config)
    }
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Workload::Acid(ref i) => write!(f, "{}", i),
            _ => unimplemented!(),
        }
    }
}

impl fmt::Display for Internal {
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
            Acid(pk) => write!(f, "{:?}", pk),
            Tatp(pk) => write!(f, "{:?}", pk),
            SmallBank(pk) => write!(f, "{:?}", pk),
        }
    }
}
