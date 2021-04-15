use crate::common::error::{FatalError, NonFatalError};
use crate::server::storage::catalog::Catalog;
use crate::server::storage::index::Index;
use crate::server::storage::table::Table;
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::tatp::keys::TatpPrimaryKey;
use crate::workloads::tpcc::keys::TpccPrimaryKey;

use config::Config;
use rand::rngs::StdRng;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

pub mod acid;

pub mod tpcc;

pub mod tatp;

pub mod smallbank;

/// Represent the workload.
#[derive(Debug)]
pub enum Workload {
    Acid(Internal),
    Tatp(Internal),
    Tpcc(Internal),
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
    Tpcc(TpccPrimaryKey),
    SmallBank(SmallBankPrimaryKey),
}

impl Workload {
    /// Create new `Workload`.
    pub fn new(config: Arc<Config>) -> crate::Result<Workload> {
        // Determine workload type.
        let workload = config.get_str("workload")?;
        match workload.as_str() {
            "acid" => {
                // Create internals from schema file
                let internals = Internal::new("./schema/acid_schema.txt", config)?;
                Ok(Workload::Acid(internals))
            }
            "tatp" => {
                // Create internals from schema file
                let internals = Internal::new("./schema/tatp_schema.txt", config)?;
                Ok(Workload::Tatp(internals))
            }
            "tpcc" => {
                // Create internals from schema file
                let internals = Internal::new("./schema/tpcc_short_schema.txt", config)?;
                Ok(Workload::Tpcc(internals))
            }
            "smallbank" => {
                // Create internals from schema file
                let internals = Internal::new("./schema/smallbank_schema.txt", config)?;
                Ok(Workload::SmallBank(internals))
            }
            _ => Err(Box::new(FatalError::IncorrectWorkload(workload))),
        }
    }

    /// Populate indexes with data.
    pub fn populate_tables(&self, rng: &mut StdRng) -> crate::Result<()> {
        use Workload::*;
        match *self {
            Acid(ref i) => {
                let sf = self.get_internals().get_config().get_int("scale_factor")?;
                if self.get_internals().get_config().get_bool("load")? {
                    info!("Load sf-{} from files", sf);
                    acid::loader::load_person_table(i)?;
                } else {
                    info!("Generate sf-{}", sf);
                    acid::loader::populate_person_table(i, rng)?;
                }
            }
            Tatp(ref i) => {
                let sf = self.get_internals().get_config().get_int("scale_factor")?;
                if self.get_internals().get_config().get_bool("load")? {
                    info!("Load sf-{} from files", sf);
                    tatp::loader::load_sub_table(i)?;
                    tatp::loader::load_access_info_table(i)?;
                    tatp::loader::load_call_forwarding_table(i)?;
                    tatp::loader::load_special_facility_table(i)?;
                } else {
                    info!("Generate sf-{}", sf);
                    tatp::loader::populate_tables(i, rng)?;
                }
            }
            Tpcc(ref i) => {
                if self.get_internals().get_config().get_bool("load")? {
                    // TODO
                    tpcc::loader::populate_tables(i, rng)?
                } else {
                    tpcc::loader::populate_tables(i, rng)?
                }
            }
            SmallBank(ref i) => {
                let sf = self.get_internals().get_config().get_int("scale_factor")?;
                if self.get_internals().get_config().get_bool("load")? {
                    info!("Load sf-{} from files", sf);
                    smallbank::loader::load_account_table(i)?;
                    smallbank::loader::load_checking_table(i)?;
                    smallbank::loader::load_savings_table(i)?;
                } else {
                    info!("Generate sf-{}", sf);
                    smallbank::loader::populate_tables(i, rng)?
                }
            } // TODO
        }
        Ok(())
    }

    /// Get reference to internals of workload.
    pub fn get_internals(&self) -> &Internal {
        use Workload::*;
        match *self {
            Acid(ref i) => &i,
            Tatp(ref i) => &i,
            Tpcc(ref i) => &i,
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
                    let column: Vec<&str> = line.split(",").collect();
                    let c_name: String = column[2].to_lowercase();
                    let c_type: &str = column[1];

                    catalog.add_column((&c_name, c_type))?;
                }
                let table = Table::init(catalog);
                let table = Arc::new(table);
                info!("Initialise table: {}", table.get_table_name());
                tables.insert(table_name, table);
            } else if line.starts_with("INDEX") {
                let index_name: String = match line.strip_prefix("INDEX=") {
                    Some(name) => name.to_lowercase(),
                    None => panic!("invalid index assignment"),
                };

                let attributes: Vec<&str> = match lines.next() {
                    Some(a) => a.split(",").collect(),
                    None => break,
                };

                let table_name: String = attributes[0].trim().to_lowercase();

                let table: Arc<Table> = match tables.get(&table_name) {
                    Some(t) => Arc::clone(&t),
                    None => panic!("table does not exist: {:?}", &table_name),
                };

                match table.get_primary_index() {
                    Ok(_) => table.set_secondary_index(&index_name),
                    Err(_) => table.set_primary_index(&index_name),
                }

                let index = Arc::new(Index::init(&index_name));
                info!("Initialise index: {}", index);

                indexes.insert(index_name, index);
            }
        }

        Ok(Internal {
            tables: Arc::new(tables),
            indexes: Arc::new(indexes),
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

// TODO: improve display.
impl fmt::Display for Internal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.tables)
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PrimaryKey::*;
        match &self {
            Acid(pk) => write!(f, "{:?}", pk),
            Tatp(pk) => write!(f, "{:?}", pk),
            Tpcc(pk) => write!(f, "{:?}", pk),
            SmallBank(pk) => write!(f, "{:?}", pk),
        }
    }
}
