use crate::common::error::{FatalError, NonFatalError};
use crate::storage::catalog::Catalog;
use crate::storage::table::Table;
use crate::storage::Database;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::*;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::convert::From;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

pub mod smallbank;

#[derive(Debug)]
pub struct Workload {
    tables: HashMap<String, Arc<Table>>,

    database: Database,

    config: Config,
}

#[derive(PartialEq, Debug, Clone, Eq)]
pub enum PrimaryKey {
    SmallBank(SmallBankPrimaryKey),
}

impl Workload {
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

        let mut index_cnt = 0;

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

                index_cnt += 1;
            }
        }

        let sf = config.get_int("scale_factor")? as u64;
        let accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;

        let mut database = Database::new(accounts, index_cnt, tables);

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
                    _ => panic!("invalid scale factor"),
                };

                info!("Generate SmallBank SF-{}", sf);
                smallbank::loader::populate_tables(&config, &tables, &mut database, &mut rng)?;
                info!("Parameter generator set seed: {}", set_seed);
                info!("Balance mix: {}", use_balance_mix);
                info!("Contention: {}", contention);
            }
            _ => unimplemented!(),
        }

        Ok(Workload {
            tables,
            database,
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
