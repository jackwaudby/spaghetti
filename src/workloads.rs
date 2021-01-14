use crate::storage::catalog::Catalog;
use crate::storage::index::Index;
use crate::storage::table::Table;
use crate::Result;

use config::Config;
use rand::rngs::ThreadRng;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
// use tracing::info;

pub mod tpcc;

pub mod tatp;

// Type of workload.
#[derive(Debug)]
pub enum Workload {
    Tatp(Internal),
    Tpcc(Internal),
}

impl Workload {
    pub fn new(config: Arc<Config>) -> Result<Workload> {
        // Determine workload type.
        match config.get_str("workload").unwrap().as_str() {
            "tatp" => {
                // Create internals from schema file
                let internals = Internal::new("tatp_schema.txt", config)?;
                Ok(Workload::Tatp(internals))
            }
            "tpcc" => {
                // Create internals from schema file
                let internals = Internal::new("tpcc_short_schema.txt", config)?;
                Ok(Workload::Tpcc(internals))
            }
            _ => unimplemented!(),
        }
    }

    pub fn populate_tables(&self, rng: &mut ThreadRng) {
        use Workload::*;
        match *self {
            Tatp(ref i) => {
                tatp::populate_tables(i, rng);
            }
            Tpcc(ref i) => tpcc::populate_tables(i, rng),
        }
    }

    pub fn get_internals(&self) -> &Internal {
        use Workload::*;
        match *self {
            Tatp(ref i) => &i,
            Tpcc(ref i) => &i,
        }
    }
}

// Represents a workload's information.
#[derive(Debug)]
pub struct Internal {
    pub tables: Arc<HashMap<String, Arc<Table>>>,
    pub indexes: Arc<HashMap<String, Arc<Index>>>,
    pub config: Arc<Config>,
}

impl fmt::Display for Internal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.tables)
    }
}

impl Internal {
    /// Create empty tables and initialise indexes.
    fn new(filename: &str, config: Arc<Config>) -> Result<Internal> {
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

                    catalog.add_column((&c_name, c_type));
                }
                let table = Table::init(catalog);
                let table = Arc::new(table);
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
                    Some(_) => table.set_secondary_index(&index_name),
                    None => table.set_primary_index(&index_name),
                }

                let index = Arc::new(Index::init(&index_name));

                indexes.insert(index_name, index);
            }
        }

        Ok(Internal {
            tables: Arc::new(tables),
            indexes: Arc::new(indexes),
            config,
        })
    }
}
