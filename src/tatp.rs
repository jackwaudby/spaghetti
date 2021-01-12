use crate::storage::catalog::Catalog;
use crate::storage::index::Index;
use crate::storage::row::Row;
use crate::storage::table::Table;
use crate::Result;
use config::Config;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// TATP workload, base tables and indexes.
pub struct Tatp {
    pub tables: Arc<HashMap<String, Arc<Table>>>,
    pub indexes: Arc<HashMap<String, Arc<Index>>>,
    pub config: Arc<Config>,
}

impl Tatp {
    /// Create empty tables and initialise indexes.
    pub fn init(filename: &str, config: Arc<Config>) -> Result<Tatp> {
        let path = Path::new(filename);
        let mut file = File::open(&path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut lines = contents.lines();

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

        Ok(Tatp {
            tables: Arc::new(tables),
            indexes: Arc::new(indexes),
            config,
        })
    }

    pub fn populate_tables(&self, rng: &mut ThreadRng) {
        self.populate_subscriber_table(rng);
    }

    /// Populate the `Subscriber` table.
    ///
    /// Schema:
    /// Primary key: s_id
    fn populate_subscriber_table(&self, rng: &mut ThreadRng) {
        info!("Loading subscriber table");
        let s_name = String::from("subscriber");
        let t = self.tables.get(&s_name).unwrap();
        let i_name = t.get_primary_index().unwrap();
        let i = self.indexes.get(&i_name).unwrap();

        let subs = self.config.get_int("subscribers").unwrap() as u64;
        for s_id in 0..subs {
            let mut row = Row::new(Arc::clone(&t));
            row.set_primary_key(s_id);
            row.set_value("s_id", s_id.to_string());
            row.set_value("sub_nbr", to_sub_nbr(s_id));
            row.set_value("bit_1", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_2", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_3", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_4", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_5", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_6", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_7", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_8", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_9", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("bit_10", rng.gen_range(0, 1 + 1).to_string());
            row.set_value("hex_1", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_2", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_3", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_4", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_5", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_6", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_7", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_8", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_9", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("hex_10", rng.gen_range(0, 15 + 1).to_string());
            row.set_value("byte_2_1", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_2", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_3", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_4", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_5", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_6", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_7", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_8", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_9", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("byte_2_10", rng.gen_range(0, 255 + 1).to_string());
            row.set_value("msc_location", rng.gen_range(1, 2 ^ 32 - 1).to_string());
            row.set_value("vlr_location", rng.gen_range(1, 2 ^ 32 - 1).to_string());
            i.index_insert(s_id, row);
        }
    }
}

fn to_sub_nbr(s_id: u64) -> String {
    let mut num = s_id.to_string();
    for i in 0..15 {
        if num.len() == 15 {
            break;
        }
        num = format!("0{}", num);
    }
    num
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn to_sub_nbr_test() {
        let s_id = 40958;
        let sub_nbr = to_sub_nbr(s_id);
        info!("{:?}", sub_nbr.to_string().len());
        assert_eq!(sub_nbr.len(), 15);
    }
}
