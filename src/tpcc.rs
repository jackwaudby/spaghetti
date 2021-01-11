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

pub mod helper;

pub mod new_order;

#[derive(Debug)]
pub struct TpcC {
    pub tables: Arc<HashMap<String, Arc<Table>>>,
    pub indexes: Arc<HashMap<String, Arc<Index>>>,
    pub config: Arc<Config>,
}

impl TpcC {
    /// Returns a workload with tables and indexes initialised.
    pub fn init(filename: &str, config: Arc<Config>) -> Result<TpcC> {
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
                    None => panic!("table does not exist"),
                };

                match table.get_primary_index() {
                    Some(_) => table.set_secondary_index(&index_name),
                    None => table.set_primary_index(&index_name),
                }

                let index = Arc::new(Index::init(&index_name));

                indexes.insert(index_name, index);
            }
        }

        Ok(TpcC {
            tables: Arc::new(tables),
            indexes: Arc::new(indexes),
            config,
        })
    }

    pub fn populate_tables(&self, rng: &mut ThreadRng) {
        self.populate_warehouse_table(rng);
        // self.populate_item_table(rng);
        // self.populate_stock_table(rng);
        self.populate_district_table(rng);
        self.populate_customer_table(rng);
    }
}

impl TpcC {
    /// Populate the `Item` table.
    ///
    /// Schema: (int,i_id) (int,i_im_id) (string,i_name) (double,i_price) (string,i_data)
    /// Primary key: i_id
    fn populate_item_table(&self, rng: &mut ThreadRng) {
        info!("Loading item table");
        let t_name = String::from("item");
        let t = self.tables.get(&t_name).unwrap();
        let i_name = t.get_primary_index().unwrap();
        let i = self.indexes.get(&i_name).unwrap();

        let max_items = self.config.get_int("max_items").unwrap() as u64;

        for i_id in 0..max_items + 1 {
            let mut row = Row::new(Arc::clone(&t));
            row.set_primary_key(i_id);
            row.set_value("i_id", i_id.to_string());
            row.set_value("i_im_id", rng.gen_range(1, 10000 + 1).to_string());
            row.set_value("i_name", helper::random_string(14, 24, rng));
            row.set_value("i_price", helper::random_float(1.0, 100.0, 2, rng));
            row.set_value("i_data", helper::item_data(rng));
            i.index_insert(i_id, row);
        }
    }

    /// Populate the `Warehouse` table.
    ///
    /// Schema: (int,w_id) (string,w_name) (string,w_street_1) (string,w_street_2) (string,w_city) (string,w_state) (string,w_zip)
    /// (double,w_tax) (double,w_ytd)
    /// Primary key: w_id
    fn populate_warehouse_table(&self, rng: &mut ThreadRng) {
        info!("Loading warehouse table");
        let n = self.config.get_int("warehouses").unwrap() as u64;

        let t_name = String::from("warehouse");
        let t = self.tables.get(&t_name).unwrap();
        let i_name = t.get_primary_index().unwrap();
        let i = self.indexes.get(&i_name).unwrap();

        for w_id in 0..n {
            let mut row = Row::new(Arc::clone(&t));
            row.set_primary_key(w_id);
            row.set_value("w_id", w_id.to_string());
            row.set_value("w_name", helper::random_string(6, 10, rng));
            row.set_value("w_street_1", helper::random_string(10, 20, rng));
            row.set_value("w_street_2", helper::random_string(10, 20, rng));
            row.set_value("w_city", helper::random_string(10, 20, rng));
            row.set_value("w_state", helper::random_string(2, 2, rng));
            row.set_value("w_zip", helper::zip(rng));
            row.set_value("w_tax", helper::random_float(0.0, 0.2, 4, rng));
            row.set_value("w_ytd", "300000.0".to_string());
            i.index_insert(w_id, row);
        }
    }

    /// Populate the `District` table.
    ///
    /// Schema: (int,d_id) (int,d_w_id) (string,d_name) (string,d_street_1) (string,d_street_2) (string,d_city) (string,d_state)
    /// (string,d_zip) (double,d_tax) (double,d_ytd) (int,d_next_o_id)
    /// Primary key:
    fn populate_district_table(&self, rng: &mut ThreadRng) {
        info!("Loading district table");
        let t_name = String::from("district");
        let t = self.tables.get(&t_name).unwrap();
        let i_name = t.get_primary_index().unwrap();
        let i = self.indexes.get(&i_name).unwrap();
        let n = self.config.get_int("warehouses").unwrap() as u64;
        let d = self.config.get_int("districts").unwrap() as u64;

        for w_id in 0..n {
            for d_id in 0..d {
                let mut row = Row::new(Arc::clone(&t));
                row.set_primary_key(d_id);
                row.set_value("d_id", d_id.to_string());
                row.set_value("d_w_id", w_id.to_string());
                row.set_value("d_name", helper::random_string(6, 10, rng));
                row.set_value("d_street_1", helper::random_string(10, 20, rng));
                row.set_value("d_street_2", helper::random_string(10, 20, rng));
                row.set_value("d_city", helper::random_string(10, 20, rng));
                row.set_value("d_state", helper::random_string(2, 2, rng));
                row.set_value("d_zip", helper::zip(rng));
                row.set_value("d_tax", helper::random_float(0.0, 0.2, 4, rng));
                row.set_value("d_ytd", "30000.0".to_string());
                row.set_value("d_next_o_id", "3001".to_string());
                i.index_insert(helper::district_key(self.config.clone(), w_id, d_id), row);
            }
        }
    }

    /// Populate the `Stock` table.
    ///
    /// Schema: (int,s_i_id) (int,s_w_id) (int,s_quantity) (int,s_remote_cnt)
    /// Primary key: (s_i_id,s_w_id)
    fn populate_stock_table(&self, rng: &mut ThreadRng) {
        info!("loading stock table");
        let t_name = String::from("stock");
        let t = self.tables.get(&t_name).unwrap();
        let i_name = t.get_primary_index().unwrap();
        let i = self.indexes.get(&i_name).unwrap();

        let n = self.config.get_int("warehouses").unwrap() as u64;

        let mi = self.config.get_int("max_items").unwrap() as u64;

        for w_id in 0..n {
            for s_i_id in 0..mi {
                let mut row = Row::new(Arc::clone(&t));
                row.set_primary_key(s_i_id);
                row.set_value("s_i_id", s_i_id.to_string());
                row.set_value("s_w_id", w_id.to_string());
                row.set_value("s_quantity", rng.gen_range(10, 101).to_string());
                row.set_value("s_remote_cnt", "0".to_string());
                i.index_insert(helper::stock_key(self.config.clone(), w_id, s_i_id), row);
            }
        }
    }

    /// Populate the `Customer` table.
    ///
    /// Schema: (int,c_id) (int,c_d_id) (int,c_w_id) (string,c_middle) (string,c_last) (string,c_state) (string,c_credit) (int,c_discount)
    /// (double,c_balance) (double,c_ytd_payment) (int,c_payment_cnt)
    /// Primary key: c_id
    fn populate_customer_table(&self, rng: &mut ThreadRng) {
        info!("Loading customer table");
        let t_name = String::from("customer");
        let t = self.tables.get(&t_name).unwrap();
        let i_name = t.get_primary_index().unwrap();
        let i = self.indexes.get(&i_name).unwrap();

        let w = self.config.get_int("warehouses").unwrap() as u64;
        let d = self.config.get_int("districts").unwrap() as u64;
        let c = self.config.get_int("customers").unwrap() as u64;

        for w_id in 0..w {
            for d_id in 0..d {
                for c_id in 0..c {
                    let mut row = Row::new(Arc::clone(&t));
                    row.set_primary_key(c_id);
                    row.set_value("c_id", c_id.to_string());
                    row.set_value("c_d_id", d_id.to_string());
                    row.set_value("c_w_id", w_id.to_string());
                    row.set_value("c_last", helper::last_name(c_id, rng));
                    row.set_value("c_discount", helper::random_float(0.0, 0.5, 4, rng));
                    row.set_value("c_balance", "-10.0".to_string());
                    row.set_value("c_ytd_payment", "10.0".to_string());
                    row.set_value("c_payment_cnt", "1".to_string());
                    i.index_insert(
                        helper::customer_key(self.config.clone(), w_id, d_id, c_id),
                        row,
                    );
                }
            }
        }
    }

    //TODO: new order, order, history
}

impl fmt::Display for TpcC {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.tables)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::sync::Arc;

    #[test]
    fn tpcc() {
        // TABLE=WAREHOUSE
        //     int64_t,W_ID
        //     string,W_NAME
        //     string,W_STREET_1
        //     string,W_STREET_2
        //     string,W_CITY
        //     string,W_STATE
        //     string,W_ZIP
        //     double,W_TAX
        //     double,W_YTD

        // 0. Create atomic table map
        let mut tables = HashMap::new(); // String: table_name, table: Table

        // 1. Create schema and table
        let table_name = String::from("warehouse");
        let mut catalog = Catalog::init(&table_name, 1);
        catalog.add_column(("w_id", "int"));
        catalog.add_column(("w_name", "string"));
        catalog.add_column(("w_street_1", "string"));
        catalog.add_column(("w_street_2", "string"));
        catalog.add_column(("w_city", "string"));
        catalog.add_column(("w_state", "string"));
        catalog.add_column(("w_zip", "string"));
        catalog.add_column(("w_tax", "double"));
        catalog.add_column(("w_ytd", "double"));
        let w_table = Table::init(catalog);

        // 2. Create index for table
        let w_index = Index::init("warehouse_idx");
        w_table.set_primary_index("warehouse_idx");

        // 3. Allow table multiple owners
        let w_table = Arc::new(w_table);

        // 4. Put in table map
        tables.insert(table_name, Arc::clone(&w_table));

        // 5. Generate row and set fields
        let mut row = Row::new(Arc::clone(&w_table));
        row.set_value("w_id", "1".to_string());
        row.set_value("w_name", "main".to_string());
        row.set_value("w_street_1", "church lane".to_string());
        row.set_value("w_street_2", "hedon".to_string());
        row.set_value("w_city", "hull".to_string());
        row.set_value("w_state", "east yorkshire".to_string());
        row.set_value("w_zip", "hu11 8uz".to_string());
        row.set_value("w_tax", "0.2".to_string());
        row.set_value("w_ytd", "1000.0".to_string());

        let mut row2 = Row::new(Arc::clone(&w_table));
        row2.set_value("w_id", "2".to_string());
        row2.set_value("w_name", "backup".to_string());
        row2.set_value("w_street_1", "ganstead lane".to_string());
        row2.set_value("w_street_2", "ganstead".to_string());
        row2.set_value("w_city", "hull".to_string());
        row2.set_value("w_state", "east yorkshire".to_string());
        row2.set_value("w_zip", "hu11 4bg".to_string());
        row2.set_value("w_tax", "0.2".to_string());
        row2.set_value("w_ytd", "2000.0".to_string());

        // 6. Assign row to index
        w_index.index_insert(1, row);
        w_index.index_insert(2, row2);

        // 7. Change row field
        w_index
            .index_read_mut(1)
            .unwrap()
            .set_value("w_ytd", "3000.0".to_string());

        assert_eq!(
            format!("{}", *w_index.index_read(1).unwrap()),
            String::from(
                "[0, 0, warehouse, 1, main, church lane, hedon, hull, east yorkshire, hu11 8uz, 0.2, 3000]"
            )
        );

        assert_eq!(
            format!("{}", *w_index.index_read(2).unwrap()),
            String::from(
                "[1, 0, warehouse, 2, backup, ganstead lane, ganstead, hull, east yorkshire, hu11 4bg, 0.2, 2000]"
            )
        );
    }
}
