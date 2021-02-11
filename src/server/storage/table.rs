use crate::common::error::SpaghettiError;
use crate::server::storage::catalog::Catalog;
use crate::Result;

use std::fmt;
use std::sync::Mutex;

#[derive(Debug)]
pub struct Table {
    /// Table schema.
    schema: Catalog,
    /// Next valid row id.
    next_row_id: Mutex<u64>,
    /// Primary index.
    primary_index: Mutex<Option<String>>,
    /// Seconday index.
    secondary_index: Mutex<Option<String>>,
}

impl Table {
    /// Create a new `Table` instance.
    pub fn init(schema: Catalog) -> Self {
        Table {
            schema,
            next_row_id: Mutex::new(0),
            primary_index: Mutex::new(None),
            secondary_index: Mutex::new(None),
        }
    }

    /// Returns the table name.
    pub fn get_table_name(&self) -> String {
        self.schema.table_name().clone()
    }

    /// Returns a shared reference to the table schema.
    pub fn schema(&self) -> &Catalog {
        &self.schema
    }

    /// Returns the table id.
    pub fn get_table_id(&self) -> u64 {
        self.schema.table_id()
    }

    /// Get number of rows in tables
    pub fn get_num_rows(&self) -> u64 {
        let row_id = self.next_row_id.lock().unwrap();
        // 0 indexed so add 1
        *row_id
    }

    /// Returns a `Table`s next valid row id.
    pub fn get_next_row_id(&self) -> u64 {
        let mut row_id = self.next_row_id.lock().unwrap();
        let next_id = *row_id;
        *row_id += 1;
        next_id
    }

    /// Set the primary index for a table
    pub fn set_primary_index(&self, index_name: &str) {
        let mut lock = self.primary_index.lock().unwrap();
        *lock = Some(String::from(index_name));
    }

    /// Get the primary index for a table
    pub fn get_primary_index(&self) -> Result<String> {
        let lock = self.primary_index.lock().unwrap();
        match &*lock {
            Some(pi) => Ok(pi.clone()),
            None => Err(Box::new(SpaghettiError::NoPrimaryIndex)),
        }
    }

    /// Set the secondary index for a table
    pub fn set_secondary_index(&self, index_name: &str) {
        let mut lock = self.secondary_index.lock().unwrap();
        *lock = Some(String::from(index_name));
    }

    /// Get the secondary index for a table
    pub fn get_secondary_index(&self) -> Option<String> {
        let lock = self.secondary_index.lock().unwrap();
        match &*lock {
            Some(pi) => Some(pi.clone()),
            None => None,
        }
    }
}

// schema: [row_id,pk,table_name,fields]
// schema: [primary,secondary]
impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pi = match self.get_primary_index() {
            Ok(p) => p,
            Err(_) => "null".to_string(),
        };

        let si = match self.get_secondary_index() {
            Some(p) => p,
            None => "null".to_string(),
        };

        write!(f, "schema: {}\nindexes: [{},{}]", self.schema, pi, si)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::server::storage::row::Row;
    use std::sync::Arc;

    #[test]
    fn tables() {
        // Create schema
        let table_name = String::from("products");
        let mut catalog = Catalog::init(&table_name, 1);
        catalog.add_column(("id", "int")).unwrap();
        catalog.add_column(("price", "double")).unwrap();
        catalog.add_column(("desc", "string")).unwrap();
        let c_catalog = catalog.clone();

        // create table
        let table = Table::init(catalog);
        assert_eq!(table.get_table_name(), table_name);
        assert_eq!(table.schema(), &c_catalog);
        assert_eq!(table.get_table_id(), 1);

        assert_eq!(table.get_num_rows(), 0);
        assert_eq!(table.get_next_row_id(), 0);
        assert_eq!(table.get_secondary_index(), None);
        assert_eq!(table.set_secondary_index("n_idx"), ());
        assert_eq!(table.get_secondary_index(), Some("n_idx".to_string()));

        assert_eq!(
            format!("{}", table),
            "schema: [products,1,(id, int),(price, double),(desc, varchar)]\nindexes: [null,n_idx]"
        );
        // Generate new rows
        let t = Arc::new(table);

        let r1 = Row::new(Arc::clone(&t), "2pl");
        let r2 = Row::new(Arc::clone(&t), "2pl");

        assert_eq!(
            format!("{}", r1),
            String::from("[1, None, false, products, null, null, null, None]")
        );

        assert_eq!(
            format!("{}", r2),
            String::from("[2, None, false, products, null, null, null, None]")
        );
    }
}
