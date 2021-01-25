//! `Table`s do not own `Row`s in Spaghetti, `Index`es do.
//! Indexes can not create new row, thus tables track the next row id in order to create a new row wrt to the schema defined in Catalog.
//! Ownership is then passed to the index.
//! Tables do own Catalog and contain reference to indexes

use std::fmt;
use std::sync::Mutex;

use crate::server::storage::catalog::Catalog;

#[derive(Debug)]
pub struct Table {
    schema: Catalog,
    next_row_id: Mutex<u64>,
    primary_index: Mutex<Option<String>>,
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
    pub fn get_primary_index(&self) -> Option<String> {
        let lock = self.primary_index.lock().unwrap();
        match &*lock {
            Some(pi) => Some(pi.clone()),
            None => None,
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
            Some(p) => p,
            None => "null".to_string(),
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

    use crate::storage::row::Row;
    use std::sync::Arc;

    #[test]
    fn tables() {
        // create schema
        let table_name = String::from("products");
        let mut catalog = Catalog::init(&table_name, 1);
        catalog.add_column(("id", "int"));
        catalog.add_column(("price", "double"));
        catalog.add_column(("desc", "string"));

        // create table
        let table = Table::init(catalog);

        assert_eq!(table.get_table_name(), table_name);
        assert_eq!(table.get_table_id(), 1);

        // Generate new rows
        let t = Arc::new(table);

        let r1 = Row::new(Arc::clone(&t));
        let r2 = Row::new(Arc::clone(&t));

        assert_eq!(
            format!("{}", r1),
            String::from("[0, 0, products, null, null, null]")
        );

        assert_eq!(
            format!("{}", r2),
            String::from("[1, 0, products, null, null, null]")
        );
    }
}
