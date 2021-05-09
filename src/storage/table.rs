use crate::common::error::NonFatalError;
use crate::storage::catalog::Catalog;

use std::fmt;
use std::sync::Mutex;

#[derive(Debug)]
pub struct Table {
    /// Table schema.
    schema: Catalog,

    /// Next valid row id.
    next_row_id: Mutex<u64>,

    /// Primary index.
    primary_index: Option<String>,

    /// Seconday index.
    secondary_index: Mutex<Option<String>>,
}

impl Table {
    /// Create a new `Table` instance.
    pub fn init(schema: Catalog) -> Self {
        Table {
            schema,
            next_row_id: Mutex::new(0),
            primary_index: None,
            secondary_index: Mutex::new(None),
        }
    }

    /// Returns the table name.
    pub fn get_table_name(&self) -> String {
        self.schema.table_name().clone()
    }

    /// Returns a shared reference to the table schema.
    pub fn get_schema(&self) -> &Catalog {
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
    pub fn set_primary_index(&mut self, index_name: &str) {
        self.primary_index = Some(String::from(index_name));
    }

    /// Get the primary index for a table
    pub fn get_primary_index(&self) -> Result<String, NonFatalError> {
        Ok(self.primary_index.as_ref().unwrap().clone())
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