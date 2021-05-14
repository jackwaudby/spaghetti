use crate::storage::catalog::Catalog;

use std::fmt;

#[derive(Debug)]
pub struct Table {
    /// Table schema.
    schema: Catalog,

    /// Next valid row id.
    next_rid: u64,
}

impl Table {
    /// Create a new table.
    pub fn init(schema: Catalog) -> Self {
        Table {
            schema,
            next_rid: 0,
        }
    }

    /// Returns a shared reference to the table schema.
    pub fn get_schema(&self) -> &Catalog {
        &self.schema
    }

    /// Returns the table name.
    pub fn get_table_name(&self) -> String {
        self.schema.table_name().clone()
    }

    /// Returns the table id.
    pub fn get_table_id(&self) -> u64 {
        self.schema.table_id()
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "schema: {}", self.schema)
    }
}
