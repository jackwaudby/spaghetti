use crate::server::storage::catalog::ColumnKind;
use crate::server::storage::datatype::Data;
use crate::server::storage::datatype::Field;
use crate::server::storage::index::Access;
use crate::server::storage::table::Table;
use crate::workloads::PrimaryKey;
use crate::Result;

use std::fmt;
use std::sync::Arc;

/// Represents a row in the database.
#[derive(Debug)]
pub struct Row {
    /// Optional field storing the row's primary key.
    primary_key: Option<PrimaryKey>,

    /// Row id.
    row_id: u64,

    /// Handle to table row belongs to.
    table: Arc<Table>,

    /// List of fields.
    fields: Vec<Field>,

    /// Optional list of accesses.
    access_history: Option<Vec<Access>>,
}

impl Row {
    /// Return a new row instance.
    pub fn new(table: Arc<Table>, protocol: &str) -> Self {
        // Get handle to table.
        let t = Arc::clone(&table);
        // Get number of fields in this table.
        let field_cnt = t.schema().column_cnt();
        // Get row id.
        let row_id = t.get_next_row_id();
        // Create fields.
        let mut fields = Vec::new();
        for _ in 0..field_cnt {
            fields.push(Field::new());
        }
        // Optional track access history
        let access_history = match protocol {
            "sgt" => Some(vec![]),
            _ => None,
        };

        Row {
            primary_key: None,
            row_id,
            table: t,
            fields,
            access_history,
        }
    }

    /// Returns a row's primary key.
    pub fn get_primary_key(&self) -> Option<PrimaryKey> {
        self.primary_key
    }

    /// Set a row's primary key.
    pub fn set_primary_key(&mut self, key: PrimaryKey) {
        self.primary_key = Some(key);
    }

    /// Returns the row id.
    pub fn get_row_id(&self) -> u64 {
        self.row_id
    }

    /// Returns a shared reference to the `Table` the row belongs to.
    pub fn get_table(&self) -> Arc<Table> {
        Arc::clone(&self.table)
    }

    /// Get the value of a field in a row.
    pub fn get_value(&self, col_name: &str) -> Result<Data> {
        // Get reference to table row resides in.
        let table = Arc::clone(&self.table);
        // Get index of field in row.
        let field_index = table.schema().column_position_by_name(col_name)?;
        // Get field.
        let field = &self.fields[field_index];
        // Copy value.
        let value = field.get();
        Ok(value)
    }

    /// Set the value of a field in a row.
    pub fn set_value(&mut self, col_name: &str, col_value: &str) -> Result<()> {
        // Get handle to table.
        let table = Arc::clone(&self.table);
        // Get index of field in row.
        let field_index = table.schema().column_position_by_name(col_name)?;
        let field_type = table.schema().column_type_by_index(field_index);
        // Convert value to spaghetti data type.
        let value = match field_type {
            ColumnKind::VarChar => Data::VarChar(col_value.to_string()),
            ColumnKind::Int => Data::Int(col_value.parse::<i64>()?),
            ColumnKind::Double => Data::Double(col_value.parse::<f64>()?),
        };
        // Set value.
        self.fields[field_index].set(value);
        Ok(())
    }

    /// Append `Access` to access history.
    pub fn append_access(&mut self, access: Access) -> Result<()> {
        match &mut self.access_history {
            Some(ref mut ah) => ah.push(access),
            None => panic!("Access history not found"),
        }
        Ok(())
    }

    /// Get access history.
    pub fn get_access_history(&self) -> Result<Vec<Access>> {
        let ah = match &self.access_history {
            Some(ah) => ah.clone(),
            None => panic!("Access history not found"),
        };
        Ok(ah)
    }
}

// [row_id,pk,table_name,fields]
impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = Arc::clone(&self.table);
        let fc = self.fields.len();
        let mut fields = String::new();
        for field in &self.fields[0..fc - 1] {
            fields.push_str(format!("{}, ", field).as_str());
        }
        let last = &self.fields[fc - 1];
        fields.push_str(format!("{}", last).as_str());
        write!(
            f,
            "[{}, {:?}, {}, {}]",
            self.get_row_id(),
            self.get_primary_key(),
            table.get_table_name(),
            fields
        )
    }
}
#[cfg(test)]
mod tests {

    use super::*;
    use crate::server::storage::catalog::Catalog;

    #[test]
    fn row() {
        // create table schema
        let mut catalog = Catalog::init("films", 1);
        catalog.add_column(("name", "string")).unwrap();
        catalog.add_column(("year", "int")).unwrap();
        catalog.add_column(("amount", "double")).unwrap();
        // create table
        let table = Table::init(catalog);
        // create row in table
        let mut row = Row::new(Arc::new(table));
        assert_eq!(row.get_table().get_table_id(), 1);
        assert_eq!(row.get_row_id(), 0);
        assert_eq!(row.get_value("name").unwrap(), Data::Null);
        row.set_value("year", "2019").unwrap();
        assert_eq!(row.get_value("year").unwrap(), Data::Int(2019));
        row.set_value("name", "el camino").unwrap();
        assert_eq!(
            row.get_value("name").unwrap(),
            Data::VarChar("el camino".to_string())
        );
        row.set_value("amount", "53.2").unwrap();
        assert_eq!(row.get_value("amount").unwrap(), Data::Double(53.2));

        assert_eq!(
            format!("{}", row),
            "[0, None, films, el camino, 2019, 53.2]"
        );
    }
}
