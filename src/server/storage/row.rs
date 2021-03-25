use crate::common::error::{FatalError, NonFatalError};
use crate::server::storage::catalog::ColumnKind;
use crate::server::storage::datatype::Data;
use crate::server::storage::datatype::Field;
use crate::server::storage::table::Table;
use crate::workloads::PrimaryKey;

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

    /// Current version of fields.
    current_fields: Vec<Field>,

    /// Previous version of fields.
    prev_fields: Option<Vec<Field>>,

    /// Dirty flag.
    dirty: bool,

    /// Delete flag.
    delete: bool,

    /// Optional list of accesses.
    access_history: Option<Vec<Access>>,
}

/// Represents the type of access made to row.
#[derive(Debug, PartialEq, Clone)]
pub enum Access {
    /// Get
    Read(String),
    /// Set.
    Write(String),
}

/// Represents the packet of information returned from a get/set operation on a row.
#[derive(Debug)]
pub struct OperationResult {
    /// Optional values.
    values: Option<Vec<Data>>,
    /// Optional access history.
    access_history: Option<Vec<Access>>,
}

impl Row {
    /// Return an empty `Row`.
    pub fn new(table: Arc<Table>, protocol: &str) -> Self {
        // Get handle to table.
        let t = Arc::clone(&table);
        // Get row id.
        let row_id = t.get_next_row_id();
        // Get number of fields in this table.
        let field_cnt = t.schema().column_cnt();
        // Create fields.
        let mut fields = Vec::new();
        for _ in 0..field_cnt {
            fields.push(Field::new());
        }
        // Optional track access history
        let access_history = match protocol {
            "sgt" | "hit" => Some(vec![]),
            _ => None,
        };

        Row {
            primary_key: None,
            row_id,
            table: t,
            current_fields: fields,
            prev_fields: None,
            dirty: false,
            delete: false,
            access_history,
        }
    }

    /// Returns a row's primary key.
    pub fn get_primary_key(&self) -> Option<PrimaryKey> {
        self.primary_key.clone()
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

    /// Initialise the value of a field in a row. Used by loaders.
    ///
    /// # Non-Fatal Errors
    ///
    /// A non-fatal error is returned if there is a conversion error or the column cannot be found.
    /// - Parsing error.
    pub fn init_value(&mut self, col_name: &str, col_value: &str) -> Result<(), NonFatalError> {
        // Get handle to table.
        let table = Arc::clone(&self.table);

        // Get index of field in row.
        let field_index = table.schema().column_position_by_name(col_name)?;
        let field_type = table.schema().column_type_by_index(field_index);

        // Convert value to spaghetti data type.
        let value = match field_type {
            ColumnKind::VarChar => Data::VarChar(col_value.to_string()),
            ColumnKind::Int => Data::Int(col_value.parse::<i64>().map_err(|_| {
                NonFatalError::UnableToConvertToDataType(col_value.to_string(), "int".to_string())
            })?),
            ColumnKind::Double => Data::Double(col_value.parse::<f64>().map_err(|_| {
                NonFatalError::UnableToConvertToDataType(
                    col_value.to_string(),
                    "double".to_string(),
                )
            })?),
        };
        // Set value.
        self.current_fields[field_index].set(value);
        Ok(())
    }

    /// Get the values in a row.
    ///
    /// # Non-Fatal Errors
    ///
    /// A non-fatal error is returned if (i) row is marked for deletecolumn does not exist in the table.
    /// - Row marked for delete.
    ///
    /// # Fatal Errors
    ///
    /// The access history is not initialised.
    pub fn get_values(
        &mut self,
        columns: &Vec<&str>,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        // If deleted operation fails.
        if self.is_deleted() {
            return Err(NonFatalError::RowDeleted(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            ));
        }

        let access_history = match protocol {
            "sgt" | "hit" => {
                // Get access history.
                let ah = self.get_access_history().unwrap();
                // Append this operation.
                self.append_access(Access::Read(tid.to_string())).unwrap();
                Some(ah)
            }
            _ => None,
        };

        // Get reference to table row resides in.
        let table = Arc::clone(&self.table);
        // Get each value.
        let mut values = Vec::new();
        for column in columns {
            // Get index of field in row.
            let field_index = table.schema().column_position_by_name(column)?;
            // Get field.
            let field = &self.current_fields[field_index];
            // Copy value.
            let value = field.get();
            // Add to values.
            values.push(value);
        }
        // Create return result.
        let res = OperationResult::new(Some(values), access_history);

        Ok(res)
    }

    /// Set the values of a field in a row.
    pub fn set_values(
        &mut self,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        // If dirty operation fails.
        if self.is_dirty() {
            return Err(NonFatalError::RowDirty(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            ));
        }

        // If deleted operation fails.
        if self.is_deleted() {
            return Err(NonFatalError::RowDeleted(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            ));
        }

        let access_history = match protocol {
            "sgt" | "hit" => {
                // Get access history.
                let ah = self.get_access_history().unwrap();
                // Append this operation.
                self.append_access(Access::Write(tid.to_string())).unwrap();
                Some(ah)
            }
            _ => None,
        };

        // Create copy of old fields.
        let prev_fields = self.current_fields.clone();
        // Set prev.
        self.set_prev(Some(prev_fields));

        // Get handle to table.
        let table = Arc::clone(&self.table);
        // Update each field.
        for (i, col_name) in columns.iter().enumerate() {
            // Get index of field in row.
            let field_index = table.schema().column_position_by_name(col_name)?;
            // Get type of field.
            let field_type = table.schema().column_type_by_index(field_index);
            // New value.
            let value = values[i];
            // Convert value to spaghetti data type.
            let new_value = match field_type {
                ColumnKind::VarChar => Data::VarChar(value.to_string()),
                ColumnKind::Int => Data::Int(value.parse::<i64>().map_err(|_| {
                    NonFatalError::UnableToConvertToDataType(value.to_string(), "int".to_string())
                })?),
                ColumnKind::Double => Data::Double(value.parse::<f64>().map_err(|_| {
                    NonFatalError::UnableToConvertToDataType(
                        value.to_string(),
                        "double".to_string(),
                    )
                })?),
            };
            // Set value.
            self.current_fields[field_index].set(new_value);
        }
        // Set dirty flag.
        self.set_dirty(true);
        // Create return result.
        let res = OperationResult::new(None, access_history);

        Ok(res)
    }

    /// Mark row as deleted.
    pub fn delete(&mut self, protocol: &str) -> Result<OperationResult, NonFatalError> {
        // If dirty operation fails.
        if self.is_dirty() {
            return Err(NonFatalError::RowDirty(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            ));
        }

        // If deleted operation fails.
        if self.is_deleted() {
            return Err(NonFatalError::RowDeleted(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            ));
        }

        // Set dirty flag.
        self.set_deleted(true);

        // Get access history.
        let access_history = match protocol {
            "sgt" | "hit" => {
                // Get access history.
                let ah = self.get_access_history().unwrap();
                Some(ah)
            }
            _ => None,
        };

        let res = OperationResult::new(None, access_history);
        Ok(res)
    }

    /// Make an update permanent.
    ///
    /// Committing a delete is handled at the index level.
    pub fn commit(&mut self, protocol: &str, tid: &str) {
        // Set dirty flag to false.
        self.set_dirty(false);
        // Remove perv version.
        self.set_prev(None);
        // Trim access history.
        match protocol {
            "sgt" | "hit" => {
                let mut ah = self.access_history.take().unwrap();

                // Get index of this write.
                let ind = ah
                    .iter()
                    .position(|a| a == &Access::Write(tid.to_string()))
                    .unwrap();
                // Remove "old" access information.
                let new_ah = ah.split_off(ind + 1);
                // Reset access history
                self.access_history = Some(new_ah);
            }

            _ => {}
        };
    }

    /// Revert to previous version of row.
    ///
    /// Handles reverting a delete and an update.
    pub fn revert(&mut self, protocol: &str, tid: &str) {
        // Handle case when record has been flagged for deletion.
        if self.delete {
            self.delete = false;
        } else {
            // Handle case when record has been updated.
            // Retrieve old values.
            let old_fields = self.prev_fields.take();
            // Reset.
            self.current_fields = old_fields.unwrap();
            self.set_dirty(false);
            // Trim access history.
            match protocol {
                "sgt" | "hit" => {
                    let mut ah = self.access_history.take().unwrap();

                    // Get index of this write.
                    let ind = ah
                        .iter()
                        .position(|a| a == &Access::Write(tid.to_string()))
                        .unwrap();
                    // Remove "old" access information.
                    let _s = ah.split_off(ind);
                    // Reset access history
                    self.access_history = Some(ah);
                }
                _ => {}
            };
        }
    }

    /// Revert reads to a `Row`.
    ///
    /// Handles reverting a read operation SGT and HIT only.
    pub fn revert_read(&mut self, tid: &str) {
        // Remove read from access history
        self.access_history
            .as_mut()
            .unwrap()
            .retain(|a| a != &Access::Read(tid.to_string()));
    }

    /// Set previous version of row.
    fn set_prev(&mut self, vers: Option<Vec<Field>>) {
        self.prev_fields = vers;
    }

    /// Append `Access` to access history.
    pub fn append_access(&mut self, access: Access) -> Result<(), FatalError> {
        match &mut self.access_history {
            Some(ref mut ah) => ah.push(access),
            None => return Err(FatalError::NotTrackingAccessHistory),
        }
        Ok(())
    }

    /// Get access history.
    pub fn get_access_history(&self) -> Result<Vec<Access>, FatalError> {
        let ah = match &self.access_history {
            Some(ah) => ah.clone(),
            None => return Err(FatalError::NotTrackingAccessHistory),
        };
        Ok(ah)
    }

    /// Get dirty flag.
    fn is_dirty(&self) -> bool {
        self.dirty
    }

    // Set dirty flag.
    fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Get delete flag.
    pub fn is_deleted(&self) -> bool {
        self.delete
    }

    // Set delete flag.
    pub fn set_deleted(&mut self, delete: bool) {
        self.delete = delete;
    }
}

impl OperationResult {
    /// Create new operation result.
    pub fn new(values: Option<Vec<Data>>, access_history: Option<Vec<Access>>) -> OperationResult {
        OperationResult {
            values,
            access_history,
        }
    }

    /// Get values.
    pub fn get_values(&self) -> Option<Vec<Data>> {
        self.values.clone()
    }

    /// Get access history.
    pub fn get_access_history(&self) -> Option<Vec<Access>> {
        self.access_history.clone()
    }
}

// [row_id,pk,dirty,table_name,fields,access_history]
impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Table.
        let table = Arc::clone(&self.table);
        // Current fields.
        let fc = self.current_fields.len();
        let mut fields = String::new();
        for field in &self.current_fields[0..fc - 1] {
            fields.push_str(format!("{}, ", field).as_str());
        }
        let last = &self.current_fields[fc - 1];
        fields.push_str(format!("{}", last).as_str());
        write!(
            f,
            "[{}, {:?}, {}, {}, {}, {:?}]",
            self.get_row_id(),
            self.get_primary_key(),
            self.is_dirty(),
            table.get_table_name(),
            fields,
            self.access_history
        )
    }
}
#[cfg(test)]
mod tests {

    use super::*;
    use crate::server::storage::catalog::Catalog;
    use crate::server::storage::datatype;

    #[test]
    fn row_2pl_test() {
        // create table schema
        let mut catalog = Catalog::init("cars", 1);
        catalog.add_column(("name", "string")).unwrap();
        catalog.add_column(("year", "int")).unwrap();
        catalog.add_column(("amount", "double")).unwrap();
        // create table
        let table = Table::init(catalog);
        // create row in table
        let mut row = Row::new(Arc::new(table), "2pl");
        assert_eq!(row.get_table().get_table_id(), 1);
        assert_eq!(row.get_row_id(), 0);

        let columns = vec!["name", "year", "amount"];
        let values = vec!["el camino", "2019", "53.2"];

        // read
        let r1 = row.get_values(&columns, "2pl", "t1").unwrap();
        assert_eq!(
            datatype::to_result(&columns, &r1.get_values().unwrap()).unwrap(),
            "{name=\"null\", year=\"null\", amount=\"null\"}"
        );
        assert_eq!(r1.get_access_history(), None);

        assert_eq!(row.is_dirty(), false);
        assert_eq!(format!("{:?}", row.prev_fields), "None");

        // write
        let w1 = row.set_values(&columns, &values, "2pl", "t1").unwrap();
        assert_eq!(w1.get_values(), None);
        assert_eq!(w1.get_access_history(), None);

        assert_eq!(row.is_dirty(), true);
        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }])"
        );

        // read
        let r2 = row.get_values(&columns, "2pl", "t1").unwrap();
        assert_eq!(
            datatype::to_result(&columns, &r2.get_values().unwrap()).unwrap(),
            "{name=\"el camino\", year=\"2019\", amount=\"53.2\"}"
        );
        assert_eq!(r2.get_access_history(), None);

        // commit
        row.commit("2pl", "t1");
        assert_eq!(format!("{:?}", row.prev_fields), "None");

        // write
        let values2 = vec!["ford", "2005", "78.2"];
        row.set_values(&columns, &values2, "2pl", "t1").unwrap();

        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }])"
        );
        assert_eq!(
                    format!("{:?}", row.current_fields),
            "[Field { data: VarChar(\"ford\") }, Field { data: Int(2005) }, Field { data: Double(78.2) }]"
        );

        // revert
        row.revert("2pl", "t1");
        assert_eq!(format!("{:?}", row.prev_fields), "None");
        assert_eq!(
                    format!("{:?}", row.current_fields),
            "[Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }]"
        );
        assert_eq!(row.is_dirty(), false);

        assert_eq!(
            format!("{}", row),
            "[0, None, false, cars, el camino, 2019, 53.2, None]"
        );
    }

    #[test]
    fn row_sgt_test() {
        // create table schema
        let mut catalog = Catalog::init("cars", 1);
        catalog.add_column(("name", "string")).unwrap();
        catalog.add_column(("year", "int")).unwrap();
        catalog.add_column(("amount", "double")).unwrap();
        // create table
        let table = Table::init(catalog);
        // create row in table
        let mut row = Row::new(Arc::new(table), "sgt");
        assert_eq!(row.get_table().get_table_id(), 1);
        assert_eq!(row.get_row_id(), 0);

        let columns = vec!["name", "year", "amount"];
        let values = vec!["el camino", "2019", "53.2"];

        // read
        let r1 = row.get_values(&columns, "sgt", "t1").unwrap();
        assert_eq!(
            datatype::to_result(&columns, &r1.get_values().unwrap()).unwrap(),
            "{name=\"null\", year=\"null\", amount=\"null\"}"
        );
        assert_eq!(r1.get_access_history(), Some(vec![]));
        assert_eq!(
            row.access_history,
            Some(vec![Access::Read("t1".to_string())])
        );
        assert_eq!(row.is_dirty(), false);
        assert_eq!(format!("{:?}", row.prev_fields), "None");

        // write
        let w1 = row.set_values(&columns, &values, "sgt", "t2").unwrap();
        assert_eq!(w1.get_values(), None);
        assert_eq!(
            w1.get_access_history(),
            Some(vec![Access::Read("t1".to_string())])
        );
        assert_eq!(
            row.access_history,
            Some(vec![
                Access::Read("t1".to_string()),
                Access::Write("t2".to_string())
            ])
        );

        assert_eq!(row.is_dirty(), true);
        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }])"
        );

        // read
        let r2 = row.get_values(&columns, "sgt", "t3").unwrap();
        assert_eq!(
            datatype::to_result(&columns, &r2.get_values().unwrap()).unwrap(),
            "{name=\"el camino\", year=\"2019\", amount=\"53.2\"}"
        );
        assert_eq!(
            r2.get_access_history(),
            Some(vec![
                Access::Read("t1".to_string()),
                Access::Write("t2".to_string())
            ])
        );
        assert_eq!(
            row.access_history,
            Some(vec![
                Access::Read("t1".to_string()),
                Access::Write("t2".to_string()),
                Access::Read("t3".to_string())
            ])
        );

        // commit
        row.commit("sgt", "t2");
        assert_eq!(format!("{:?}", row.prev_fields), "None");
        assert_eq!(
            row.access_history,
            Some(vec![Access::Read("t3".to_string())])
        );
        assert_eq!(row.is_dirty(), false);

        // write
        let values2 = vec!["ford", "2005", "78.2"];
        let w2 = row.set_values(&columns, &values2, "sgt", "t4").unwrap();
        assert_eq!(
            w2.get_access_history(),
            Some(vec![Access::Read("t3".to_string())])
        );
        assert_eq!(
            row.access_history,
            Some(vec![
                Access::Read("t3".to_string()),
                Access::Write("t4".to_string())
            ])
        );
        assert_eq!(row.is_dirty(), true);
        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }])"
        );
        assert_eq!(
                    format!("{:?}", row.current_fields),
            "[Field { data: VarChar(\"ford\") }, Field { data: Int(2005) }, Field { data: Double(78.2) }]"
        );

        // revert
        row.revert("sgt", "t4");
        assert_eq!(format!("{:?}", row.prev_fields), "None");
        assert_eq!(
                    format!("{:?}", row.current_fields),
            "[Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }]"
        );
        assert_eq!(row.is_dirty(), false);
        assert_eq!(
            row.access_history,
            Some(vec![Access::Read("t3".to_string()),])
        );

        assert_eq!(
            format!("{}", row),
            "[0, None, false, cars, el camino, 2019, 53.2, Some([Read(\"t3\")])]"
        );
    }

    #[test]
    fn row_edge_cases_test() {
        // create table schema
        let mut catalog = Catalog::init("cars", 1);
        catalog.add_column(("name", "string")).unwrap();
        catalog.add_column(("year", "int")).unwrap();
        catalog.add_column(("amount", "double")).unwrap();
        // create table
        let table = Arc::new(Table::init(catalog));
        // create row in table
        let mut row = Row::new(Arc::clone(&table), "sgt");

        assert_eq!(row.get_table().get_table_id(), 1);
        assert_eq!(row.get_row_id(), 0);

        let columns = vec!["name", "year", "amount"];
        let values = vec!["el camino", "2019", "53.2"];

        // writes
        row.set_values(&columns, &values, "sgt", "t1").unwrap();

        assert_eq!(
            format!(
                "{}",
                row.set_values(&columns, &values, "sgt", "t2").unwrap_err()
            ),
            "dirty: None in table cars"
        );

        // access history error.
        let mut row1 = Row::new(Arc::clone(&table), "2pl");

        assert_eq!(
            format!(
                "{}",
                row1.append_access(Access::Read("t1".to_string()))
                    .unwrap_err()
            ),
            "not tracking access history"
        );

        assert_eq!(
            format!("{}", row1.get_access_history().unwrap_err()),
            "not tracking access history"
        );

        // init_value
        let mut row1 = Row::new(Arc::clone(&table), "sgt");
        assert_eq!(row1.init_value("name", "jack").unwrap(), ());
        assert_eq!(row1.init_value("year", "40").unwrap(), ());
        assert_eq!(row1.init_value("amount", "43.2").unwrap(), ());
        // row dirty
        row1.set_values(&columns, &values, "sgt", "t2").unwrap();

        assert_eq!(
            format!(
                "{}",
                row1.set_values(&columns, &values, "sgt", "t2").unwrap_err()
            ),
            "dirty: None in table cars"
        );
    }
}
