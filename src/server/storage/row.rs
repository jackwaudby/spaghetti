use crate::common::error::NonFatalError;
use crate::server::storage::catalog::ColumnKind;
use crate::server::storage::datatype::Data;
use crate::server::storage::datatype::Field;
use crate::server::storage::table::Table;
use crate::workloads::PrimaryKey;

use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum State {
    Clean,
    Modified,
    Deleted,
}

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

    /// Row state.
    state: State,

    /// Sequence of accesses to the row (op_type,txn_id) (optional)
    access_history: Option<Vec<Access>>,

    /// List of delayed transactions. (thread_id, txn_id)
    delayed: Vec<(usize, u64)>,
}

/// Represents the type of access made to row.
#[derive(Debug, PartialEq, Clone)]
pub enum Access {
    Read(String),
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
        let t = Arc::clone(&table); // get handle to table
        let row_id = t.get_next_row_id(); // get row id
        let field_cnt = t.schema().column_cnt(); // get number of fields in this table
        let mut fields = Vec::new(); // create emty fields
        for _ in 0..field_cnt {
            fields.push(Field::new());
        }

        let access_history = match protocol {
            "sgt" | "hit" | "opt-hit" | "basic-sgt" => Some(vec![]), // (optional) track access history
            _ => None,
        };

        Row {
            primary_key: None,
            row_id,
            table: t,
            current_fields: fields,
            prev_fields: None,
            state: State::Clean,
            access_history,
            delayed: vec![],
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
    pub fn init_value(&mut self, col_name: &str, col_value: &str) -> Result<(), NonFatalError> {
        let table = Arc::clone(&self.table); // get handle to table
        let field_index = table.schema().column_position_by_name(col_name)?; // get index of field in row
        let field_type = table.schema().column_type_by_index(field_index); // get field type

        // convert value to `Data`
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
            ColumnKind::List => Data::List(vec![]), // lists are always empty upon initialisation
        };
        self.current_fields[field_index].set(value); // set value
        Ok(())
    }

    /// Get the values of `columns` in a row.
    pub fn get_values(
        &mut self,
        columns: &Vec<&str>,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        if let State::Deleted = self.state {
            return Err(NonFatalError::RowDeleted(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            )); // if marked deleted operation
        }

        let access_history = match protocol {
            "sgt" | "hit" | "opt-hit" | "basic-sgt" => {
                let ah = self.access_history.unwrap().clone(); // get access history
                self.append_access(Access::Read(tid.to_string())); // append operation
                Some(ah)
            }
            _ => None,
        };

        let table = Arc::clone(&self.table); // get handle to table
        let mut values = Vec::new(); // get each value
        for column in columns {
            let field_index = table.schema().column_position_by_name(column)?; // get index of field in row
            let field = &self.current_fields[field_index]; // get handle to field
            let value = field.get(); // clone field
            values.push(value); // add to result set
        }
        let res = OperationResult::new(Some(values), access_history); // create return result

        Ok(res)
    }

    /// Append value to list. (list datatype only).
    pub fn append_value(
        &mut self,
        column: &str,
        value: &str,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => {
                return Err(NonFatalError::RowDirty(
                    format!("{:?}", self.primary_key),
                    self.table.get_table_name(),
                ));
            }
            State::Deleted => {
                return Err(NonFatalError::RowDeleted(
                    format!("{:?}", self.primary_key),
                    self.table.get_table_name(),
                ));
            }
            State::Clean => {
                let access_history = match protocol {
                    "sgt" | "hit" | "opt-hit" | "basic-sgt" => {
                        let ah = self.get_access_history(); // get access history
                        self.append_access(Access::Write(tid.to_string())); // append this operation
                        Some(ah)
                    }
                    _ => None,
                };

                let prev_fields = self.current_fields.clone(); // create copy of old fields
                self.set_prev(Some(prev_fields)); // set prev
                let table = Arc::clone(&self.table); // get handle to table
                let field_index = table.schema().column_position_by_name(column)?; // get index of field in row
                let field_type = table.schema().column_type_by_index(field_index); // get type of field
                let field = &self.current_fields[field_index]; // get handle to field

                if let ColumnKind::List = field_type {
                    let current_list = field.get(); // get the current value
                    if let Data::List(mut list) = current_list {
                        let to_append = value.parse::<u64>().unwrap(); // convert to u64
                        list.push(to_append); // append value
                        self.current_fields[field_index].set(Data::List(list)); // set value
                    } else {
                        panic!("expected list data type");
                    }
                } else {
                    panic!(
                        "append operation not supported for {} data type",
                        field_type
                    );
                }

                self.state = State::Modified; // set state
                let res = OperationResult::new(None, access_history); // create return result

                return Ok(res);
            }
        }
    }

    /// Set the values of a field in a row.
    pub fn set_values(
        &mut self,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => {
                return Err(NonFatalError::RowDirty(
                    format!("{:?}", self.primary_key),
                    self.table.get_table_name(),
                ));
            }
            State::Deleted => {
                return Err(NonFatalError::RowDeleted(
                    format!("{:?}", self.primary_key),
                    self.table.get_table_name(),
                ));
            }
            State::Clean => {
                let access_history = match protocol {
                    "sgt" | "hit" | "opt-hit" | "basic-sgt" => {
                        let ah = self.access_history.unwrap().clone(); // get access history
                        self.append_access(Access::Write(tid.to_string())); // append this operation
                        Some(ah)
                    }
                    _ => None,
                };

                let prev_fields = self.current_fields.clone(); // create copy of old fields
                self.set_prev(Some(prev_fields)); // set prev
                let table = Arc::clone(&self.table); // get handle to table

                // update each field;
                for (i, col_name) in columns.iter().enumerate() {
                    let field_index = table.schema().column_position_by_name(col_name)?; // get index of field in row
                    let field_type = table.schema().column_type_by_index(field_index); // get type of field
                    let value = values[i]; // new value

                    // convert value to `Data`
                    let new_value = match field_type {
                        ColumnKind::VarChar => Data::VarChar(value.to_string()),
                        ColumnKind::Int => Data::Int(value.parse::<i64>().map_err(|_| {
                            NonFatalError::UnableToConvertToDataType(
                                value.to_string(),
                                "int".to_string(),
                            )
                        })?),
                        ColumnKind::Double => Data::Double(value.parse::<f64>().map_err(|_| {
                            NonFatalError::UnableToConvertToDataType(
                                value.to_string(),
                                "double".to_string(),
                            )
                        })?),
                        ColumnKind::List => {
                            panic!("set operation not supported for list data type")
                        }
                    };

                    self.current_fields[field_index].set(new_value); // set value
                }

                self.state = State::Modified; // set dirty flag
                let res = OperationResult::new(None, access_history); // create return result

                Ok(res)
            }
        }
    }

    /// Get and set
    pub fn get_and_set_values(
        &mut self,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        let res = self.get_values(columns, protocol, tid); // get old
        self.set_values(columns, values, protocol, tid)?; // set new
        res
    }

    /// Mark row as deleted.
    pub fn delete(&mut self, protocol: &str) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => {
                return Err(NonFatalError::RowDirty(
                    format!("{:?}", self.primary_key),
                    self.table.get_table_name(),
                ));
            }
            State::Deleted => {
                return Err(NonFatalError::RowDeleted(
                    format!("{:?}", self.primary_key),
                    self.table.get_table_name(),
                ));
            }
            State::Clean => {
                self.state = State::Deleted; // set state
                let access_history = match protocol {
                    "sgt" | "hit" | "opt-hit" | "basic-sgt" => {
                        let ah = self.access_history.unwrap().clone(); // get access history
                        Some(ah)
                    }
                    _ => None,
                };
                let res = OperationResult::new(None, access_history);
                return Ok(res);
            }
        }
    }

    /// Make an update permanent.
    ///
    /// Committing a delete is handled at the index level.
    pub fn commit(&mut self, protocol: &str, tid: &str) {
        self.state = State::Clean;
        self.set_prev(None);

        // safe to remove all accesses before this write; as if this committed all previous accesses must have committed.
        match protocol {
            "sgt" | "hit" | "opt-hit" | "basic-sgt" => {
                let mut ah = self.access_history.take().unwrap(); // take access history
                let ind = ah
                    .iter()
                    .position(|a| a == &Access::Write(tid.to_string()))
                    .unwrap(); // get index of this write
                let new_ah = ah.split_off(ind + 1); // remove "old" access information
                self.access_history = Some(new_ah); // reset
            }
            _ => {}
        };
    }

    /// Revert to previous version of row.
    ///
    /// Handles reverting a delete and an update.
    pub fn revert(&mut self, protocol: &str, tid: &str) {
        match self.state {
            State::Deleted => self.state = State::Clean,
            State::Modified => {
                self.current_fields = self.prev_fields.take().unwrap(); // revert to old values
                self.state = State::Clean;

                // remove all accesses after this write as they should also have aborted
                match protocol {
                    "sgt" | "hit" | "opt-hit" | "basic-sgt" => {
                        let mut ah = self.access_history.take().unwrap();
                        let ind = ah
                            .iter()
                            .position(|a| a == &Access::Write(tid.to_string()))
                            .unwrap(); // get index of this write
                        ah.split_off(ind); // remove "old" access information
                        self.access_history = Some(ah); // reset access history
                    }
                    _ => {}
                };
            }
            State::Clean => {}
        }
    }

    /// Revert reads to a `Row`.
    pub fn revert_read(&mut self, tid: &str) {
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
    pub fn append_access(&mut self, access: Access) {
        self.access_history.unwrap().push(access);
    }

    /// Get access history.
    pub fn get_access_history(&self) -> Vec<Access> {
        self.access_history.unwrap().clone()
    }

    /// Get row state
    pub fn get_state(&self) -> State {
        self.state.clone()
    }

    pub fn get_delayed(&self) -> Vec<(usize, u64)> {
        self.delayed.clone()
    }

    pub fn remove_delayed(&mut self, t: (usize, u64)) {
        self.delayed.retain(|&x| x != t);
    }

    pub fn add_delayed(&mut self, t: (usize, u64)) -> (usize, u64) {
        let wait = self.delayed.last().unwrap().clone();
        self.delayed.push(t);
        wait
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
    pub fn get_access_history(&self) -> Vec<Access> {
        self.access_history.unwrap().clone()
    }
}

// [row_id,pk,dirty,table_name,fields,access_history]
impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = Arc::clone(&self.table);
        let fc = self.current_fields.len();
        let mut fields = String::new();
        for field in &self.current_fields[0..fc - 1] {
            fields.push_str(format!("{}, ", field).as_str());
        }
        let last = &self.current_fields[fc - 1];
        fields.push_str(format!("{}", last).as_str());
        write!(
            f,
            "[{}, {:?}, {:?}, {}, {}, {:?}]",
            self.get_row_id(),
            self.get_primary_key(),
            self.state,
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

    use test_env_log::test;

    #[test]
    fn row_2pl_test() {
        // create table schema
        let mut catalog = Catalog::init("cars", 1);
        catalog.add_column(("name", "string")).unwrap();
        catalog.add_column(("year", "int")).unwrap();
        catalog.add_column(("amount", "double")).unwrap();
        catalog.add_column(("calories", "list")).unwrap();

        let table = Table::init(catalog); // create table
        let mut row = Row::new(Arc::new(table), "2pl"); // create row in table
        assert_eq!(row.get_table().get_table_id(), 1);
        assert_eq!(row.get_row_id(), 0);

        let rcolumns = vec!["name", "year", "amount", "calories"];
        let wcolumns = vec!["name", "year", "amount"];
        let values = vec!["el camino", "2019", "53.2"];

        // read
        let r1 = row.get_values(&rcolumns, "2pl", "t1").unwrap();
        assert_eq!(
            datatype::to_result(
                None,
                None,
                None,
                Some(&rcolumns),
                Some(&r1.get_values().unwrap())
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"calories\":\"null\",\"name\":\"null\",\"year\":\"null\"}}"
        );
        assert_eq!(r1.get_access_history(), None);

        assert_eq!(row.is_dirty(), false);
        assert_eq!(format!("{:?}", row.prev_fields), "None");

        // write
        let w1 = row.set_values(&wcolumns, &values, "2pl", "t1").unwrap();

        assert_eq!(w1.get_values(), None);
        assert_eq!(w1.get_access_history(), None);

        assert_eq!(row.is_dirty(), true);
        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: Null }])"
        );

        // read
        let r2 = row.get_values(&rcolumns, "2pl", "t1").unwrap();
        assert_eq!(
            datatype::to_result(
                None,
                None,
                None,
                Some(&rcolumns),
                Some(&r2.get_values().unwrap())
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"53.2\",\"calories\":\"null\",\"name\":\"el camino\",\"year\":\"2019\"}}"
        );
        assert_eq!(r2.get_access_history(), None);

        // commit
        row.commit("2pl", "t1");
        assert_eq!(format!("{:?}", row.prev_fields), "None");

        // write
        let values2 = vec!["ford", "2005", "78.2"];
        row.set_values(&wcolumns, &values2, "2pl", "t1").unwrap();

        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }, Field { data: Null }])"
        );
        assert_eq!(
                    format!("{:?}", row.current_fields),
            "[Field { data: VarChar(\"ford\") }, Field { data: Int(2005) }, Field { data: Double(78.2) }, Field { data: Null }]"
        );

        // revert
        row.revert("2pl", "t1");
        assert_eq!(format!("{:?}", row.prev_fields), "None");
        assert_eq!(
                    format!("{:?}", row.current_fields),
            "[Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }, Field { data: Null }]"
        );
        assert_eq!(row.is_dirty(), false);

        assert_eq!(
            format!("{}", row),
            "[0, None, false, cars, el camino, 2019, 53.2, null, None]"
        );
    }

    #[test]
    fn row_2pl_append_test() {
        // create table schema
        let mut catalog = Catalog::init("cars", 1);
        catalog.add_column(("name", "string")).unwrap();
        catalog.add_column(("year", "int")).unwrap();
        catalog.add_column(("amount", "double")).unwrap();
        catalog.add_column(("calories", "list")).unwrap();

        let table = Table::init(catalog); // create table
        let mut row = Row::new(Arc::new(table), "2pl"); // create row in table

        assert_eq!(row.get_table().get_table_id(), 1);
        assert_eq!(row.get_row_id(), 0);

        let rcolumns = vec!["name", "year", "amount", "calories"];

        row.init_value("calories", "").unwrap(); // init list

        // read
        let r1 = row.get_values(&rcolumns, "2pl", "t1").unwrap();
        assert_eq!(
            datatype::to_result(
                None,
                None,
                None,
                Some(&rcolumns),
                Some(&r1.get_values().unwrap())
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"calories\":\"[]\",\"name\":\"null\",\"year\":\"null\"}}"
        );
        assert_eq!(r1.get_access_history(), None);

        assert_eq!(row.is_dirty(), false);
        assert_eq!(format!("{:?}", row.prev_fields), "None");

        // write
        row.append_value("calories", "1678", "2pl", "t1").unwrap();

        assert_eq!(row.is_dirty(), true);
        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([]) }])"
        );

        // read
        let r2 = row.get_values(&rcolumns, "2pl", "t1").unwrap();

        assert_eq!(
            datatype::to_result(
                None,
                None,
                None,
                Some(&rcolumns),
                Some(&r2.get_values().unwrap())
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"calories\":\"[1678]\",\"name\":\"null\",\"year\":\"null\"}}"
        );
        assert_eq!(r2.get_access_history(), None);

        // commit
        row.commit("2pl", "t1");
        assert_eq!(format!("{:?}", row.prev_fields), "None");

        // write
        row.append_value("calories", "1203", "2pl", "t1").unwrap();

        assert_eq!(
            format!("{:?}", row.prev_fields),
            "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([1678]) }])"
        );
        assert_eq!(
            format!("{:?}", row.current_fields),
            "[Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([1678, 1203]) }]"
        );

        // revert
        row.revert("2pl", "t1");
        assert_eq!(format!("{:?}", row.prev_fields), "None");
        assert_eq!(
            format!("{:?}", row.current_fields),
            "[Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([1678]) }]"
        );

        assert_eq!(row.is_dirty(), false);

        assert_eq!(
            format!("{}", row),
            "[0, None, false, cars, null, null, null, [1678], None]"
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
            datatype::to_result(
                None,
                None,
                None,
                Some(&columns),
                Some(&r1.get_values().unwrap())
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"name\":\"null\",\"year\":\"null\"}}"
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
            datatype::to_result(
                None,
                None,
                None,
                Some(&columns),
                Some(&r2.get_values().unwrap())
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"53.2\",\"name\":\"el camino\",\"year\":\"2019\"}}"
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
