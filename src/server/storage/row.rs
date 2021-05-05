use crate::common::error::NonFatalError;
use crate::server::scheduler::TransactionInfo;
use crate::server::storage::catalog::ColumnKind;
use crate::server::storage::datatype::{Data, Field};
use crate::server::storage::table::Table;
use crate::workloads::PrimaryKey;

use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum State {
    Clean,
    Modified,
    Deleted,
}

/// Represents a row in the database.
#[derive(Debug)]
pub struct Row {
    /// Primary key.
    primary_key: PrimaryKey,

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

    /// Sequence of accesses to the row.
    access_history: Option<Vec<Access>>,

    /// List of delayed transactions.
    delayed: Option<Vec<TransactionInfo>>,
}

/// Represents the type of access made to row.
#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionInfo),
    Write(TransactionInfo),
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
    pub fn new(
        primary_key: PrimaryKey,
        table: Arc<Table>,
        track_access: bool,
        track_delayed: bool,
    ) -> Self {
        let row_id = table.get_next_row_id();

        let fields = table.get_schema().column_cnt();
        let mut current_fields = Vec::with_capacity(fields);
        for _ in 0..fields {
            current_fields.push(Field::new());
        }

        let access_history;
        if track_access {
            access_history = Some(vec![]);
        } else {
            access_history = None;
        }

        let delayed;
        if track_delayed {
            delayed = Some(vec![]);
        } else {
            delayed = None;
        }

        Row {
            primary_key,
            row_id,
            table,
            current_fields,
            prev_fields: None,
            state: State::Clean,
            access_history,
            delayed,
        }
    }

    /// Returns a row's primary key.
    pub fn get_primary_key(&self) -> PrimaryKey {
        self.primary_key.clone()
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
    pub fn init_value(&mut self, column: &str, value: Data) -> Result<(), NonFatalError> {
        let schema = self.table.get_schema();
        let field_index = schema.column_position_by_name(column)?;
        let field_type = schema.column_type_by_index(field_index);

        data_eq_column(field_type, &value)?;

        self.current_fields[field_index].set(value);

        Ok(())
    }

    /// Get values of columns.
    pub fn get_values(
        &mut self,
        columns: &[&str],
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        if let State::Deleted = self.state {
            return Err(NonFatalError::RowDeleted(
                self.primary_key.to_string(),
                self.table.get_table_name(),
            ));
        }

        let access_history;
        if let TransactionInfo::TwoPhaseLocking { .. } = tid {
            access_history = None;
        } else {
            let ah = self.access_history.as_ref().unwrap().clone();
            self.append_access(Access::Read(tid.clone()));
            access_history = Some(ah);
        }

        let mut values = Vec::with_capacity(columns.len());
        let schema = self.table.get_schema();
        for column in columns {
            let field_index = schema.column_position_by_name(column)?;
            let field = &self.current_fields[field_index];
            let value = field.get();
            values.push(value);
        }
        Ok(OperationResult::new(Some(values), access_history))
    }

    /// Append value to list. (list datatype only).
    pub fn append_value(
        &mut self,
        column: &str,
        value: Data,
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => Err(NonFatalError::RowDirty(
                self.primary_key.to_string(),
                self.table.get_table_name(),
            )),
            State::Deleted => Err(NonFatalError::RowDeleted(
                self.primary_key.to_string(),
                self.table.get_table_name(),
            )),
            State::Clean => {
                let schema = self.table.get_schema(); // get schema
                let field_index = schema.column_position_by_name(column)?; // get field index
                let field_type = schema.column_type_by_index(field_index); // get field type
                data_eq_column(field_type, &Data::List(vec![]))?; // check field is list type

                let prev_fields = self.current_fields.clone(); // set prev fields
                self.prev_fields = Some(prev_fields);

                self.current_fields[field_index].append(value); // append value to list

                let access_history;
                if let TransactionInfo::TwoPhaseLocking { .. } = tid {
                    access_history = None;
                } else {
                    let ah = self.access_history.as_ref().unwrap().clone();
                    self.append_access(Access::Read(tid.clone()));
                    access_history = Some(ah);
                }

                self.state = State::Modified; // set state
                let res = OperationResult::new(None, access_history); // create return result

                Ok(res)
            }
        }
    }

    /// Set the values of a field in a row.
    pub fn set_values(
        &mut self,
        columns: &[&str],
        values: &[Data],
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => Err(NonFatalError::RowDirty(
                self.primary_key.to_string(),
                self.table.get_table_name(),
            )),
            State::Deleted => Err(NonFatalError::RowDeleted(
                self.primary_key.to_string(),
                self.table.get_table_name(),
            )),
            State::Clean => {
                let prev_fields = self.current_fields.clone(); // set prev fields
                self.prev_fields = Some(prev_fields);

                self.state = State::Modified; // set state

                let access_history;
                if let TransactionInfo::TwoPhaseLocking { .. } = tid {
                    access_history = None;
                } else {
                    let ah = self.access_history.as_ref().unwrap().clone();
                    self.append_access(Access::Write(tid.clone()));
                    access_history = Some(ah);
                }

                let schema = self.table.get_schema();

                // update each field;
                for (i, col_name) in columns.iter().enumerate() {
                    let field_index = schema.column_position_by_name(col_name)?; // get index of field in row
                    let field_type = schema.column_type_by_index(field_index); // get type of field
                    data_eq_column(field_type, &values[i])?; // check field is list type
                    self.current_fields[field_index].set(values[i].clone());
                }

                let res = OperationResult::new(None, access_history); // create return result

                Ok(res)
            }
        }
    }

    /// Get and set
    pub fn get_and_set_values(
        &mut self,
        columns: &[&str],
        values: &[Data],
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let res = self.get_values(columns, tid); // get old
        self.set_values(columns, values, tid)?; // set new
        res
    }

    /// Mark row as deleted.
    ///
    /// Committing a delete is handled at the index level.
    pub fn delete(&mut self, tid: &TransactionInfo) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => Err(NonFatalError::RowDirty(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            )),
            State::Deleted => Err(NonFatalError::RowDeleted(
                format!("{:?}", self.primary_key),
                self.table.get_table_name(),
            )),
            State::Clean => {
                self.state = State::Deleted; // set state

                let access_history;
                if let TransactionInfo::TwoPhaseLocking { .. } = tid {
                    access_history = None;
                } else {
                    let ah = self.access_history.as_ref().unwrap().clone();
                    access_history = Some(ah);
                }

                let res = OperationResult::new(None, access_history);
                Ok(res)
            }
        }
    }

    /// Make an update permanent.
    pub fn commit(&mut self, tid: &TransactionInfo) {
        self.state = State::Clean;
        self.prev_fields = None;

        // safe to remove all accesses before this write; as if this committed all previous accesses must have committed.
        if let Some(ref mut ah) = &mut self.access_history {
            let ind = ah
                .iter()
                .position(|a| a == &Access::Write(tid.clone()))
                .unwrap();
            let new_ah = ah.split_off(ind + 1); // remove "old" access information; including self
            self.access_history = Some(new_ah); // reset
        }
    }

    /// Revert to previous version of row.
    ///
    /// Handles reverting a delete and an update.
    pub fn revert(&mut self, tid: &TransactionInfo) {
        match self.state {
            State::Deleted => self.state = State::Clean,
            State::Modified => {
                self.current_fields = self.prev_fields.take().unwrap(); // revert to old values
                self.state = State::Clean;

                // remove all accesses after this write as they should also have aborted
                if let Some(ref mut ah) = &mut self.access_history {
                    let ind = ah
                        .iter()
                        .position(|a| a == &Access::Write(tid.clone()))
                        .unwrap();
                    ah.truncate(ind); // remove "old" access information

                    self.access_history = Some(ah.to_vec()); // reset
                }
            }
            State::Clean => {}
        }
    }

    /// Revert reads to a `Row`.
    pub fn revert_read(&mut self, tid: &TransactionInfo) {
        self.access_history
            .as_mut()
            .unwrap()
            .retain(|a| a != &Access::Read(tid.clone()));
    }

    /// Append `Access` to access history.
    pub fn append_access(&mut self, access: Access) {
        self.access_history.as_mut().unwrap().push(access);
    }

    /// Get access history.
    pub fn get_access_history(&self) -> Vec<Access> {
        self.access_history.as_ref().unwrap().clone()
    }

    /// Get row state
    pub fn get_state(&self) -> State {
        self.state.clone()
    }

    /// Returns `true` if there are transactions delayed from operating on the row.
    pub fn is_delayed(&self) -> bool {
        !self.delayed.as_ref().unwrap().is_empty()
    }

    /// Returns a copy of the delayed transactions.
    pub fn get_delayed(&self) -> Vec<TransactionInfo> {
        self.delayed.as_ref().unwrap().clone()
    }

    /// Removes a transaction from the delayed queue.
    pub fn remove_delayed(&mut self, t: &TransactionInfo) {
        self.delayed.as_mut().unwrap().retain(|x| x != t);
    }

    /// Append a transaction to the delayed queue.
    pub fn append_delayed(&mut self, t: &TransactionInfo) {
        self.delayed.as_mut().unwrap().push(t.clone());
    }

    /// Returns true if the transaction is the head of the delayed queue and the row is clean.
    pub fn resume(&self, t: &TransactionInfo) -> bool {
        self.delayed.as_ref().unwrap()[0] == *t && self.state == State::Clean
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
    pub fn get_values(&self) -> Vec<Data> {
        self.values.as_ref().unwrap().clone()
    }

    /// Get access history.
    pub fn get_access_history(&self) -> Vec<Access> {
        self.access_history.as_ref().unwrap().clone()
    }
}

/// Returns true if the value type matches the column type.
fn data_eq_column(a: &ColumnKind, b: &Data) -> Result<(), NonFatalError> {
    match (a, b) {
        (&ColumnKind::Double, &Data::Double(..)) => Ok(()),
        (&ColumnKind::Int, &Data::Int(..)) => Ok(()),
        (&ColumnKind::Uint, &Data::Uint(..)) => Ok(()),
        (&ColumnKind::List, &Data::List(..)) => Ok(()),
        (&ColumnKind::VarChar, &Data::VarChar(..)) => Ok(()),
        _ => Err(NonFatalError::InvalidColumnType(a.to_string())), // TODO: need better error
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            State::Clean => write!(f, "clean"),
            State::Modified => write!(f, "dirty"),
            State::Deleted => write!(f, "deleted"),
        }
    }
}
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
            "[{}, {:?}, {:?}, {}, {}, {:?}, {:?}]",
            self.get_row_id(),
            self.get_primary_key(),
            self.state,
            table.get_table_name(),
            fields,
            self.access_history,
            self.delayed,
        )
    }
}

// #[cfg(test)]
// mod tests {

//     use super::*;
//     use crate::server::storage::catalog::Catalog;
//     use crate::server::storage::datatype;

//     use test_env_log::test;

//     #[test]
//     fn row_2pl_test() {
//         // create table schema
//         let mut catalog = Catalog::init("cars", 1);
//         catalog.add_column(("name", "string")).unwrap();
//         catalog.add_column(("year", "int")).unwrap();
//         catalog.add_column(("amount", "double")).unwrap();
//         catalog.add_column(("calories", "list")).unwrap();

//         let table = Table::init(catalog); // create table
//         let mut row = Row::new(Arc::new(table), "2pl"); // create row in table
//         assert_eq!(row.get_table().get_table_id(), 1);
//         assert_eq!(row.get_row_id(), 0);

//         let rcolumns = vec!["name", "year", "amount", "calories"];
//         let wcolumns = vec!["name", "year", "amount"];
//         let values = vec!["el camino", "2019", "53.2"];

//         // read
//         let r1 = row.get_values(&rcolumns, "2pl", "t1").unwrap();
//         assert_eq!(
//             datatype::to_result(
//                 None,
//                 None,
//                 None,
//                 Some(&rcolumns),
//                 Some(&r1.get_values().unwrap())
//             )
//                 .unwrap(),
//             "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"calories\":\"null\",\"name\":\"null\",\"year\":\"null\"}}"
//         );

//         assert_eq!(format!("{:?}", row.prev_fields), "None");

//         // write
//         let w1 = row.set_values(&wcolumns, &values, "2pl", "t1").unwrap();

//         assert_eq!(w1.get_values(), None);

//         assert_eq!(
//             format!("{:?}", row.prev_fields),
//             "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: Null }])"
//         );

//         // read
//         let r2 = row.get_values(&rcolumns, "2pl", "t1").unwrap();
//         assert_eq!(
//             datatype::to_result(
//                 None,
//                 None,
//                 None,
//                 Some(&rcolumns),
//                 Some(&r2.get_values().unwrap())
//             )
//                 .unwrap(),
//             "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"53.2\",\"calories\":\"null\",\"name\":\"el camino\",\"year\":\"2019\"}}"
//         );

//         // commit
//         row.commit("2pl", "t1");
//         assert_eq!(format!("{:?}", row.prev_fields), "None");

//         // write
//         let values2 = vec!["ford", "2005", "78.2"];
//         row.set_values(&wcolumns, &values2, "2pl", "t1").unwrap();

//         assert_eq!(
//             format!("{:?}", row.prev_fields),
//             "Some([Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }, Field { data: Null }])"
//         );
//         assert_eq!(
//                     format!("{:?}", row.current_fields),
//             "[Field { data: VarChar(\"ford\") }, Field { data: Int(2005) }, Field { data: Double(78.2) }, Field { data: Null }]"
//         );

//         // revert
//         row.revert("2pl", "t1");
//         assert_eq!(format!("{:?}", row.prev_fields), "None");
//         assert_eq!(
//                     format!("{:?}", row.current_fields),
//             "[Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }, Field { data: Null }]"
//         );

//         assert_eq!(
//             format!("{}", row),
//             "[0, None, Clean, cars, el camino, 2019, 53.2, null, None, []]"
//         );
//     }

//     #[test]
//     fn row_2pl_append_test() {
//         // create table schema
//         let mut catalog = Catalog::init("cars", 1);
//         catalog.add_column(("name", "string")).unwrap();
//         catalog.add_column(("year", "int")).unwrap();
//         catalog.add_column(("amount", "double")).unwrap();
//         catalog.add_column(("calories", "list")).unwrap();

//         let table = Table::init(catalog); // create table
//         let mut row = Row::new(Arc::new(table), "2pl"); // create row in table

//         assert_eq!(row.get_table().get_table_id(), 1);
//         assert_eq!(row.get_row_id(), 0);

//         let rcolumns = vec!["name", "year", "amount", "calories"];

//         row.init_value("calories", "").unwrap(); // init list

//         // read
//         let r1 = row.get_values(&rcolumns, "2pl", "t1").unwrap();
//         assert_eq!(
//             datatype::to_result(
//                 None,
//                 None,
//                 None,
//                 Some(&rcolumns),
//                 Some(&r1.get_values().unwrap())
//             )
//                 .unwrap(),
//             "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"calories\":\"[]\",\"name\":\"null\",\"year\":\"null\"}}"
//         );

//         assert_eq!(format!("{:?}", row.prev_fields), "None");

//         // write
//         row.append_value("calories", "1678", "2pl", "t1").unwrap();

//         assert_eq!(
//             format!("{:?}", row.prev_fields),
//             "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([]) }])"
//         );

//         // read
//         let r2 = row.get_values(&rcolumns, "2pl", "t1").unwrap();

//         assert_eq!(
//             datatype::to_result(
//                 None,
//                 None,
//                 None,
//                 Some(&rcolumns),
//                 Some(&r2.get_values().unwrap())
//             )
//                 .unwrap(),
//             "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"calories\":\"[1678]\",\"name\":\"null\",\"year\":\"null\"}}"
//         );

//         // commit
//         row.commit("2pl", "t1");
//         assert_eq!(format!("{:?}", row.prev_fields), "None");

//         // write
//         row.append_value("calories", "1203", "2pl", "t1").unwrap();

//         assert_eq!(
//             format!("{:?}", row.prev_fields),
//             "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([1678]) }])"
//         );
//         assert_eq!(
//             format!("{:?}", row.current_fields),
//             "[Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([1678, 1203]) }]"
//         );

//         // revert
//         row.revert("2pl", "t1");
//         assert_eq!(format!("{:?}", row.prev_fields), "None");
//         assert_eq!(
//             format!("{:?}", row.current_fields),
//             "[Field { data: Null }, Field { data: Null }, Field { data: Null }, Field { data: List([1678]) }]"
//         );

//         assert_eq!(
//             format!("{}", row),
//             "[0, None, Clean, cars, null, null, null, [1678], None, []]"
//         );
//     }

//     #[test]
//     fn row_sgt_test() {
//         // create table schema
//         let mut catalog = Catalog::init("cars", 1);
//         catalog.add_column(("name", "string")).unwrap();
//         catalog.add_column(("year", "int")).unwrap();
//         catalog.add_column(("amount", "double")).unwrap();
//         // create table
//         let table = Table::init(catalog);
//         // create row in table
//         let mut row = Row::new(Arc::new(table), "sgt");
//         assert_eq!(row.get_table().get_table_id(), 1);
//         assert_eq!(row.get_row_id(), 0);

//         let columns = vec!["name", "year", "amount"];
//         let values = vec!["el camino", "2019", "53.2"];

//         // read
//         let r1 = row.get_values(&columns, "sgt", "t1").unwrap();
//         assert_eq!(
//             datatype::to_result(
//                 None,
//                 None,
//                 None,
//                 Some(&columns),
//                 Some(&r1.get_values().unwrap())
//             )
//                 .unwrap(),
//             "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"null\",\"name\":\"null\",\"year\":\"null\"}}"
//         );
//         assert_eq!(r1.get_access_history(), vec![]);
//         assert_eq!(
//             row.get_access_history(),
//             vec![Access::Read("t1".to_string())]
//         );

//         assert_eq!(format!("{:?}", row.prev_fields), "None");

//         // write
//         let w1 = row.set_values(&columns, &values, "sgt", "t2").unwrap();
//         assert_eq!(w1.get_values(), None);
//         assert_eq!(
//             w1.get_access_history(),
//             vec![Access::Read("t1".to_string())]
//         );
//         assert_eq!(
//             row.get_access_history(),
//             vec![
//                 Access::Read("t1".to_string()),
//                 Access::Write("t2".to_string())
//             ]
//         );

//         assert_eq!(
//             format!("{:?}", row.prev_fields),
//             "Some([Field { data: Null }, Field { data: Null }, Field { data: Null }])"
//         );

//         // read
//         let r2 = row.get_values(&columns, "sgt", "t3").unwrap();
//         assert_eq!(
//             datatype::to_result(
//                 None,
//                 None,
//                 None,
//                 Some(&columns),
//                 Some(&r2.get_values().unwrap())
//             )
//                 .unwrap(),
//             "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"amount\":\"53.2\",\"name\":\"el camino\",\"year\":\"2019\"}}"
//         );
//         assert_eq!(
//             r2.get_access_history(),
//             vec![
//                 Access::Read("t1".to_string()),
//                 Access::Write("t2".to_string())
//             ]
//         );
//         assert_eq!(
//             row.get_access_history(),
//             vec![
//                 Access::Read("t1".to_string()),
//                 Access::Write("t2".to_string()),
//                 Access::Read("t3".to_string())
//             ]
//         );

//         // commit
//         row.commit("sgt", "t2");
//         assert_eq!(format!("{:?}", row.prev_fields), "None");
//         assert_eq!(
//             row.get_access_history(),
//             vec![Access::Read("t3".to_string())]
//         );

//         // write
//         let values2 = vec!["ford", "2005", "78.2"];
//         let w2 = row.set_values(&columns, &values2, "sgt", "t4").unwrap();
//         assert_eq!(
//             w2.get_access_history(),
//             vec![Access::Read("t3".to_string())]
//         );
//         assert_eq!(
//             row.get_access_history(),
//             vec![
//                 Access::Read("t3".to_string()),
//                 Access::Write("t4".to_string())
//             ]
//         );

//         assert_eq!(
//             format!("{:?}", row.prev_fields),
//             "Some([Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }])"
//         );
//         assert_eq!(
//                     format!("{:?}", row.current_fields),
//             "[Field { data: VarChar(\"ford\") }, Field { data: Int(2005) }, Field { data: Double(78.2) }]"
//         );

//         // revert
//         row.revert("sgt", "t4");
//         assert_eq!(format!("{:?}", row.prev_fields), "None");
//         assert_eq!(
//                     format!("{:?}", row.current_fields),
//             "[Field { data: VarChar(\"el camino\") }, Field { data: Int(2019) }, Field { data: Double(53.2) }]"
//         );

//         assert_eq!(
//             row.access_history,
//             Some(vec![Access::Read("t3".to_string()),])
//         );

//         assert_eq!(
//             format!("{}", row),
//             "[0, None, Clean, cars, el camino, 2019, 53.2, Some([Read(\"t3\")]), []]"
//         );
//     }

//     #[test]
//     fn row_edge_cases_test() {
//         // create table schema
//         let mut catalog = Catalog::init("cars", 1);
//         catalog.add_column(("name", "string")).unwrap();
//         catalog.add_column(("year", "int")).unwrap();
//         catalog.add_column(("amount", "double")).unwrap();
//         // create table
//         let table = Arc::new(Table::init(catalog));
//         // create row in table
//         let mut row = Row::new(Arc::clone(&table), "sgt");

//         assert_eq!(row.get_table().get_table_id(), 1);
//         assert_eq!(row.get_row_id(), 0);

//         let columns = vec!["name", "year", "amount"];
//         let values = vec!["el camino", "2019", "53.2"];

//         // writes
//         row.set_values(&columns, &values, "sgt", "t1").unwrap();

//         assert_eq!(
//             format!(
//                 "{}",
//                 row.set_values(&columns, &values, "sgt", "t2").unwrap_err()
//             ),
//             "dirty: None in table cars"
//         );

//         // init_value
//         let mut row1 = Row::new(Arc::clone(&table), "sgt");
//         assert_eq!(row1.init_value("name", "jack").unwrap(), ());
//         assert_eq!(row1.init_value("year", "40").unwrap(), ());
//         assert_eq!(row1.init_value("amount", "43.2").unwrap(), ());
//         // row dirty
//         row1.set_values(&columns, &values, "sgt", "t2").unwrap();

//         assert_eq!(
//             format!(
//                 "{}",
//                 row1.set_values(&columns, &values, "sgt", "t2").unwrap_err()
//             ),
//             "dirty: None in table cars"
//         );
//     }
// }
