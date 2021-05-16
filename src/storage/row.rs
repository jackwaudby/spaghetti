use crate::common::error::NonFatalError;
use crate::scheduler::TransactionInfo;
use crate::storage::catalog::ColumnKind;
use crate::storage::datatype::{Data, Field};
use crate::storage::table::Table;
use crate::workloads::PrimaryKey;

use std::fmt;
use std::sync::Arc;

/// Represents a row in the database.
#[derive(Debug)]
pub struct Row {
    /// Primary key.
    primary_key: PrimaryKey,

    /// Handle to table row belongs to.
    table: Arc<Table>,

    /// Current version of fields.
    current_fields: Vec<Field>,

    /// Previous version of fields.
    prev_fields: Option<Vec<Field>>,

    /// Row state.
    state: State,
}

/// Represents the state of a row.
#[derive(Debug, Clone, PartialEq)]
pub enum State {
    Clean,
    Modified,
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
}

impl Row {
    /// Return a row with null fields.
    pub fn new(primary_key: PrimaryKey, table: Arc<Table>) -> Self {
        let fields = table.get_schema().column_cnt();
        let mut current_fields = Vec::with_capacity(fields);
        for _ in 0..fields {
            current_fields.push(Field::new());
        }

        Row {
            primary_key,
            table,
            current_fields,
            prev_fields: None,
            state: State::Clean,
        }
    }

    /// Returns a row's primary key.
    pub fn get_primary_key(&self) -> PrimaryKey {
        self.primary_key.clone()
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
    pub fn get_values(&mut self, columns: &[&str]) -> Result<OperationResult, NonFatalError> {
        let mut values = Vec::with_capacity(columns.len());
        let schema = self.table.get_schema();
        for column in columns {
            let field_index = schema.column_position_by_name(column)?;
            let field = &self.current_fields[field_index];
            let value = field.get();
            values.push(value);
        }
        Ok(OperationResult::new(Some(values)))
    }

    /// Append value to list. (list datatype only).
    pub fn append_value(
        &mut self,
        column: &str,
        value: Data,
    ) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => Err(NonFatalError::RowDirty(
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
                self.state = State::Modified; // set state
                let res = OperationResult::new(None); // create return result
                Ok(res)
            }
        }
    }

    /// Set the values of a field in a row.
    pub fn set_values(
        &mut self,
        columns: &[&str],
        values: &[Data],
    ) -> Result<OperationResult, NonFatalError> {
        match self.state {
            State::Modified => Err(NonFatalError::RowDirty(
                self.primary_key.to_string(),
                self.table.get_table_name(),
            )),
            State::Clean => {
                let prev_fields = self.current_fields.clone(); // set prev fields
                self.prev_fields = Some(prev_fields);
                self.state = State::Modified; // set state

                let schema = self.table.get_schema();

                // update each field;
                for (i, col_name) in columns.iter().enumerate() {
                    let field_index = schema.column_position_by_name(col_name)?; // get index of field in row
                    let field_type = schema.column_type_by_index(field_index); // get type of field
                    data_eq_column(field_type, &values[i])?; // check field is list type
                    self.current_fields[field_index].set(values[i].clone());
                }

                let res = OperationResult::new(None); // create return result

                Ok(res)
            }
        }
    }

    /// Make an update permanent.
    pub fn commit(&mut self) {
        self.state = State::Clean;
        self.prev_fields = None;
    }

    /// Revert to previous version of row.
    pub fn revert(&mut self) {
        match self.state {
            State::Modified => {
                self.current_fields = self.prev_fields.take().unwrap(); // revert to old values
                self.state = State::Clean;
            }
            State::Clean => {}
        }
    }

    /// Get row state
    pub fn get_state(&self) -> State {
        self.state.clone()
    }
}

impl OperationResult {
    /// Create new operation result.
    pub fn new(values: Option<Vec<Data>>) -> Self {
        OperationResult { values }
    }

    /// Get values.
    pub fn get_values(&mut self) -> Vec<Data> {
        self.values.take().unwrap()
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

/// Returns true if the access types are the same.
pub fn access_eq(a: &Access, b: &Access) -> bool {
    match (a, b) {
        (&Access::Read(..), &Access::Read(..)) => true,
        (&Access::Write(..), &Access::Write(..)) => true,
        _ => false,
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            State::Clean => write!(f, "clean"),
            State::Modified => write!(f, "dirty"),
        }
    }
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Access::*;
        match &self {
            Read(id) => write!(f, "r-{}", id),
            Write(id) => write!(f, "w-{}", id),
        }
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // fields
        let fc = self.current_fields.len();
        let mut fields = String::new();
        for field in &self.current_fields[0..fc - 1] {
            fields.push_str(format!("{}, ", field).as_str());
        }
        let last = &self.current_fields[fc - 1];
        fields.push_str(format!("{}", last).as_str());

        write!(
            f,
            "[table: {}, pk: {}, state: {}, fields: [{}]",
            self.table.get_table_name(),
            self.get_primary_key(),
            self.state,
            fields,
        )
    }
}
