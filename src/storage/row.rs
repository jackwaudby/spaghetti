use crate::common::error::NonFatalError;
use crate::scheduler::TransactionInfo;
use crate::storage::catalog::ColumnKind;
use crate::storage::datatype::{Data, Field};
use crate::storage::table::Table;
use crate::workloads::PrimaryKey;

use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum State {
    Clean,
    Modified,
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

    written_by: Option<TransactionInfo>,

    prev_written_by: Option<TransactionInfo>,

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
            written_by: None,
            prev_written_by: None,
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
            State::Clean => {
                let prev_fields = self.current_fields.clone(); // set prev fields

                self.prev_fields = Some(prev_fields);

                self.prev_written_by = self.written_by.take();

                self.state = State::Modified; // set state
                self.written_by = Some(tid.clone());

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
            State::Modified => {
                self.current_fields = self.prev_fields.take().unwrap(); // revert to old values
                self.state = State::Clean;
                self.written_by = self.prev_written_by.take();
                self.prev_written_by = None;

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
    pub fn append_delayed(&mut self, t: &TransactionInfo) -> TransactionInfo {
        // if there are no delayed transactions the dependency is the last write
        // else it is the last element of the delayed queue
        let dependency;
        if !self.is_delayed() {
            dependency = self.written_by.as_ref().unwrap().clone();
        } else {
            dependency = self
                .delayed
                .as_mut()
                .unwrap()
                .last()
                .clone()
                .unwrap()
                .clone();
        }

        self.delayed.as_mut().unwrap().push(t.clone());
        dependency
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
    pub fn get_values(&mut self) -> Vec<Data> {
        self.values.take().unwrap()
    }

    /// Get access history.
    pub fn get_access_history(&mut self) -> Vec<Access> {
        self.access_history.take().unwrap()
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