use crate::common::error::{FatalError, NonFatalError};

use std::fmt;

// A catalog contains the schema of a table.
#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    /// Table name.
    table_name: String,

    /// Table identifier.
    table_id: u64,

    /// Number of columns in table.
    column_cnt: usize,

    /// List of columns.
    columns: Vec<Column>,
}

/// An entry in a catalog's list of columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    name: String,
    kind: ColumnKind,
}

/// Column datatypes.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnKind {
    Int,
    Uint,
    VarChar,
    Double,
    List,
}

impl Catalog {
    /// Initiate a new schema for table.
    pub fn init(table_name: &str, table_id: u64) -> Self {
        Catalog {
            table_name: String::from(table_name),
            table_id,
            column_cnt: 0,
            columns: Vec::new(),
        }
    }

    /// Returns a shared reference to the table's name.
    pub fn table_name(&self) -> &String {
        &self.table_name
    }

    /// Returns the table's id.
    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Adds a column (name, type) to the schema.
    ///
    /// # Fatal Error
    ///
    /// Returns a fatal error if the desired column type is not recognised.
    pub fn add_column(&mut self, col: (&str, &str)) -> Result<(), FatalError> {
        let col_type = match col.1 {
            "uint" => ColumnKind::Uint,
            "int" => ColumnKind::Int,
            "string" => ColumnKind::VarChar,
            "double" => ColumnKind::Double,
            "list" => ColumnKind::List,
            _ => return Err(FatalError::InvalidColumnType(col.1.to_string())),
        };

        let column = Column::new(col.0, col_type);
        self.column_cnt += 1;
        self.columns.push(column);
        Ok(())
    }

    /// Given a column name returns the column position.
    ///
    /// # Non-Fatal Error
    ///
    /// Returns a non-fatal error if the column cannot be found.
    pub fn column_position_by_name(&self, name: &str) -> Result<usize, NonFatalError> {
        match self.columns.iter().position(|x| *x.name() == name) {
            Some(pos) => Ok(pos),
            None => Err(NonFatalError::ColumnNotFound(name.to_string())),
        }
    }

    /// Given a column name returns the column type.
    ///
    /// # Non-Fatal Error
    ///
    /// Returns a non-fatal error if the column cannot be found.
    pub fn column_type_by_name(&self, col_name: &str) -> Result<&ColumnKind, NonFatalError> {
        let pos = self.column_position_by_name(col_name)?;
        let column_type = self.columns[pos].kind();
        Ok(column_type)
    }

    /// Given a column position returns the column type.
    pub fn column_type_by_index(&self, index: usize) -> &ColumnKind {
        self.columns[index].kind()
    }

    /// Given a column name returns the column position.
    pub fn column_name_by_index(&self, index: usize) -> String {
        self.columns[index].name.clone()
    }

    /// Returns the number of columns in the schema.
    pub fn column_cnt(&self) -> usize {
        self.column_cnt
    }
}

/// Format: [table_name,table_id,(col_name,col_type),...]
impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ind = self.columns.len();
        let mut printable = String::new();
        for column in &self.columns[0..ind - 1] {
            printable.push_str(format!("{},", column).as_str());
        }
        let last = &self.columns[ind - 1];
        printable.push_str(format!("{}", last).as_str());

        write!(f, "[{},{},{}]", self.table_name, self.table_id, printable)
    }
}

impl Column {
    /// Create a new column.
    pub fn new(name: &str, kind: ColumnKind) -> Self {
        Column {
            name: String::from(name),
            kind,
        }
    }

    /// Return a shared reference to the column name.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Return a shared reference to the column kind.
    pub fn kind(&self) -> &ColumnKind {
        &self.kind
    }
}

/// Format: (name,kind)
impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.name, self.kind)
    }
}

/// Format: type
impl fmt::Display for ColumnKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let printable = match *self {
            ColumnKind::Uint => "uint",
            ColumnKind::Int => "int",
            ColumnKind::VarChar => "varchar",
            ColumnKind::Double => "double",
            ColumnKind::List => "list",
        };
        write!(f, "{}", printable)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn catalog_test() {
        // Create column.
        let int = Column::new("id", ColumnKind::Int);
        assert_eq!(format!("{}", int), "(id, int)");
        assert_eq!(int.name(), &String::from("id"));
        assert_eq!(int.kind(), &ColumnKind::Int);

        assert_eq!(
            format!("{}", Column::new("name", ColumnKind::VarChar)),
            "(name, varchar)"
        );

        assert_eq!(
            format!("{}", Column::new("ratio", ColumnKind::Double)),
            "(ratio, double)"
        );

        // Create schema.x
        let mut catalog = Catalog::init("products", 1);
        catalog.add_column(("id", "int")).unwrap();
        catalog.add_column(("price", "double")).unwrap();
        catalog.add_column(("desc", "string")).unwrap();

        assert_eq!(
            format!("{}", catalog.add_column(("images", "blob")).unwrap_err()),
            format!("invalid: column type blob")
        );

        assert_eq!(catalog.table_id(), 1);
        assert_eq!(catalog.table_name(), &String::from("products"));

        assert_eq!(catalog.column_cnt(), 3);
        assert_eq!(catalog.column_position_by_name("price").unwrap(), 1);

        assert_eq!(
            catalog.column_type_by_name("desc").unwrap(),
            &ColumnKind::VarChar
        );
        assert_eq!(catalog.column_type_by_index(2), &ColumnKind::VarChar);
        assert_eq!(catalog.column_name_by_index(2), "desc".to_string());

        assert_eq!(
            format!("{}", catalog),
            format!("[products,1,(id, int),(price, double),(desc, varchar)]")
        );
    }
}
