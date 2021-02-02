//! A `Catalog` contains the schema of a `Table`.

use crate::common::error::SpaghettiError;
use crate::Result;
use std::fmt;

#[derive(Debug)]
pub struct Catalog {
    table_name: String,
    table_id: u64,
    column_cnt: u64,
    columns: Vec<Column>,
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

    /// Adds a column (name, type) to the schema
    pub fn add_column(&mut self, col: (&str, &str)) {
        let col_type = match col.1 {
            "int" | "int64_t" | "uint64_t" => ColumnKind::Int,
            "string" => ColumnKind::VarChar,
            "double" => ColumnKind::Double,
            _ => panic!("invalid column type {}", col.1),
        };

        let column = Column::new(col.0, col_type);
        self.column_cnt += 1;
        self.columns.push(column);
    }

    /// Given a column name returns the column position.
    pub fn column_position_by_name(&self, name: &str) -> Result<usize> {
        match self.columns.iter().position(|x| *x.name() == name) {
            Some(pos) => Ok(pos),
            None => Err(Box::new(SpaghettiError::ColumnNotFound(name.to_string()))),
        }
    }

    /// Given a column name returns the column type.
    pub fn column_type_by_name(&self, col_name: &str) -> Result<&ColumnKind> {
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
    pub fn column_cnt(&self) -> u64 {
        self.column_cnt
    }
}

// [table_name,table_id,(col_name,col_type),...]
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

#[derive(Debug)]
pub struct Column {
    name: String,
    kind: ColumnKind,
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

// (name,kind)
impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.name, self.kind)
    }
}

#[derive(Debug, PartialEq)]
pub enum ColumnKind {
    Int,
    VarChar,
    Double,
}

// map kind to string type
impl fmt::Display for ColumnKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let printable = match *self {
            ColumnKind::Int => "int",
            ColumnKind::VarChar => "varchar",
            ColumnKind::Double => "double",
        };
        write!(f, "{}", printable)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::common::error::SpaghettiError;

    #[test]
    fn catalog_test() {
        // Create column.
        let int = Column::new("id", ColumnKind::Int);

        assert_eq!(int.name(), &String::from("id"));
        assert_eq!(int.kind(), &ColumnKind::Int);

        // Create schema.x
        let mut catalog = Catalog::init("products", 1);
        catalog.add_column(("id", "int"));
        catalog.add_column(("price", "double"));
        catalog.add_column(("desc", "string"));

        assert_eq!(catalog.table_id(), 1);
        assert_eq!(catalog.table_name(), &String::from("products"));

        assert_eq!(catalog.column_cnt(), 3);
        assert_eq!(catalog.column_position_by_name("price").unwrap(), 1);
        assert_eq!(
            *catalog
                .column_position_by_name("location")
                .unwrap_err()
                .downcast::<SpaghettiError>()
                .unwrap(),
            SpaghettiError::ColumnNotFound("location".to_string())
        );

        assert_eq!(
            catalog.column_type_by_name("desc").unwrap(),
            &ColumnKind::VarChar
        );
        assert_eq!(catalog.column_type_by_index(2), &ColumnKind::VarChar);
        assert_eq!(catalog.column_name_by_index(2), "desc".to_string());
    }
}
