use crate::Result;
use std::fmt::{self, Write};

/// Element of a `Row' that holds `Data`.
#[derive(Debug)]
pub struct Field {
    data: Data,
}

impl Field {
    /// Create a new instance of `Field`.
    pub fn new() -> Self {
        Field { data: Data::Null }
    }

    /// Return the `Data` stored in a `Field`.
    pub fn get(&self) -> Data {
        self.data.clone()
    }

    /// Set the `Data` stored in a `Field`
    pub fn set(&mut self, data: Data) {
        self.data = data;
    }
}

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Data::Int(val) => write!(f, "{}", val.to_string()),
            Data::VarChar(ref val) => write!(f, "{}", val),
            Data::Double(val) => write!(f, "{}", val.to_string()),
            Data::Null => write!(f, "null"),
        }
    }
}

// value
impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.data {
            Data::Int(val) => write!(f, "{}", val.to_string()),
            Data::VarChar(ref val) => write!(f, "{}", val),
            Data::Double(val) => write!(f, "{}", val.to_string()),
            Data::Null => write!(f, "null"),
        }
    }
}

/// Represents fundamental datatype.
#[derive(Debug, Clone, PartialEq)]
pub enum Data {
    Int(i64),
    VarChar(String),
    Double(f64),
    Null,
}

/// Convert columns and values to a result string.
pub fn to_result(columns: &Vec<&str>, values: &Vec<Data>) -> Result<String> {
    let mut res: String;
    res = "[".to_string();
    for (i, column) in columns.iter().enumerate() {
        write!(res, "{}={}, ", column, values[i])?;
    }
    res.truncate(res.len() - 2);
    write!(res, "]")?;
    Ok(res)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn fields_test() {
        let mut f = Field::new();
        assert_eq!(f.get(), Data::Null);
        assert_eq!(format!("{}", f), String::from("null"));
        f.set(Data::Int(5));
        assert_eq!(f.get(), Data::Int(5));
        assert_eq!(format!("{}", f), String::from("5"));
        f.set(Data::VarChar("abc".to_string()));
        assert_eq!(f.get(), Data::VarChar("abc".to_string()));
        assert_eq!(format!("{}", f), String::from("abc"));
    }

    #[test]
    fn to_result_test() {
        let columns = vec!["a", "b", "c", "d"];
        let values = vec![
            Data::Double(1.3),
            Data::Null,
            Data::Int(10),
            Data::VarChar("hello".to_string()),
        ];
        assert_eq!(
            to_result(&columns, &values).unwrap(),
            "[a=1.3, b=null, c=10, d=hello]"
        );
    }
}
