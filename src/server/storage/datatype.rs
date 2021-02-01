//! A `Field` contains `Data` and is stored in a `Row`.
use std::fmt;

#[derive(Debug)]
pub struct Field {
    data: Option<Data>,
}

impl Field {
    /// Create a new instance of `Field`.
    pub fn new() -> Self {
        Field { data: None }
    }

    /// Return the (optional) `Data` stored in a `Field`.
    ///
    /// All data types are converted to a String.
    pub fn get(&self) -> String {
        match &self.data {
            Some(value) => match value {
                Data::Int(val) => val.to_string(),
                Data::VarChar(ref val) => val.clone(),
                Data::Double(val) => val.to_string(),
            },
            None => "null".to_string(),
        }
    }

    /// Set the `Data` stored in a `Field`
    pub fn set(&mut self, data: Data) {
        self.data = Some(data);
    }
}

// value
impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.data {
            Some(value) => match value {
                Data::Int(val) => write!(f, "{}", val.to_string()),
                Data::VarChar(ref val) => write!(f, "{}", val),
                Data::Double(val) => write!(f, "{}", val.to_string()),
            },
            None => write!(f, "null"),
        }
    }
}

#[derive(Debug)]
pub enum Data {
    Int(i64),
    VarChar(String),
    Double(f64),
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn fields() {
        let mut f = Field::new();
        assert_eq!(f.get(), "null".to_string());
        assert_eq!(format!("{}", f), String::from("null"));
        f.set(Data::Int(5));
        assert_eq!(f.get(), "5".to_string());
        assert_eq!(format!("{}", f), String::from("5"));
        f.set(Data::VarChar("abc".to_string()));
        assert_eq!(f.get(), "abc".to_string());
        assert_eq!(format!("{}", f), String::from("abc"));
    }
}
