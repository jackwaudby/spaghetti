use crate::common::error::NonFatalError;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;

/// Element of a `Row' that holds `Data`.
#[derive(Debug, Clone)]
pub struct Field {
    data: Data,
}

/// Represents spaghetti's fundamental datatype.
#[derive(Debug, Clone, PartialEq)]
pub enum Data {
    Int(i64),
    VarChar(String),
    Double(f64),
    Null,
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

impl fmt::Display for Field {
    /// Format: value.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl fmt::Display for Data {
    /// Format: value.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Data::Int(val) => write!(f, "{}", val.to_string()),
            Data::VarChar(ref val) => write!(f, "{}", val),
            Data::Double(val) => write!(f, "{}", val.to_string()),
            Data::Null => write!(f, "null"),
        }
    }
}

impl TryFrom<Data> for i64 {
    type Error = NonFatalError;

    fn try_from(value: Data) -> Result<Self, Self::Error> {
        if let Data::Int(int) = value {
            Ok(int)
        } else {
            Err(NonFatalError::UnableToConvertFromDataType(
                value.to_string(),
                "i64".to_string(),
            ))
        }
    }
}

impl TryFrom<Data> for f64 {
    type Error = NonFatalError;

    fn try_from(value: Data) -> Result<Self, Self::Error> {
        if let Data::Double(int) = value {
            Ok(int)
        } else {
            Err(NonFatalError::UnableToConvertFromDataType(
                value.to_string(),
                "f64".to_string(),
            ))
        }
    }
}

impl TryFrom<Data> for String {
    type Error = NonFatalError;

    fn try_from(value: Data) -> Result<Self, Self::Error> {
        if let Data::VarChar(int) = value {
            Ok(int)
        } else {
            Err(NonFatalError::UnableToConvertFromDataType(
                value.to_string(),
                "string".to_string(),
            ))
        }
    }
}

/// Convert columns and values to a result string.
pub fn to_result(
    created: Option<u64>,
    updated: Option<u64>,
    deleted: Option<u64>,
    columns: Option<&Vec<&str>>,
    values: Option<&Vec<Data>>,
) -> crate::Result<String> {
    let mut vals;
    if let Some(_) = columns {
        vals = Some(BTreeMap::new());

        for (i, column) in columns.unwrap().iter().enumerate() {
            let key = format!("{}", column);
            let val = format!("{}", values.unwrap()[i]);
            vals.as_mut().unwrap().insert(key, val);
        }
    } else {
        vals = None;
    }
    let sm = SuccessMessage::new(created, updated, deleted, vals);

    let res = serde_json::to_string(&sm).unwrap();

    Ok(res)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SuccessMessage {
    created: Option<u64>,
    updated: Option<u64>,
    deleted: Option<u64>,
    val: Option<BTreeMap<String, String>>,
}

impl SuccessMessage {
    /// Create new success message.
    fn new(
        created: Option<u64>,
        updated: Option<u64>,
        deleted: Option<u64>,
        val: Option<BTreeMap<String, String>>,
    ) -> Self {
        SuccessMessage {
            created,
            updated,
            deleted,
            val,
        }
    }

    /// Get values
    pub fn get_values(&self) -> &BTreeMap<String, String> {
        &self.val.as_ref().unwrap()
    }
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

        f.set(Data::Double(1.7));
        assert_eq!(f.get(), Data::Double(1.7));
        assert_eq!(format!("{}", f), String::from("1.7"));

        // Conversion success
        assert_eq!(i64::try_from(Data::Int(5)), Ok(5));
        assert_eq!(f64::try_from(Data::Double(5.5)), Ok(5.5));
        assert_eq!(
            String::try_from(Data::VarChar("test".to_string())),
            Ok("test".to_string())
        );

        // Conversion failure
        assert_eq!(
            i64::try_from(Data::Double(1.6)),
            Err(NonFatalError::UnableToConvertFromDataType(
                "1.6".to_string(),
                "i64".to_string()
            ))
        );
        assert_eq!(
            f64::try_from(Data::Int(1)),
            Err(NonFatalError::UnableToConvertFromDataType(
                "1".to_string(),
                "f64".to_string()
            ))
        );
        assert_eq!(
            String::try_from(Data::Int(1)),
            Err(NonFatalError::UnableToConvertFromDataType(
                "1".to_string(),
                "string".to_string()
            ))
        );
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
        assert_eq!(to_result(None, None, None, Some(&columns), Some(&values)).unwrap(),"{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"a\":\"1.3\",\"b\":\"null\",\"c\":\"10\",\"d\":\"hello\"}}");
    }
}
