use crate::common::error::NonFatalError;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::From;
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
    Uint(u64),
    Int(i64),
    VarChar(String),
    Double(f64),
    List(Vec<Data>),
    Null,
}

impl Default for Field {
    fn default() -> Self {
        Self::new()
    }
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

    /// Append data to list.
    pub fn append(&mut self, data: Data) {
        if let Data::List(ref mut list) = &mut self.data {
            list.push(data);
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Data::Uint(val) => write!(f, "{}", val.to_string()),
            Data::Int(val) => write!(f, "{}", val.to_string()),
            Data::VarChar(ref val) => write!(f, "{}", val),
            Data::Double(val) => write!(f, "{}", val.to_string()),
            Data::List(vec) => {
                let mut res = String::new();

                let size = vec.len();
                res.push_str("[");
                for e in vec[0..size - 1].iter() {
                    res.push_str(&e.to_string());
                    res.push_str(", ");
                }
                res.push_str(&vec[size - 1].to_string());
                res.push_str("]");
                write!(f, "{}", res)
            }
            Data::Null => write!(f, "null"),
        }
    }
}

impl From<f64> for Data {
    fn from(item: f64) -> Self {
        Data::Double(item)
    }
}

impl From<u64> for Data {
    fn from(item: u64) -> Self {
        Data::Uint(item)
    }
}

impl From<i64> for Data {
    fn from(item: i64) -> Self {
        Data::Int(item)
    }
}

impl From<String> for Data {
    fn from(item: String) -> Self {
        Data::VarChar(item)
    }
}

impl TryFrom<Data> for u64 {
    type Error = NonFatalError;

    fn try_from(value: Data) -> Result<Self, Self::Error> {
        if let Data::Uint(int) = value {
            Ok(int)
        } else {
            Err(NonFatalError::UnableToConvertFromDataType(
                value.to_string(),
                "u64".to_string(),
            ))
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
    id: Option<u64>,
    created: Option<u64>,
    updated: Option<Vec<(usize, usize)>>,
    deleted: Option<u64>,
    columns: Option<&[&str]>,
    values: Option<&Vec<Data>>,
) -> crate::Result<String> {
    let mut vals;
    if let Some(cols) = columns {
        vals = Some(BTreeMap::new());

        for (i, column) in cols.iter().enumerate() {
            let key = column.to_string();
            let val = format!("{}", values.unwrap()[i]);
            vals.as_mut().unwrap().insert(key, val);
        }
    } else {
        vals = None;
    }
    let sm = SuccessMessage::new(id, created, updated, deleted, vals);

    let res = serde_json::to_string(&sm).unwrap();

    Ok(res)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SuccessMessage {
    id: Option<u64>,
    created: Option<u64>,
    updated: Option<Vec<(usize, usize)>>,
    deleted: Option<u64>,
    val: Option<BTreeMap<String, String>>,
}

impl SuccessMessage {
    fn new(
        id: Option<u64>,
        created: Option<u64>,
        updated: Option<Vec<(usize, usize)>>,
        deleted: Option<u64>,
        val: Option<BTreeMap<String, String>>,
    ) -> Self {
        SuccessMessage {
            id,
            created,
            updated,
            deleted,
            val,
        }
    }

    pub fn get_values(&self) -> Option<BTreeMap<String, String>> {
        self.val.clone()
    }

    pub fn get_updated(&self) -> Option<Vec<(usize, usize)>> {
        self.updated.clone()
    }
}
