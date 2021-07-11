use crate::common::error::NonFatalError;
use crate::storage::datatype::Data;
use crate::workloads::acid::paramgen::AcidTransactionProfile;
use crate::workloads::acid::AcidTransaction;
use crate::workloads::smallbank::paramgen::SmallBankTransactionProfile;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::IsolationLevel;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Message {
    Request {
        request_no: u32,
        transaction: Transaction,
        parameters: Parameters,
        isolation: IsolationLevel,
    },

    Response {
        request_no: u32,
        transaction: Transaction,
        isolation: IsolationLevel,
        outcome: Outcome,
    },
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Transaction {
    Acid(AcidTransaction),
    Tatp,
    SmallBank(SmallBankTransaction),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Parameters {
    Acid(AcidTransactionProfile),
    Tatp,
    SmallBank(SmallBankTransactionProfile),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Outcome {
    Committed(Success),
    Aborted(NonFatalError),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Success {
    created: Option<Vec<(usize, usize)>>,
    read: Option<BTreeMap<String, String>>,
    updated: Option<Vec<(usize, usize)>>,
    deleted: Option<Vec<(usize, usize)>>,
}

impl Success {
    pub fn new(
        created: Option<Vec<(usize, usize)>>,
        updated: Option<Vec<(usize, usize)>>,
        deleted: Option<Vec<(usize, usize)>>,
        columns: Option<&[&str]>,
        values: Option<&Vec<Data>>,
    ) -> Self {
        let mut read;
        if let Some(cols) = columns {
            read = Some(BTreeMap::new());

            for (i, column) in cols.iter().enumerate() {
                let key = column.to_string();
                let val = format!("{}", values.unwrap()[i]);
                read.as_mut().unwrap().insert(key, val);
            }
        } else {
            read = None;
        }

        Self {
            created,
            updated,
            deleted,
            read,
        }
    }

    pub fn get_values(&self) -> Option<&BTreeMap<String, String>> {
        self.read.as_ref()
    }

    pub fn get_updated(&self) -> Option<&Vec<(usize, usize)>> {
        self.updated.as_ref()
    }
}

//   let res = serde_json::to_string(&sm).unwrap();

// impl fmt::Display for Message {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         use Message::*;
//         match *self {
//             Response {
//                 request_no,
//                 ref outcome,
//                 ..
//             } => write!(f, "[id=\"{}\",{}]", request_no, outcome),
//             _ => write!(f, "{:?}", self),
//         }
//     }
// }

// impl fmt::Display for Outcome {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         use Outcome::*;
//         match &*self {
//             Committed { value } => write!(f, "{}", value.as_ref().unwrap()),
//             Aborted { reason } => write!(f, "val={{{}}}", reason),
//         }
//     }
// }o
