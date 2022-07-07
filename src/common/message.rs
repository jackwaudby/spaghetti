use crate::common::error::NonFatalError;
use crate::common::isolation_level::IsolationLevel;
use crate::common::stored_procedure_result::StoredProcedureResult;
use crate::storage::{access::TransactionId, datatype::Data};
use crate::workloads::smallbank::{paramgen::SmallBankTransactionProfile, SmallBankTransaction};
use crate::workloads::ycsb::paramgen::YcsbTransactionProfile;
use crate::workloads::ycsb::YcsbTransaction;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Request {
    request_no: (usize, u32),
    transaction: Transaction,
    parameters: Parameters,
    isolation: IsolationLevel,
}

impl Request {
    pub fn new(
        request_no: (usize, u32),
        transaction: Transaction,
        parameters: Parameters,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            request_no,
            transaction,
            parameters,
            isolation,
        }
    }

    pub fn get_request_no(&self) -> (usize, u32) {
        self.request_no
    }

    pub fn get_transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub fn get_parameters(&self) -> &Parameters {
        &self.parameters
    }

    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Response {
    request_no: (u32, u32),
    transaction: Transaction,
    result: StoredProcedureResult,
    retries: u64,
}

impl Response {
    pub fn new(
        request_no: (u32, u32),
        transaction: Transaction,
        result: StoredProcedureResult,
        retries: u64,
    ) -> Self {
        Self {
            request_no,
            transaction,
            result,
            retries,
        }
    }

    pub fn set_retries(&mut self, r: u64) {
        self.retries = r
    }

    pub fn get_request_no(&self) -> (u32, u32) {
        self.request_no
    }

    pub fn get_transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub fn get_isolation_level(&self) -> &IsolationLevel {
        &self.result.get_isolation_level()
    }

    pub fn get_outcome(&self) -> &Outcome {
        &self.result.get_outcome()
    }

    pub fn get_result(&self) -> &StoredProcedureResult {
        &self.result
    }

    pub fn get_transaction_id(&self) -> u64 {
        self.result.get_internal_id()
    }
    pub fn get_problem_transactions(&self) -> HashSet<usize> {
        self.result.get_problem_transactions()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Transaction {
    // Tatp(TatpTransaction),
    SmallBank(SmallBankTransaction),
    Ycsb(YcsbTransaction),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Parameters {
    // Tatp(TatpTransactionProfile),
    SmallBank(SmallBankTransactionProfile),
    Ycsb(YcsbTransactionProfile),
}

// impl Parameters {
//     pub fn new(profile: SmallBankTransactionProfile) -> Self {
//         Self(profile)
//     }
//     pub fn get(&self) -> &SmallBankTransactionProfile {
//         &self.0
//     }
// }

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Outcome {
    Committed(Success),
    Aborted(NonFatalError),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Success {
    id: TransactionId,
    read: Option<BTreeMap<String, String>>,
    updated: Option<Vec<(usize, usize)>>,
}

impl Success {
    pub fn default(id: TransactionId) -> Self {
        Self {
            id,
            updated: None,
            read: None,
        }
    }

    pub fn new(
        id: TransactionId,
        updated: Option<Vec<(usize, usize)>>,
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

        Self { id, updated, read }
    }

    pub fn get_values(&self) -> Option<&BTreeMap<String, String>> {
        self.read.as_ref()
    }

    pub fn get_updated(&self) -> Option<&Vec<(usize, usize)>> {
        self.updated.as_ref()
    }
}
