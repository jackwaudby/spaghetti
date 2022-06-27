use crate::common::error::NonFatalError;
use crate::common::stored_procedure_result::StoredProcedureResult;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
// use crate::workloads::acid::paramgen::AcidTransactionProfile;
// use crate::workloads::acid::AcidTransaction;
// use crate::workloads::dummy::paramgen::DummyTransactionProfile;
// use crate::workloads::dummy::DummyTransaction;
use crate::workloads::smallbank::paramgen::SmallBankTransactionProfile;
use crate::workloads::smallbank::SmallBankTransaction;
// use crate::workloads::tatp::paramgen::TatpTransactionProfile;
// use crate::workloads::tatp::TatpTransaction;
// use crate::workloads::ycsb::paramgen::YcsbTransactionProfile;
// use crate::workloads::ycsb::YcsbTransaction;
use crate::workloads::IsolationLevel;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashSet;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Request {
    request_no: (u32, u32),
    transaction: Transaction,
    parameters: Parameters,
    isolation: IsolationLevel,
}

impl Request {
    pub fn new(
        request_no: (u32, u32),
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

    pub fn get_request_no(&self) -> (u32, u32) {
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
}

impl Response {
    pub fn new(
        request_no: (u32, u32),
        transaction: Transaction,
        result: StoredProcedureResult,
    ) -> Self {
        Self {
            request_no,
            transaction,
            result,
        }
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

    pub fn set_total_latency(&mut self, dur: u128) {
        self.result.set_total_latency(dur);
    }

    pub fn get_transaction_id(&self) -> u64 {
        self.result.get_internal_id()
    }
    pub fn get_problem_transactions(&self) -> HashSet<u64> {
        self.result.get_problem_transactions()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Transaction {
    // Dummy(DummyTransaction),
    // Acid(AcidTransaction),
    // Tatp(TatpTransaction),
    SmallBank(SmallBankTransaction),
    // Ycsb(YcsbTransaction),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Parameters {
    // Dummy(DummyTransactionProfile),
    // Acid(AcidTransactionProfile),
    // Tatp(TatpTransactionProfile),
    SmallBank(SmallBankTransactionProfile),
    // Ycsb(YcsbTransactionProfile),
}

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
