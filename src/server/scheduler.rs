use crate::server::scheduler::serialization_graph_testing::SerializationGraphTesting;
use crate::server::scheduler::two_phase_locking::TwoPhaseLocking;
use crate::server::storage::datatype::Data;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use chrono::{DateTime, Utc};
use std::fmt;
use std::sync::Arc;

pub mod two_phase_locking;

pub mod serialization_graph_testing;

pub struct Protocol {
    pub scheduler: Box<dyn Scheduler + Send + Sync + 'static>,
}

#[derive(Clone)]
pub struct TransactionInfo {
    // Transaction ID.
    id: Option<String>,
    // Timestamp used in deadlock detection.
    ts: Option<DateTime<Utc>>,
}

impl Protocol {
    pub fn new(workload: Arc<Workload>, cores: usize) -> crate::Result<Protocol> {
        // Determine workload type.
        let scheduler = match workload
            .get_internals()
            .config
            .get_str("protocol")?
            .as_str()
        {
            "2pl" => Protocol {
                scheduler: Box::new(TwoPhaseLocking::new(Arc::clone(&workload))),
            },
            "sgt" => Protocol {
                scheduler: Box::new(SerializationGraphTesting::new(
                    cores as i32,
                    Arc::clone(&workload),
                )),
            },
            _ => panic!("Incorrect concurrency control protocol"),
        };
        Ok(scheduler)
    }
}

impl TransactionInfo {
    pub fn new(id: Option<String>, ts: Option<DateTime<Utc>>) -> TransactionInfo {
        TransactionInfo { id, ts }
    }

    pub fn get_id(&self) -> Option<String> {
        self.id.clone()
    }
    pub fn get_ts(&self) -> Option<DateTime<Utc>> {
        self.ts.clone()
    }
}

pub trait Scheduler {
    /// Register a transaction with the scheduler.
    fn register(&self) -> Result<TransactionInfo, Aborted>;

    /// Attempt to commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), Aborted>;

    /// Abort a transaction.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()>;

    /// Insert a new row in a table.
    fn create(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<(), Aborted>;

    /// Read some values from a row.
    fn read(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<Vec<Data>, Aborted>;

    /// Update columns with values in a row.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<(), Aborted>;

    /// Delete a row from a table.
    fn delete(&self, table: &str, key: PrimaryKey, meta: TransactionInfo) -> Result<(), Aborted>;
}

#[derive(PartialEq, Debug, Clone)]
pub struct Aborted {
    pub reason: String,
}

impl fmt::Display for Aborted {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Aborted: {}", self.reason)
    }
}

impl std::error::Error for Aborted {}
