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

impl Protocol {
    pub fn new(w: Arc<Workload>) -> crate::Result<Protocol> {
        // Determine workload type.
        match w.get_internals().config.get_str("protocol")?.as_str() {
            "2pl" => {
                // Create scheduler
                let scheduler = Box::new(TwoPhaseLocking::new(Arc::clone(&w)));
                let protocol = Protocol { scheduler };

                Ok(protocol)
            }

            _ => panic!("Incorrect concurrency control protocol"),
        }
    }
}

pub trait Scheduler {
    /// Register a transaction with the scheduler.
    fn register(&self, tid: &str) -> Result<(), Aborted>;

    /// Attempt to commit a transaction.
    fn commit(&self, tid: &str) -> Result<(), Aborted>;

    /// Abort a transaction.
    fn abort(&self, tid: &str) -> crate::Result<()>;

    /// Insert a new row in a table.
    fn create(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        tid: &str,
        tts: DateTime<Utc>,
    ) -> Result<(), Aborted>;

    /// Read some values from a row.
    fn read(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        tid: &str,
        tts: DateTime<Utc>,
    ) -> Result<Vec<Data>, Aborted>;

    /// Update columns with values in a row.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        tid: &str,
        tts: DateTime<Utc>,
    ) -> Result<(), Aborted>;

    /// Delete a row from a table.
    fn delete(
        &self,
        table: &str,
        key: PrimaryKey,
        tid: &str,
        tts: DateTime<Utc>,
    ) -> Result<(), Aborted>;
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
