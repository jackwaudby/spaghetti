use crate::server::scheduler::two_phase_locking::TwoPhaseLocking;
use crate::server::storage::datatype::Data;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;
use crate::Result;

use chrono::{DateTime, Utc};
use std::sync::Arc;

pub mod two_phase_locking;

pub mod serialization_graph_testing;

pub struct Protocol {
    pub scheduler: Box<dyn Scheduler + Send + Sync + 'static>,
}

impl Protocol {
    pub fn new(w: Arc<Workload>) -> Result<Protocol> {
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
    fn register(&self, transaction_name: &str) -> Result<()>;

    fn commit(&self, transaction_name: &str) -> Result<()>;

    fn abort(&self, transaction_name: &str) -> Result<()>;

    fn insert(
        &self,
        table: &str,
        pk: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        transaction_name: &str,
    ) -> Result<()>;

    fn read(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<Vec<Data>>;

    fn write(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<()>;

    fn delete(&self, index: &str, pk: PrimaryKey, transaction_name: &str) -> Result<()>;
}
