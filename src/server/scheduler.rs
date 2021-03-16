use crate::common::error::NonFatalError;
use crate::server::scheduler::hit_list::HitList;
use crate::server::scheduler::serialization_graph_testing::SerializationGraphTesting;
use crate::server::scheduler::two_phase_locking::TwoPhaseLocking;
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::table::Table;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use std::sync::Arc;

pub mod hit_list;

pub mod two_phase_locking;

pub mod serialization_graph_testing;

pub struct Protocol {
    pub scheduler: Box<dyn Scheduler + Send + Sync + 'static>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TransactionInfo {
    // Transaction ID.
    id: Option<String>,
    // Timestamp used in deadlock detection.
    ts: Option<u64>,
}

impl Protocol {
    pub fn new(workload: Arc<Workload>, cores: usize) -> crate::Result<Protocol> {
        // Determine workload type.
        let scheduler = match workload
            .get_internals()
            .get_config()
            .get_str("protocol")?
            .as_str()
        {
            "2pl" => Protocol {
                scheduler: Box::new(TwoPhaseLocking::new(Arc::clone(&workload))),
            },
            "sgt" => Protocol {
                scheduler: Box::new(SerializationGraphTesting::new(
                    cores as u32,
                    Arc::clone(&workload),
                )),
            },
            "hit" => Protocol {
                scheduler: Box::new(HitList::new(Arc::clone(&workload))),
            },
            _ => panic!("Incorrect concurrency control protocol"),
        };
        Ok(scheduler)
    }
}

impl TransactionInfo {
    pub fn new(id: Option<String>, ts: Option<u64>) -> TransactionInfo {
        TransactionInfo { id, ts }
    }

    pub fn get_id(&self) -> Option<String> {
        self.id.clone()
    }
    pub fn get_ts(&self) -> Option<u64> {
        self.ts.clone()
    }
}

pub trait Scheduler {
    /// Register a transaction with the scheduler.
    fn register(&self) -> Result<TransactionInfo, NonFatalError>;

    /// Attempt to commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError>;

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
    ) -> Result<(), NonFatalError>;

    /// Read some values from a row.
    fn read(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError>;

    /// Update columns with values in a row.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError>;

    /// Delete a row from a table.
    fn delete(
        &self,
        table: &str,
        key: PrimaryKey,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError>;

    /// Get atomic shared reference to underlying data.
    fn get_data(&self) -> Arc<Workload>;

    /// Get shared reference to a table.
    fn get_table(&self, table: &str, meta: TransactionInfo) -> Result<Arc<Table>, NonFatalError> {
        // Get table.
        let res = self.get_data().get_internals().get_table(table);
        match res {
            Ok(table) => Ok(table),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(e)
            }
        }
    }

    /// Get primary index name on a table.
    fn get_index_name(
        &self,
        table: Arc<Table>,
        meta: TransactionInfo,
    ) -> Result<String, NonFatalError> {
        let res = table.get_primary_index();
        match res {
            Ok(index_name) => Ok(index_name),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(e)
            }
        }
    }

    /// Get shared reference to index for a table.
    fn get_index(
        &self,
        table: Arc<Table>,
        meta: TransactionInfo,
    ) -> Result<Arc<Index>, NonFatalError> {
        // Get index name.
        let index_name = self.get_index_name(table, meta.clone())?;

        // Get index for this key's table.
        let res = self.get_data().get_internals().get_index(&index_name);
        match res {
            Ok(index) => Ok(index),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(e)
            }
        }
    }
}
