use crate::common::error::NonFatalError;
use crate::scheduler::basic_sgt::BasicSerializationGraphTesting;
use crate::scheduler::hit_list::HitList;
use crate::scheduler::opt_hit_list::OptimisedHitList;
use crate::scheduler::serialization_graph_testing::SerializationGraphTesting;
use crate::scheduler::two_phase_locking::TwoPhaseLocking;
use crate::storage::datatype::Data;
use crate::storage::index::Index;
use crate::storage::table::Table;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use std::fmt;
use std::sync::Arc;

pub mod hit_list;

pub mod opt_hit_list;

pub mod two_phase_locking;

pub mod serialization_graph_testing;

pub mod basic_sgt;

/// A concurrency control protocol.
///
/// Uses trait object for dynamic dispatch to switch between protocols.
#[derive(Debug)]
pub struct Protocol {
    /// Trait object pointing to scheduler.
    pub scheduler: Box<dyn Scheduler + Send + Sync + 'static>,
}

#[derive(Debug, Clone, PartialEq)] // TODO: Clone or Copy
pub enum TransactionInfo {
    BasicSerializationGraph { thread_id: usize, txn_id: u64 },
    OptimisticSerializationGraph { thread_id: usize, txn_id: u64 },
    HitList { txn_id: u64 },
    OptimisticHitList { thread_id: usize, txn_id: u64 },
    TwoPhaseLocking { txn_id: String, timestamp: u64 },
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
            "basic-sgt" => Protocol {
                scheduler: Box::new(BasicSerializationGraphTesting::new(
                    cores as u32,
                    Arc::clone(&workload),
                )),
            },
            "hit" => Protocol {
                scheduler: Box::new(HitList::new(Arc::clone(&workload))),
            },
            "opt-hit" => Protocol {
                scheduler: Box::new(OptimisedHitList::new(cores, Arc::clone(&workload))),
            },
            _ => panic!("Incorrect concurrency control protocol"),
        };
        Ok(scheduler)
    }
}

pub trait Scheduler: fmt::Display + fmt::Debug {
    /// Register a transaction with the scheduler.
    fn register(&self) -> Result<TransactionInfo, NonFatalError>;

    /// Attempt to commit a transaction.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError>;

    /// Abort a transaction.
    fn abort(&self, meta: &TransactionInfo) -> crate::Result<()>;

    /// Read some values from a row.
    fn read(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError>;

    /// Update and return old values. (Get and set equivalent).
    fn read_and_update(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError>;

    /// List data type only.
    ///
    /// Append `value` to `column`.
    fn append(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        column: &str,
        value: Data,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError>;

    /// Update columns with values in a row.
    fn update(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        read: bool,
        params: Option<&[Data]>,
        f: &dyn Fn(
            &[&str],           // columns
            Option<Vec<Data>>, // current values
            Option<&[Data]>,   // parameters
        ) -> Result<(Vec<String>, Vec<Data>), NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError>;

    /// Get atomic shared reference to underlying data.
    fn get_data(&self) -> Arc<Workload>;

    /// Get shared reference to a table.
    fn get_table(&self, table: &str, meta: &TransactionInfo) -> Result<Arc<Table>, NonFatalError> {
        let res = self.get_data().get_internals().get_table(table);
        match res {
            Ok(table) => Ok(table),
            Err(e) => {
                self.abort(meta).unwrap();
                Err(e)
            }
        }
    }

    /// Get primary index name on a table.
    fn get_index_name(
        &self,
        table: Arc<Table>,
        meta: &TransactionInfo,
    ) -> Result<String, NonFatalError> {
        let res = table.get_primary_index();
        match res {
            Ok(index_name) => Ok(index_name),
            Err(e) => {
                self.abort(meta).unwrap();
                Err(e)
            }
        }
    }

    /// Get shared reference to index for a table.
    fn get_index(
        &self,
        table: Arc<Table>,
        meta: &TransactionInfo,
    ) -> Result<Arc<Index>, NonFatalError> {
        let index_name = self.get_index_name(table, meta)?;

        let res = self.get_data().get_internals().get_index(&index_name);
        match res {
            Ok(index) => Ok(index),
            Err(e) => {
                self.abort(meta).unwrap();
                Err(e)
            }
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.scheduler)
    }
}

impl fmt::Display for TransactionInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionInfo::*;

        match &self {
            BasicSerializationGraph { thread_id, txn_id } => write!(f, "{}-{}", thread_id, txn_id),
            OptimisticSerializationGraph { thread_id, txn_id } => {
                write!(f, "{}-{}", thread_id, txn_id)
            }
            HitList { txn_id } => write!(f, "{}", txn_id),
            OptimisticHitList { thread_id, txn_id } => write!(f, "{}-{}", thread_id, txn_id),
            TwoPhaseLocking { txn_id, timestamp } => write!(f, "{}-{}", txn_id, timestamp),
        }
    }
}
