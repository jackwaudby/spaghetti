use crate::common::error::NonFatalError;
use crate::scheduler::basic_sgt::BasicSerializationGraphTesting;
// use crate::scheduler::hit_list::HitList;
// use crate::scheduler::opt_hit_list::OptimisedHitList;
// use crate::scheduler::serialization_graph_testing::SerializationGraphTesting;
// use crate::scheduler::two_phase_locking::TwoPhaseLocking;
use crate::storage::datatype::Data;
//use crate::storage::index::Index;
//use crate::storage::table::Table;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use std::fmt;
//use std::sync::Arc;

// pub mod hit_list;

// pub mod opt_hit_list;

// pub mod two_phase_locking;

// pub mod serialization_graph_testing;

pub mod basic_sgt;

// /// A concurrency control protocol.
// ///
// /// Uses trait object for dynamic dispatch to switch between protocols.
// #[derive(Debug)]
// pub struct Protocol {
//     /// Trait object pointing to scheduler.
//     pub scheduler: Box<dyn Scheduler + Send + Sync + 'static>,
// }

pub enum Protocol {
    BasicSerializationGraphTesting(BasicSerializationGraphTesting),
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
    pub fn new(workload: Workload, cores: usize) -> crate::Result<Protocol> {
        // Determine workload type.
        let protocol = match workload.get_config().get_str("protocol")?.as_str() {
            // "2pl" => Protocol {
            //     scheduler: Box::new(TwoPhaseLocking::new(Arc::clone(&workload))),
            // },
            // "sgt" => Protocol {
            //     scheduler: Box::new(SerializationGraphTesting::new(
            //         cores as u32,
            //         Arc::clone(&workload),
            //     )),
            // },
            // "basic-sgt" => Protocol {
            //     scheduler: Box::new(BasicSerializationGraphTesting::new(cores as u32, workload)),
            // },
            "basic-sgt" => Protocol::BasicSerializationGraphTesting(
                BasicSerializationGraphTesting::new(cores as u32, workload),
            ),
            // },
            // "hit" => Protocol {
            //     scheduler: Box::new(HitList::new(Arc::clone(&workload))),
            // },
            // "opt-hit" => Protocol {
            //     scheduler: Box::new(OptimisedHitList::new(cores, Arc::clone(&workload))),
            // },
            _ => panic!("Incorrect concurrency control protocol"),
        };
        Ok(protocol)
    }

    pub fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        use Protocol::*;
        match self {
            BasicSerializationGraphTesting(bsgt) => bsgt.register(),
        }
    }

    pub fn read(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        use Protocol::*;
        match self {
            BasicSerializationGraphTesting(bsgt) => bsgt.read(table, index, key, columns, meta),
        }
    }

    pub fn update(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: &dyn Fn(Option<Vec<Data>>, Option<&[Data]>) -> Result<Vec<Data>, NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        use Protocol::*;
        match self {
            BasicSerializationGraphTesting(bsgt) => {
                bsgt.update(table, index, key, columns, read, params, f, meta)
            }
        }
    }

    pub fn read_and_update(
        &self,
        table: &str,
        index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        use Protocol::*;
        match self {
            BasicSerializationGraphTesting(bsgt) => {
                bsgt.read_and_update(table, index, key, columns, values, meta)
            }
        }
    }

    pub fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        use Protocol::*;
        match self {
            BasicSerializationGraphTesting(bsgt) => bsgt.commit(meta),
        }
    }

    /// Abort a transaction.
    pub fn abort(&self, meta: &TransactionInfo) -> crate::Result<()> {
        use Protocol::*;
        match self {
            BasicSerializationGraphTesting(bsgt) => bsgt.abort(meta),
        }
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
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: &dyn Fn(Option<Vec<Data>>, Option<&[Data]>) -> Result<Vec<Data>, NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError>;
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO")
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
