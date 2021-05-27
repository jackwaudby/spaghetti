use crate::common::error::NonFatalError;
use crate::scheduler::sgt::node::WeakNode;
use crate::scheduler::sgt::SerializationGraph;

//use crate::scheduler::owh::OptimisedWaitHit;
use crate::storage::datatype::Data;
use crate::workloads::{PrimaryKey, Workload};

use std::fmt;

pub type CurrentValues = Option<Vec<Data>>;
pub type NewValues = Vec<Data>;

pub mod sgt;

//pub mod owh;

#[derive(Debug)]
pub enum Protocol {
    SerializationGraph(SerializationGraph),
    //   OptimisticWaitHit(OptimisedWaitHit),
}

#[derive(Debug, Clone)]
pub enum TransactionInfo {
    SerializationGraph(WeakNode),
    OptimisticWaitHit(usize, usize),
}

impl Protocol {
    pub fn new(workload: Workload, cores: usize) -> crate::Result<Protocol> {
        let protocol = match workload.get_config().get_str("protocol")?.as_str() {
            "sgt" => Protocol::SerializationGraph(SerializationGraph::new(cores as u32, workload)),
            //    "owh" => Protocol::OptimisticWaitHit(OptimisedWaitHit::new(cores, workload)),
            _ => panic!("Incorrect concurrency control protocol"),
        };
        Ok(protocol)
    }

    pub fn begin(&self) -> TransactionInfo {
        use Protocol::*;
        match self {
            SerializationGraph(sg) => sg.begin(),
            //  OptimisticWaitHit(owh) => owh.begin(),
        }
    }

    pub fn read(
        &self,
        index: usize,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        use Protocol::*;
        match self {
            SerializationGraph(sg) => sg.read(index, key, columns, meta),
            //  OptimisticWaitHit(owh) => owh.read(index, key, columns, meta),
        }
    }

    pub fn write(
        &self,
        index: usize,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: &dyn Fn(CurrentValues, Option<&[Data]>) -> Result<NewValues, NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<Option<Vec<Data>>, NonFatalError> {
        use Protocol::*;
        match self {
            SerializationGraph(sg) => sg.write(index, key, columns, read, params, f, meta),
            //  OptimisticWaitHit(owh) => owh.write(index, key, columns, read, params, f, meta),
        }
    }

    pub fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        use Protocol::*;
        match self {
            SerializationGraph(sg) => sg.commit(meta),
            //    OptimisticWaitHit(owh) => owh.commit(meta),
        }
    }

    pub fn abort(&self, meta: &TransactionInfo) -> NonFatalError {
        use Protocol::*;
        match self {
            SerializationGraph(sg) => sg.abort(meta),
            //   OptimisticWaitHit(owh) => owh.abort(meta),
        }
    }
}

pub trait Scheduler: fmt::Display + fmt::Debug {
    /// Begin operation.
    fn begin(&self) -> TransactionInfo;

    /// Read operation.
    fn read(
        &self,
        index_id: usize,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError>;

    /// Write operation.
    ///
    /// From the output of `read` and `params` new values for `columns` are calculated and set.
    ///
    /// * `index_id` - The name of the index the row resides in.
    /// * `key` - The primary key of the row to be written.
    /// * `columns` - The set of columns to be written.
    /// * `read` - The set of columns to be read; used as an input into `f` to calculate new values for `columns`.
    /// * `params` - Transaction parameters; used as an input into `f` to calculate new values for `columns`.
    /// * `f` - A closure that takes current values and transaction parameters to calculate new values for `columns`.
    /// * `meta` - Transaction information.
    fn write(
        &self,
        index_id: usize,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: &dyn Fn(CurrentValues, Option<&[Data]>) -> Result<NewValues, NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<Option<Vec<Data>>, NonFatalError>;

    /// Commit operation.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError>;

    /// Abort operation.
    ///
    /// Typically called from within the other methods, it returns the reason for the abort.
    fn abort(&self, meta: &TransactionInfo) -> NonFatalError;

    // /// List data type only.
    // ///
    // /// Append `value` to `column`.
    // fn append(
    //     &self,
    //     table: &str,
    //     index: Option<&str>,
    //     key: &PrimaryKey,
    //     column: &str,
    //     value: Data,
    //     meta: &TransactionInfo,
    // ) -> Result<(), NonFatalError>;
}

impl PartialEq for TransactionInfo {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                &TransactionInfo::SerializationGraph(ref wn1),
                &TransactionInfo::SerializationGraph(ref wn2),
            ) => wn1.ptr_eq(&wn2),
            (
                &TransactionInfo::OptimisticWaitHit(a, b),
                &TransactionInfo::OptimisticWaitHit(c, d),
            ) => (a == c) && (b == d),
            _ => false,
        }
    }
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
            SerializationGraph(node) => write!(f, "{}", node.as_ptr() as usize),
            OptimisticWaitHit(thread_id, seq_num) => write!(f, "({}-{})", thread_id, seq_num),
        }
    }
}
