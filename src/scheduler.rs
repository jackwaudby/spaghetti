use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::common::error::NonFatalError;
use crate::scheduler::sgt::node::WeakNode;
use crate::scheduler::sgt::SerializationGraph;
use crate::storage::datatype::Data;
use crate::storage::row::Tuple;
use crate::storage::Access;
use crate::workloads::Database;

use config::Config;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub mod sgt;

#[derive(Debug)]
pub enum Scheduler {
    SerializationGraph(SerializationGraph),
}

#[derive(Debug, Clone)]
pub enum TransactionInfo {
    SerializationGraph(WeakNode),
    OptimisticWaitHit(usize, usize),
}

impl Scheduler {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let cores = config.get_int("cores")? as usize;

        let protocol = match config.get_str("protocol")?.as_str() {
            "sgt" => Scheduler::SerializationGraph(SerializationGraph::new(cores)),
            _ => panic!("Incorrect concurrency control protocol"),
        };
        Ok(protocol)
    }

    pub fn begin(&self) -> TransactionInfo {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.begin(),
            //  OptimisticWaitHit(owh) => owh.begin(),
        }
    }

    pub fn read_value(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionInfo,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.read_value(table_id, column_id, offset, meta, database),
            //  OptimisticWaitHit(owh) => owh.read(index, key, columns, meta),
        }
    }

    pub fn write_value(
        &self,
        value: &Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionInfo,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database)
            } //  OptimisticWaitHit(owh) => owh.write(index, key, columns, read, params, f, meta),
        }
    }

    pub fn commit(&self, meta: &TransactionInfo, database: &Database) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.commit(meta, database),
            //    OptimisticWaitHit(owh) => owh.commit(meta),
        }
    }

    pub fn abort(&self, meta: &TransactionInfo, database: &Database) -> NonFatalError {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.abort(meta, database),
            //   OptimisticWaitHit(owh) => owh.abort(meta),
        }
    }
}

// pub trait Protocol: fmt::Display + fmt::Debug {
//     fn begin(&self) -> TransactionInfo;

//     fn read_value(
//         &self,
//         column: &[Tuple],
//         lsns: &[AtomicU64],
//         rw_tables: &[AtomicLinkedList<Access>],
//         offset: usize,
//         meta: &TransactionInfo,
//     ) -> Result<Data, NonFatalError>;

//     fn write_value(
//         &self,
//         value: &Data,
//         column: &[Tuple],
//         lsns: &[AtomicU64],
//         rw_tables: &[AtomicLinkedList<Access>],
//         offset: usize,
//         meta: &TransactionInfo,
//     ) -> Result<(), NonFatalError>;

//     fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError>;

//     fn abort(&self, meta: &TransactionInfo) -> NonFatalError;
// }

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

impl fmt::Display for Scheduler {
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
