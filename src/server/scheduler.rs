use crate::server::scheduler::two_phase_locking::TwoPhaseLocking;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::Row;
use crate::workloads::Internal;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;
use crate::Result;

use chrono::{DateTime, Utc};
use std::sync::Arc;

pub mod two_phase_locking;

pub enum Protocol {
    TwoPhaseLocking(TwoPhaseLocking),
    SerializationGraph,
}

impl Protocol {
    pub fn new(w: Arc<Workload>) -> Result<Protocol> {
        // Determine workload type.
        match w.get_internals().config.get_str("protocol")?.as_str() {
            "2pl" => {
                // Create scheduler
                let tpl = TwoPhaseLocking::new(Arc::clone(&w));
                Ok(Protocol::TwoPhaseLocking(tpl))
            }

            _ => panic!("Incorrect concurrency control protocol"),
        }
    }

    pub fn get_internals(&self) -> &Internal {
        use Protocol::*;
        match self {
            TwoPhaseLocking(ref cc) => cc.get_internals(),
            _ => unimplemented!(),
        }
    }

    pub fn register(&self, transaction_name: &str) -> Result<()> {
        use Protocol::*;
        match self {
            TwoPhaseLocking(ref cc) => cc.register(transaction_name),
            _ => unimplemented!(),
        }
    }

    pub fn read(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<Vec<Data>> {
        use Protocol::*;
        match self {
            TwoPhaseLocking(ref cc) => {
                cc.read(index, key, columns, transaction_name, transaction_ts)
            }
            _ => unimplemented!(),
        }
    }

    pub fn insert(
        &self,
        index: &str,
        pk: PrimaryKey,
        row: Row,
        transaction_name: &str,
    ) -> Result<()> {
        use Protocol::*;
        match self {
            TwoPhaseLocking(ref cc) => cc.insert(index, pk, row, transaction_name),
            _ => unimplemented!(),
        }
    }

    pub fn write(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<()> {
        use Protocol::*;
        match self {
            TwoPhaseLocking(ref cc) => cc.write(
                index,
                key,
                columns,
                values,
                transaction_name,
                transaction_ts,
            ),
            _ => unimplemented!(),
        }
    }

    pub fn delete(&self, index: &str, pk: PrimaryKey, transaction_name: &str) -> Result<()> {
        use Protocol::*;
        match self {
            TwoPhaseLocking(ref cc) => cc.delete(index, pk, transaction_name),
            _ => unimplemented!(),
        }
    }

    pub fn commit(&self, transaction_name: &str) {
        use Protocol::*;
        match self {
            TwoPhaseLocking(ref cc) => cc.commit(transaction_name),
            _ => unimplemented!(),
        }
    }
}

pub trait Scheduler {
    fn register(&self, transaction_name: &str) -> Result<()>;

    fn read(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<Vec<Data>>;

    fn delete(&self, index: &str, pk: PrimaryKey, transaction_name: &str) -> Result<()>;

    fn commit(&self, transaction_name: &str);

    fn write(
        &self,
        index: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        transaction_name: &str,
        transaction_ts: DateTime<Utc>,
    ) -> Result<()>;

    fn insert(&self, index: &str, pk: PrimaryKey, row: Row, transaction_name: &str) -> Result<()>;

    fn get_internals(&self) -> &Internal;
}
