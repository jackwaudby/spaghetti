use crate::common::error::NonFatalError;
use crate::scheduler::sgt::SerializationGraph;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::Database;

use config::Config;
use std::fmt;

pub mod sgt;

#[derive(Debug)]
pub enum Scheduler {
    SerializationGraph(SerializationGraph),
}

impl Scheduler {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let cores = config.get_int("cores")? as usize;

        let protocol = match config.get_str("protocol")?.as_str() {
            "sgt" => Scheduler::SerializationGraph(SerializationGraph::new(cores)),
            "owh" => unimplemented!(),
            _ => panic!("Incorrect concurrency control protocol"),
        };
        Ok(protocol)
    }

    pub fn begin(&self) -> TransactionId {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.begin(),
        }
    }

    pub fn read_value(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.read_value(table_id, column_id, offset, meta, database),
        }
    }

    pub fn write_value(
        &self,
        value: &Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database)
            }
        }
    }

    pub fn commit(&self, _meta: &TransactionId, database: &Database) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.commit(database),
        }
    }

    pub fn abort(&self, _meta: &TransactionId, database: &Database) -> NonFatalError {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.abort(database),
        }
    }
}

impl fmt::Display for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO")
    }
}
