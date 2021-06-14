use crate::common::error::NonFatalError;
use crate::scheduler::nocc::NoConcurrencyControl;
use crate::scheduler::owh::OptimisedWaitHit;
use crate::scheduler::sgt::SerializationGraph;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::Database;
use config::Config;
use crossbeam_epoch::Guard;

pub mod owh;

pub mod sgt;

pub mod nocc;

pub mod nocc2;

#[derive(Debug)]
pub enum Scheduler<'a> {
    SerializationGraph(SerializationGraph<'a>),
    OptimisedWaitHit(OptimisedWaitHit<'a>),
    NoConcurrencyControl(NoConcurrencyControl),
}

impl<'a> Scheduler<'a> {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let cores = config.get_int("cores")? as usize;

        let protocol = match config.get_str("protocol")?.as_str() {
            "sgt" => Scheduler::SerializationGraph(SerializationGraph::new(cores)),
            "owh" => Scheduler::OptimisedWaitHit(OptimisedWaitHit::new(cores)),
            "nocc" => Scheduler::NoConcurrencyControl(NoConcurrencyControl::new(cores)),
            _ => panic!("Incorrect concurrency control protocol"),
        };

        Ok(protocol)
    }

    pub fn begin(&self) -> TransactionId {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.begin(),
            OptimisedWaitHit(owh) => owh.begin(),
            NoConcurrencyControl(nocc) => nocc.begin(),
        }
    }

    pub fn read_value<'g>(
        &self,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> Result<Data, NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => {
                sg.read_value(table_id, column_id, offset, meta, database, guard)
            }
            OptimisedWaitHit(owh) => {
                owh.read_value(table_id, column_id, offset, meta, database, guard)
            }
            NoConcurrencyControl(nocc) => {
                nocc.read_value(table_id, column_id, offset, meta, database, guard)
            }
        }
    }

    pub fn write_value<'g>(
        &self,
        value: &Data,
        table_id: usize,
        column_id: usize,
        offset: usize,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database, guard)
            }
            OptimisedWaitHit(owh) => {
                owh.write_value(value, table_id, column_id, offset, meta, database, guard)
            }
            NoConcurrencyControl(nocc) => {
                nocc.write_value(value, table_id, column_id, offset, meta, database, guard)
            }
        }
    }

    pub fn commit<'g>(
        &self,
        _meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.commit(database, guard),
            OptimisedWaitHit(owh) => owh.commit(database, guard),
            NoConcurrencyControl(nocc) => nocc.commit(database, guard),
        }
    }

    pub fn abort<'g>(
        &self,
        _meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> NonFatalError {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.abort(database, guard),
            OptimisedWaitHit(owh) => owh.abort(database, guard),
            NoConcurrencyControl(nocc) => nocc.abort(database, guard),
        }
    }
}
