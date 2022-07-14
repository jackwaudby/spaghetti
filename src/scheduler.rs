use crate::common::isolation_level::IsolationLevel;
use crate::common::{
    error::NonFatalError, statistics::local::LocalStatistics, stats_bucket::StatsBucket,
    value_id::ValueId,
};
use crate::scheduler::{
    msgt::MixedSerializationGraph, nocc::NoConcurrencyControl, sgt::SerializationGraph,
};
use crate::storage::{datatype::Data, Database};

use config::Config;

pub mod common;

pub mod sgt;

pub mod msgt;

pub mod nocc;

#[derive(Debug, Copy, Clone)]
pub enum TransactionType {
    WriteOnly,
    ReadOnly,
    ReadWrite,
}

#[derive(Debug)]
pub enum Scheduler {
    SerializationGraph(SerializationGraph),
    MixedSerializationGraph(MixedSerializationGraph),
    NoConcurrencyControl(NoConcurrencyControl),
}

impl Scheduler {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let cores = config.get_int("cores")? as usize;
        let p = config.get_str("protocol")?;

        let protocol = match p.as_str() {
            "sgt" => Scheduler::SerializationGraph(SerializationGraph::new(cores)),
            "msgt" => {
                let cycle_check_strategy = config.get_str("dfs")?;
                Scheduler::MixedSerializationGraph(MixedSerializationGraph::new(
                    cores,
                    &cycle_check_strategy,
                ))
            }
            "nocc" => Scheduler::NoConcurrencyControl(NoConcurrencyControl::new(cores)),
            _ => panic!("unknown concurrency control protocol: {}", p),
        };

        Ok(protocol)
    }

    pub fn begin(&self, isolation_level: IsolationLevel) -> StatsBucket {
        use Scheduler::*;

        let transaction_id = match self {
            SerializationGraph(sg) => sg.begin(),
            MixedSerializationGraph(sg) => sg.begin(isolation_level),
            NoConcurrencyControl(nocc) => nocc.begin(),
        };

        StatsBucket::new(transaction_id)
    }

    pub fn read_value(
        &self,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
        stats: &mut LocalStatistics,
    ) -> Result<Data, NonFatalError> {
        use Scheduler::*;

        match self {
            SerializationGraph(sg) => sg.read_value(vid, meta, database, stats),
            MixedSerializationGraph(sg) => sg.read_value(vid, meta, database),
            NoConcurrencyControl(nocc) => nocc.read_value(vid, meta, database),
        }
    }

    pub fn write_value(
        &self,
        value: &mut Data,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
        stats: &mut LocalStatistics,
    ) -> Result<(), NonFatalError> {
        use Scheduler::*;

        match self {
            SerializationGraph(sg) => sg.write_value(value, vid, meta, database, stats),
            MixedSerializationGraph(sg) => sg.write_value(value, vid, meta, database),
            NoConcurrencyControl(nocc) => nocc.write_value(value, vid, meta, database),
        }
    }

    pub fn commit(&self, meta: &mut StatsBucket, database: &Database) -> Result<(), NonFatalError> {
        use Scheduler::*;

        match self {
            SerializationGraph(sg) => sg.commit(meta, database),
            MixedSerializationGraph(sg) => sg.commit(meta, database),
            NoConcurrencyControl(nocc) => nocc.commit(meta, database),
        }
    }

    pub fn abort(&self, meta: &mut StatsBucket, database: &Database) {
        use Scheduler::*;

        let res = match self {
            SerializationGraph(sg) => sg.abort(meta, database),
            MixedSerializationGraph(sg) => {
                sg.abort(meta, database);
            }
            NoConcurrencyControl(nocc) => {
                nocc.abort(meta, database);
            }
        };

        res
    }
}
