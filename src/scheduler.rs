use crate::common::error::NonFatalError;
use crate::common::statistics::protocol_diagnostics::ProtocolDiagnostics;
use crate::scheduler::attendez::Attendez;
use crate::scheduler::msgt::MixedSerializationGraph;
use crate::scheduler::nocc::NoConcurrencyControl;
use crate::scheduler::owh::OptimisedWaitHit;
use crate::scheduler::sgt::SerializationGraph;
use crate::scheduler::wh::WaitHit;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::IsolationLevel;

use config::Config;

pub mod common;

pub mod wh;

pub mod owh;

pub mod sgt;

pub mod attendez;

pub mod msgt;

pub mod nocc;

#[derive(Debug, Copy, Clone)]
pub enum TransactionType {
    WriteOnly,
    ReadOnly,
    ReadWrite,
}

#[derive(Debug)]
pub enum Scheduler<'a> {
    SerializationGraph(SerializationGraph),
    MixedSerializationGraph(MixedSerializationGraph),
    Attendez(Attendez<'a>),
    WaitHit(WaitHit),
    OptimisedWaitHit(OptimisedWaitHit<'a>),
    NoConcurrencyControl(NoConcurrencyControl),
}

impl<'a> Scheduler<'a> {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let cores = config.get_int("cores")? as usize;
        let p = config.get_str("protocol")?;

        let protocol = match p.as_str() {
            "sgt" => Scheduler::SerializationGraph(SerializationGraph::new(cores)),
            "msgt" => {
                let relevant_cycle_check = config.get_bool("relevant_dfs")?;
                Scheduler::MixedSerializationGraph(MixedSerializationGraph::new(
                    cores,
                    relevant_cycle_check,
                ))
            }
            "wh" => Scheduler::WaitHit(WaitHit::new(cores)),
            "owh" => {
                let type_aware = config.get_bool("type_aware")?;
                Scheduler::OptimisedWaitHit(OptimisedWaitHit::new(cores, type_aware))
            }
            "attendez" => {
                let watermark = config.get_int("watermark")? as u64;
                let a = config.get_int("increase")? as u64;
                let b = config.get_int("decrease")? as u64;
                let no_wait_write = config.get_bool("no_wait_write")?;

                Scheduler::Attendez(Attendez::new(cores, watermark, a, b, no_wait_write))
            }
            "nocc" => Scheduler::NoConcurrencyControl(NoConcurrencyControl::new(cores)),
            _ => panic!("unknown concurrency control protocol: {}", p),
        };

        Ok(protocol)
    }

    pub fn begin(&self, isolation_level: IsolationLevel) -> TransactionId {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.begin(),
            MixedSerializationGraph(sg) => sg.begin(isolation_level),
            Attendez(w) => w.begin(),
            WaitHit(wh) => wh.begin(),
            OptimisedWaitHit(owh) => owh.begin(),
            NoConcurrencyControl(nocc) => nocc.begin(),
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
            MixedSerializationGraph(sg) => {
                sg.read_value(table_id, column_id, offset, meta, database)
            }
            Attendez(wh) => wh.read_value(table_id, column_id, offset, meta, database),

            WaitHit(wh) => wh.read_value(table_id, column_id, offset, meta, database),
            OptimisedWaitHit(owh) => owh.read_value(table_id, column_id, offset, meta, database),
            NoConcurrencyControl(nocc) => {
                nocc.read_value(table_id, column_id, offset, meta, database)
            }
        }
    }

    pub fn write_value(
        &self,
        value: &mut Data,
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
            MixedSerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database)
            }
            WaitHit(wh) => wh.write_value(value, table_id, column_id, offset, meta, database),
            Attendez(wh) => wh.write_value(value, table_id, column_id, offset, meta, database),
            OptimisedWaitHit(owh) => {
                owh.write_value(value, table_id, column_id, offset, meta, database)
            }
            NoConcurrencyControl(nocc) => {
                nocc.write_value(value, table_id, column_id, offset, meta, database)
            }
        }
    }

    pub fn commit(
        &self,
        meta: &TransactionId,
        database: &Database,
        transaction_type: TransactionType,
    ) -> Result<ProtocolDiagnostics, NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.commit(database),
            MixedSerializationGraph(sg) => sg.commit(database),
            WaitHit(wh) => wh.commit(meta, database),
            Attendez(wh) => wh.commit(database),
            OptimisedWaitHit(owh) => owh.commit(database, transaction_type),
            NoConcurrencyControl(nocc) => nocc.commit(database),
        }
    }

    pub fn abort(&self, meta: &TransactionId, database: &Database) -> NonFatalError {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.abort(database),
            MixedSerializationGraph(sg) => sg.abort(database),
            WaitHit(wh) => wh.abort(meta, database),
            Attendez(wh) => wh.abort(database),
            OptimisedWaitHit(owh) => owh.abort(database),
            NoConcurrencyControl(nocc) => nocc.abort(database),
        }
    }
}
