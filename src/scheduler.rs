use crate::common::error::NonFatalError;
use crate::scheduler::msgt::MixedSerializationGraph;
use crate::scheduler::mtpl::MixedTwoPhaseLocking;
use crate::scheduler::nocc::NoConcurrencyControl;
use crate::scheduler::owh::OptimisedWaitHit;
use crate::scheduler::owhtt::OptimisedWaitHitTransactionTypes;
use crate::scheduler::sgt::SerializationGraph;
use crate::scheduler::tpl::TwoPhaseLocking;
use crate::scheduler::wh::WaitHit;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::IsolationLevel;

use config::Config;

pub mod wh;

pub mod owh;

pub mod owhtt;

pub mod sgt;

pub mod msgt;

pub mod tpl;

pub mod mtpl;

pub mod nocc;

#[derive(Debug)]
pub enum TransactionType {
    WriteOnly,
    ReadOnly,
    ReadWrite,
}

#[derive(Debug)]
pub enum Scheduler<'a> {
    SerializationGraph(SerializationGraph),
    MixedSerializationGraph(MixedSerializationGraph),
    WaitHit(WaitHit),
    OptimisedWaitHit(OptimisedWaitHit<'a>),
    OptimisedWaitHitTransactionTypes(OptimisedWaitHitTransactionTypes<'a>),
    NoConcurrencyControl(NoConcurrencyControl),
    TwoPhaseLocking(TwoPhaseLocking),
    MixedTwoPhaseLocking(MixedTwoPhaseLocking),
}

impl<'a> Scheduler<'a> {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let cores = config.get_int("cores")? as usize;

        let protocol = match config.get_str("protocol")?.as_str() {
            "sgt" => Scheduler::SerializationGraph(SerializationGraph::new(cores)),
            "msgt" => {
                let relevant_cycle_check = config.get_bool("relevant_cycle_check")?;
                Scheduler::MixedSerializationGraph(MixedSerializationGraph::new(
                    cores,
                    relevant_cycle_check,
                ))
            }
            "wh" => Scheduler::WaitHit(WaitHit::new(cores)),
            "owh" => Scheduler::OptimisedWaitHit(OptimisedWaitHit::new(cores)),
            "owhtt" => Scheduler::OptimisedWaitHitTransactionTypes(
                OptimisedWaitHitTransactionTypes::new(cores),
            ),
            "nocc" => Scheduler::NoConcurrencyControl(NoConcurrencyControl::new(cores)),
            "tpl" => Scheduler::TwoPhaseLocking(TwoPhaseLocking::new(cores)),
            "mtpl" => Scheduler::MixedTwoPhaseLocking(MixedTwoPhaseLocking::new(cores)),
            _ => panic!("Incorrect concurrency control protocol"),
        };

        Ok(protocol)
    }

    pub fn begin(&self, isolation_level: IsolationLevel) -> TransactionId {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.begin(),
            MixedSerializationGraph(sg) => sg.begin(isolation_level),
            WaitHit(wh) => wh.begin(),
            OptimisedWaitHit(owh) => owh.begin(),
            OptimisedWaitHitTransactionTypes(owhtt) => owhtt.begin(),
            NoConcurrencyControl(nocc) => nocc.begin(),
            TwoPhaseLocking(tpl) => tpl.begin(),
            MixedTwoPhaseLocking(tpl) => tpl.begin(isolation_level),
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
            WaitHit(wh) => wh.read_value(table_id, column_id, offset, meta, database),
            OptimisedWaitHit(owh) => owh.read_value(table_id, column_id, offset, meta, database),
            OptimisedWaitHitTransactionTypes(owhtt) => {
                owhtt.read_value(table_id, column_id, offset, meta, database)
            }
            NoConcurrencyControl(nocc) => {
                nocc.read_value(table_id, column_id, offset, meta, database)
            }
            TwoPhaseLocking(tpl) => tpl.read_value(table_id, column_id, offset, meta, database),
            MixedTwoPhaseLocking(tpl) => {
                tpl.read_value(table_id, column_id, offset, meta, database)
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
            OptimisedWaitHit(owh) => {
                owh.write_value(value, table_id, column_id, offset, meta, database)
            }
            OptimisedWaitHitTransactionTypes(owhtt) => {
                owhtt.write_value(value, table_id, column_id, offset, meta, database)
            }
            NoConcurrencyControl(nocc) => {
                nocc.write_value(value, table_id, column_id, offset, meta, database)
            }
            TwoPhaseLocking(tpl) => {
                tpl.write_value(value, table_id, column_id, offset, meta, database)
            }
            MixedTwoPhaseLocking(tpl) => {
                tpl.write_value(value, table_id, column_id, offset, meta, database)
            }
        }
    }

    pub fn commit(
        &self,
        meta: &TransactionId,
        database: &Database,
        transaction_type: TransactionType,
    ) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.commit(meta, database),
            MixedSerializationGraph(sg) => sg.commit(database),
            WaitHit(wh) => wh.commit(meta, database),
            OptimisedWaitHit(owh) => owh.commit(database),
            OptimisedWaitHitTransactionTypes(owhtt) => owhtt.commit(database, transaction_type),
            NoConcurrencyControl(nocc) => nocc.commit(database),
            TwoPhaseLocking(tpl) => tpl.commit(database, meta),
            MixedTwoPhaseLocking(tpl) => tpl.commit(database, meta),
        }
    }

    pub fn abort(&self, meta: &TransactionId, database: &Database) -> NonFatalError {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.abort(meta, database),
            MixedSerializationGraph(sg) => sg.abort(database),
            WaitHit(wh) => wh.abort(meta, database),
            OptimisedWaitHit(owh) => owh.abort(database),
            OptimisedWaitHitTransactionTypes(owhtt) => owhtt.abort(database),
            NoConcurrencyControl(nocc) => nocc.abort(database),
            TwoPhaseLocking(tpl) => tpl.abort(database, meta),
            MixedTwoPhaseLocking(tpl) => tpl.abort(database, meta),
        }
    }
}
