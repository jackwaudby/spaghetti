use crate::common::error::NonFatalError;
use crate::scheduler::nocc::NoConcurrencyControl;
use crate::scheduler::owh::OptimisedWaitHit;
use crate::scheduler::owhtt::OptimisedWaitHitTransactionTypes;
use crate::scheduler::sgt::SerializationGraph;
use crate::scheduler::tpl::TwoPhaseLocking;
use crate::scheduler::wh::WaitHit;
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::Database;
use config::Config;
use crossbeam_epoch::Guard;

pub mod wh;

pub mod owh;

pub mod owhtt;

pub mod sgt;

pub mod tpl;

pub mod nocc;

#[derive(Debug)]
pub enum TransactionType {
    WriteOnly,
    ReadOnly,
    ReadWrite,
}

#[derive(Debug)]
pub enum Scheduler<'a> {
    SerializationGraph(SerializationGraph<'a>),
    WaitHit(WaitHit),
    OptimisedWaitHit(OptimisedWaitHit<'a>),
    OptimisedWaitHitTransactionTypes(OptimisedWaitHitTransactionTypes<'a>),
    NoConcurrencyControl(NoConcurrencyControl),
    TwoPhaseLocking(TwoPhaseLocking),
}

impl<'a> Scheduler<'a> {
    pub fn new(config: &Config) -> crate::Result<Self> {
        let cores = config.get_int("cores")? as usize;

        let protocol = match config.get_str("protocol")?.as_str() {
            "sgt" => Scheduler::SerializationGraph(SerializationGraph::new(cores)),
            "wh" => Scheduler::WaitHit(WaitHit::new(cores)),
            "owh" => Scheduler::OptimisedWaitHit(OptimisedWaitHit::new(cores)),
            "owhtt" => Scheduler::OptimisedWaitHitTransactionTypes(
                OptimisedWaitHitTransactionTypes::new(cores),
            ),
            "nocc" => Scheduler::NoConcurrencyControl(NoConcurrencyControl::new(cores)),
            "tpl" => Scheduler::TwoPhaseLocking(TwoPhaseLocking::new(cores)),
            _ => panic!("Incorrect concurrency control protocol"),
        };

        Ok(protocol)
    }

    pub fn begin(&self) -> TransactionId {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.begin(),
            WaitHit(wh) => wh.begin(),
            OptimisedWaitHit(owh) => owh.begin(),
            OptimisedWaitHitTransactionTypes(owhtt) => owhtt.begin(),
            NoConcurrencyControl(nocc) => nocc.begin(),
            TwoPhaseLocking(tpl) => tpl.begin(),
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
            WaitHit(wh) => wh.read_value(table_id, column_id, offset, meta, database, guard),
            OptimisedWaitHit(owh) => {
                owh.read_value(table_id, column_id, offset, meta, database, guard)
            }
            OptimisedWaitHitTransactionTypes(owhtt) => {
                owhtt.read_value(table_id, column_id, offset, meta, database, guard)
            }
            NoConcurrencyControl(nocc) => {
                nocc.read_value(table_id, column_id, offset, meta, database, guard)
            }
            TwoPhaseLocking(tpl) => tpl.read_value(table_id, column_id, offset, meta, database),
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
            WaitHit(wh) => {
                wh.write_value(value, table_id, column_id, offset, meta, database, guard)
            }
            OptimisedWaitHit(owh) => {
                owh.write_value(value, table_id, column_id, offset, meta, database, guard)
            }
            OptimisedWaitHitTransactionTypes(owhtt) => {
                owhtt.write_value(value, table_id, column_id, offset, meta, database, guard)
            }
            NoConcurrencyControl(nocc) => {
                nocc.write_value(value, table_id, column_id, offset, meta, database, guard)
            }
            TwoPhaseLocking(tpl) => {
                tpl.write_value(value, table_id, column_id, offset, meta, database)
            }
        }
    }

    pub fn commit<'g>(
        &self,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
        transaction_type: TransactionType,
    ) -> Result<(), NonFatalError> {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.commit(database, guard),
            WaitHit(wh) => wh.commit(meta, database, guard),
            OptimisedWaitHit(owh) => owh.commit(database, guard),
            OptimisedWaitHitTransactionTypes(owhtt) => {
                owhtt.commit(database, guard, transaction_type)
            }
            NoConcurrencyControl(nocc) => nocc.commit(database, guard),
            TwoPhaseLocking(tpl) => tpl.commit(database, meta),
        }
    }

    pub fn abort<'g>(
        &self,
        meta: &TransactionId,
        database: &Database,
        guard: &'g Guard,
    ) -> NonFatalError {
        use Scheduler::*;
        match self {
            SerializationGraph(sg) => sg.abort(database, guard),
            WaitHit(wh) => wh.abort(meta, database, guard),
            OptimisedWaitHit(owh) => owh.abort(database, guard),
            OptimisedWaitHitTransactionTypes(owhtt) => owhtt.abort(database, guard),
            NoConcurrencyControl(nocc) => nocc.abort(database, guard),
            TwoPhaseLocking(tpl) => tpl.abort(database, meta),
        }
    }
}
