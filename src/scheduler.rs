use crate::common::error::NonFatalError;
use crate::scheduler::attendez::Attendez;
use crate::scheduler::msgt::MixedSerializationGraph;
use crate::scheduler::msgtall::AllMixedSerializationGraph;
use crate::scheduler::msgtearly::EarlyMixedSerializationGraph;
use crate::scheduler::msgtrel::RelMixedSerializationGraph;
use crate::scheduler::msgtstd::StdMixedSerializationGraph;
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

pub mod error;

pub mod common;

pub mod wh;

pub mod owh;

pub mod owhtt;

pub mod sgt;

pub mod attendez;

// Testing only
pub mod msgt;

pub mod msgtstd;

pub mod msgtrel;

pub mod msgtearly;

pub mod msgtall;

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
    // For testing only
    MixedSerializationGraph(MixedSerializationGraph),
    // Edge detection
    StdMixedSerializationGraph(StdMixedSerializationGraph),
    // Edge detection + early commit
    EarlyMixedSerializationGraph(EarlyMixedSerializationGraph),
    // Edge detection + relevant cycle check
    RelMixedSerializationGraph(RelMixedSerializationGraph),
    // Edge detection + relevant cycle check + early commit
    AllMixedSerializationGraph(AllMixedSerializationGraph),
    Attendez(Attendez<'a>),
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
                let detection_deadlock = config.get_bool("deadlock_detection")?;
                Scheduler::MixedSerializationGraph(MixedSerializationGraph::new(
                    cores,
                    relevant_cycle_check,
                    detection_deadlock,
                ))
            }
            "msgt-std" => {
                Scheduler::StdMixedSerializationGraph(StdMixedSerializationGraph::new(cores))
            }
            "msgt-rel" => {
                Scheduler::RelMixedSerializationGraph(RelMixedSerializationGraph::new(cores))
            }
            "msgt-early" => {
                Scheduler::EarlyMixedSerializationGraph(EarlyMixedSerializationGraph::new(cores))
            }
            "msgt-all" => {
                Scheduler::AllMixedSerializationGraph(AllMixedSerializationGraph::new(cores))
            }
            "wh" => Scheduler::WaitHit(WaitHit::new(cores)),
            "owh" => Scheduler::OptimisedWaitHit(OptimisedWaitHit::new(cores)),
            "attendez" => {
                let watermark = config.get_int("watermark")? as u64;
                let a = config.get_int("increase")? as u64;
                let b = config.get_int("decrease")? as u64;

                Scheduler::Attendez(Attendez::new(cores, watermark, a, b))
            }

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
            StdMixedSerializationGraph(sg) => sg.begin(isolation_level),
            RelMixedSerializationGraph(sg) => sg.begin(isolation_level),
            EarlyMixedSerializationGraph(sg) => sg.begin(isolation_level),
            AllMixedSerializationGraph(sg) => sg.begin(isolation_level),
            Attendez(w) => w.begin(),
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
            StdMixedSerializationGraph(sg) => {
                sg.read_value(table_id, column_id, offset, meta, database)
            }
            RelMixedSerializationGraph(sg) => {
                sg.read_value(table_id, column_id, offset, meta, database)
            }
            EarlyMixedSerializationGraph(sg) => {
                sg.read_value(table_id, column_id, offset, meta, database)
            }
            AllMixedSerializationGraph(sg) => {
                sg.read_value(table_id, column_id, offset, meta, database)
            }
            Attendez(wh) => wh.read_value(table_id, column_id, offset, meta, database),

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
            StdMixedSerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database)
            }
            RelMixedSerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database)
            }
            EarlyMixedSerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database)
            }
            AllMixedSerializationGraph(sg) => {
                sg.write_value(value, table_id, column_id, offset, meta, database)
            }
            WaitHit(wh) => wh.write_value(value, table_id, column_id, offset, meta, database),
            Attendez(wh) => wh.write_value(value, table_id, column_id, offset, meta, database),

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
            SerializationGraph(sg) => sg.commit(database),
            MixedSerializationGraph(sg) => sg.commit(database),
            StdMixedSerializationGraph(sg) => sg.commit(database),
            RelMixedSerializationGraph(sg) => sg.commit(database),
            EarlyMixedSerializationGraph(sg) => sg.commit(database),
            AllMixedSerializationGraph(sg) => sg.commit(database),
            WaitHit(wh) => wh.commit(meta, database),
            Attendez(wh) => wh.commit(database),

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
            SerializationGraph(sg) => sg.abort(database),
            MixedSerializationGraph(sg) => sg.abort(database),
            StdMixedSerializationGraph(sg) => sg.abort(database),
            RelMixedSerializationGraph(sg) => sg.abort(database),
            EarlyMixedSerializationGraph(sg) => sg.abort(database),
            AllMixedSerializationGraph(sg) => sg.abort(database),
            WaitHit(wh) => wh.abort(meta, database),
            Attendez(wh) => wh.abort(database),
            OptimisedWaitHit(owh) => owh.abort(database),
            OptimisedWaitHitTransactionTypes(owhtt) => owhtt.abort(database),
            NoConcurrencyControl(nocc) => nocc.abort(database),
            TwoPhaseLocking(tpl) => tpl.abort(database, meta),
            MixedTwoPhaseLocking(tpl) => tpl.abort(database, meta),
        }
    }
}
