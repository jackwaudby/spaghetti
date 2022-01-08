use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::storage::PrimaryKey;
use crate::workloads::tatp::keys::TatpPrimaryKey;
use crate::workloads::tatp::paramgen::{
    GetAccessData, GetNewDestination, GetSubscriberData, UpdateLocationData, UpdateSubscriberData,
};
use crate::workloads::IsolationLevel;

use std::convert::TryFrom;
use tracing::instrument;

/// GetSubscriberData transaction.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn get_subscriber_data<'a>(
    params: GetSubscriberData,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Tatp(_) => {
            let meta = scheduler.begin(isolation); // register

            let offset = match database
                .get_table(0)
                .exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id)))
            {
                Ok(offset) => offset,
                Err(e) => {
                    scheduler.abort(&meta, database);
                    return Err(e);
                }
            };
            scheduler.read_value(0, 0, offset, &meta, database)?;
            scheduler.read_value(0, 1, offset, &meta, database)?;
            scheduler.read_value(0, 2, offset, &meta, database)?;
            scheduler.read_value(0, 3, offset, &meta, database)?;
            scheduler.read_value(0, 4, offset, &meta, database)?;
            scheduler.commit(&meta, database, TransactionType::ReadOnly)?;

            Ok(Success::new(meta, None, None, None, None, None))
        }
        _ => panic!("unexpected database"),
    }
}

/// GetNewDestination transaction.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn get_new_destination<'a>(
    params: GetNewDestination,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Tatp(_) => {
            let meta = scheduler.begin(isolation); // register

            let sf_offset = match database.get_table(2).exists(PrimaryKey::Tatp(
                TatpPrimaryKey::SpecialFacility(params.s_id, params.sf_type.into()),
            )) {
                Ok(offset) => offset,
                Err(e) => {
                    scheduler.abort(&meta, database);
                    return Err(e);
                }
            };

            scheduler.read_value(2, 0, sf_offset, &meta, database)?;
            scheduler.read_value(2, 1, sf_offset, &meta, database)?;
            let is_active = u64::try_from(scheduler.read_value(2, 2, sf_offset, &meta, database)?)?;

            if is_active != 1 {
                scheduler.abort(&meta, database);
                return Err(NonFatalError::RowNotFound(
                    "todo".to_string(),
                    "special_facility".to_string(),
                ));
            }

            let cf_offset = match database.get_table(3).exists(PrimaryKey::Tatp(
                TatpPrimaryKey::CallForwarding(
                    params.s_id,
                    params.sf_type.into(),
                    params.start_time.into(),
                ),
            )) {
                Ok(offset) => offset,
                Err(e) => {
                    scheduler.abort(&meta, database);
                    return Err(e);
                }
            };

            scheduler.read_value(3, 0, cf_offset, &meta, database)?;
            scheduler.read_value(3, 1, cf_offset, &meta, database)?;
            scheduler.read_value(3, 2, cf_offset, &meta, database)?;
            let end_time = u64::try_from(scheduler.read_value(3, 3, sf_offset, &meta, database)?)?;
            scheduler.read_value(3, 4, sf_offset, &meta, database)?;

            if params.end_time as u64 >= end_time {
                scheduler.abort(&meta, database);
                return Err(NonFatalError::RowNotFound(
                    "todo".to_string(),
                    "call_forwarding".to_string(),
                ));
            }

            scheduler.commit(&meta, database, TransactionType::ReadOnly)?;

            Ok(Success::new(meta, None, None, None, None, None))
        }
        _ => panic!("unexpected database"),
    }
}

/// GetAccessData transaction.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn get_access_data<'a>(
    params: GetAccessData,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Tatp(_) => {
            let meta = scheduler.begin(isolation); // register

            let offset =
                match database
                    .get_table(0)
                    .exists(PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(
                        params.s_id,
                        params.ai_type.into(),
                    ))) {
                    Ok(offset) => offset,
                    Err(e) => {
                        scheduler.abort(&meta, database);
                        return Err(e);
                    }
                };

            scheduler.read_value(0, 2, offset, &meta, database)?;
            scheduler.read_value(0, 3, offset, &meta, database)?;
            scheduler.read_value(0, 4, offset, &meta, database)?;
            scheduler.read_value(0, 5, offset, &meta, database)?;

            scheduler.commit(&meta, database, TransactionType::ReadOnly)?;

            Ok(Success::new(meta, None, None, None, None, None))
        }
        _ => panic!("unexpected database"),
    }
}

/// Update subscriber transaction.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn update_subscriber_data<'a>(
    params: UpdateSubscriberData,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Tatp(_) => {
            let meta = scheduler.begin(isolation);

            let sub_offset = match database
                .get_table(0)
                .exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id)))
            {
                Ok(offset) => offset,
                Err(e) => {
                    scheduler.abort(&meta, database);
                    return Err(e);
                }
            };

            let mut bit1 = Data::Uint(params.bit_1 as u64);
            scheduler.write_value(&mut bit1, 0, 2, sub_offset, &meta, database)?;

            let sf_offset = match database.get_table(0).exists(PrimaryKey::Tatp(
                TatpPrimaryKey::SpecialFacility(params.s_id, params.sf_type.into()),
            )) {
                Ok(offset) => offset,
                Err(e) => {
                    scheduler.abort(&meta, database);
                    return Err(e);
                }
            };

            let mut data_a = Data::Uint(params.bit_1 as u64);
            scheduler.write_value(&mut data_a, 2, 4, sf_offset, &meta, database)?;

            scheduler.commit(&meta, database, TransactionType::WriteOnly)?;

            Ok(Success::new(meta, None, None, None, None, None))
        }
        _ => panic!("unexpected database"),
    }
}

/// Update location transaction.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn update_location<'a>(
    params: UpdateLocationData,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Tatp(_) => {
            let meta = scheduler.begin(isolation);

            let offset = match database
                .get_table(0)
                .exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id)))
            {
                Ok(offset) => offset,
                Err(e) => {
                    scheduler.abort(&meta, database);
                    return Err(e);
                }
            };

            let mut vlr = Data::Uint(params.vlr_location as u64);
            scheduler.write_value(&mut vlr, 0, 4, offset, &meta, database)?;

            scheduler.commit(&meta, database, TransactionType::WriteOnly)?;

            Ok(Success::new(meta, None, None, None, None, None))
        }
        _ => panic!("unexpected database"),
    }
}
