use crate::common::{error::NonFatalError, stats_bucket::StatsBucket, value_id::ValueId};
use crate::scheduler::Scheduler;
use crate::storage::{datatype::Data, Database, PrimaryKey};
use crate::workloads::tatp::{
    keys::TatpPrimaryKey,
    paramgen::{
        GetAccessData, GetNewDestination, GetSubscriberData, UpdateLocationData,
        UpdateSubscriberData,
    },
};

use std::convert::TryFrom;

pub fn get_subscriber_data<'a>(
    meta: &mut StatsBucket,
    params: GetSubscriberData,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset = database
        .get_table(0)
        .exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id)))?;

    for column in 0..=4 {
        let vid = ValueId::new(0, column, offset);
        scheduler.read_value(vid, meta, database)?;
    }

    Ok(())
}

pub fn get_new_destination<'a>(
    meta: &mut StatsBucket,
    params: GetNewDestination,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let sf_offset =
        database
            .get_table(2)
            .exists(PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
                params.s_id,
                params.sf_type.into(),
            )))?;

    let vid = ValueId::new(2, 0, sf_offset);
    scheduler.read_value(vid, meta, database)?;

    let vid = ValueId::new(2, 1, sf_offset);
    scheduler.read_value(vid, meta, database)?;

    let vid = ValueId::new(2, 2, sf_offset);
    let is_active = u64::try_from(scheduler.read_value(vid, meta, database)?)?;

    if is_active != 1 {
        return Err(NonFatalError::RowNotFound);
    }

    let cf_offset =
        database
            .get_table(3)
            .exists(PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
                params.s_id,
                params.sf_type.into(),
                params.start_time.into(),
            )))?;

    let vid = ValueId::new(3, 0, cf_offset);
    scheduler.read_value(vid, meta, database)?;

    let vid = ValueId::new(3, 1, cf_offset);
    scheduler.read_value(vid, meta, database)?;

    let vid = ValueId::new(3, 2, cf_offset);
    scheduler.read_value(vid, meta, database)?;

    let vid = ValueId::new(3, 3, cf_offset);
    let end_time = u64::try_from(scheduler.read_value(vid, meta, database)?)?;

    let vid = ValueId::new(3, 4, cf_offset);
    scheduler.read_value(vid, meta, database)?;

    if params.end_time >= end_time {
        return Err(NonFatalError::RowNotFound);
    }

    Ok(())
}

/// GetAccessData transaction.
pub fn get_access_data<'a>(
    meta: &mut StatsBucket,
    params: GetAccessData,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset = database
        .get_table(1)
        .exists(PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(
            params.s_id,
            params.ai_type.into(),
        )))?;

    for column in 2..=5 {
        let vid = ValueId::new(1, column, offset);
        scheduler.read_value(vid, meta, database)?;
    }

    Ok(())
}

/// Update subscriber transaction.
pub fn update_subscriber_data<'a>(
    meta: &mut StatsBucket,
    params: UpdateSubscriberData,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let sub_offset = database
        .get_table(0)
        .exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id)))?;

    let bit1 = &mut Data::Uint(params.bit_1);
    let vid1 = ValueId::new(0, 2, sub_offset);
    scheduler.write_value(bit1, vid1, meta, database)?;

    let sf_offset =
        database
            .get_table(2)
            .exists(PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
                params.s_id,
                params.sf_type.into(),
            )))?;

    let data_a = &mut Data::Uint(params.data_a);
    let vid2 = ValueId::new(2, 4, sf_offset);
    scheduler.write_value(data_a, vid2, meta, database)?;

    Ok(())
}

pub fn update_location<'a>(
    meta: &mut StatsBucket,
    params: UpdateLocationData,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset = database
        .get_table(0)
        .exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id)))?;

    let value = &mut Data::Uint(params.vlr_location);
    let vid = ValueId::new(0, 4, offset);
    scheduler.write_value(value, vid, meta, database)?;

    Ok(())
}
