use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::scheduler::{Scheduler, TransactionType};
//use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::storage::PrimaryKey;
use crate::workloads::tatp::keys::TatpPrimaryKey;
// use crate::workloads::tatp::paramgen::{
//     GetAccessData, GetNewDestination, GetSubscriberData, UpdateLocationData, UpdateSubscriberData,
// };
use crate::workloads::tatp::paramgen::GetSubscriberData;
use crate::workloads::IsolationLevel;

use crossbeam_epoch as epoch;

//use std::convert::TryFrom;

/// GetSubscriberData transaction.
pub fn get_subscriber_data<'a>(
    params: GetSubscriberData,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Tatp(_) => {
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(isolation); // register

            let offset = database
                .get_table(0)
                .exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id)))?; // find offset
            scheduler.read_value(0, 0, offset, &meta, database, guard)?;
            scheduler.read_value(0, 1, offset, &meta, database, guard)?;
            scheduler.read_value(0, 2, offset, &meta, database, guard)?;
            scheduler.read_value(0, 3, offset, &meta, database, guard)?;
            scheduler.read_value(0, 4, offset, &meta, database, guard)?;
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?;

            Ok(Success::new(None, None, None, None, None))
        }
        _ => panic!("unexpected database"),
    }
}

// /// GetNewDestination transaction.
// pub fn get_new_destination(
//     params: GetNewDestination,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     let sf_columns = ["s_id", "sf_type", "is_active"];
//     let cf_columns = ["s_id", "sf_type", "start_time", "end_time", "number_x"];

//     let sf_pk = Tatp(SpecialFacility(params.s_id, params.sf_type.into()));
//     let cf_pk = Tatp(CallForwarding(
//         params.s_id,
//         params.sf_type.into(),
//         params.start_time.into(),
//     ));

//     let meta = protocol.scheduler.register().unwrap();

//     let sf_res = protocol.scheduler.read(
//         "special_facility",
//         Some("special_idx"),
//         &sf_pk,
//         &sf_columns,
//         &meta,
//     )?;

//     let is_active = i64::try_from(sf_res[2].clone()).unwrap();

//     if is_active != 1 {
//         protocol.scheduler.abort(&meta).unwrap();
//         return Err(NonFatalError::RowNotFound(
//             sf_pk.to_string(),
//             "special_facility".to_string(),
//         ));
//     }

//     let cf_res = protocol.scheduler.read(
//         "call_forwarding",
//         Some("call_idx"),
//         &cf_pk,
//         &cf_columns,
//         &meta,
//     )?;

//     let end_time = i64::try_from(cf_res[3].clone()).unwrap();

//     if params.end_time as i64 >= end_time {
//         protocol.scheduler.abort(&meta).unwrap();
//         return Err(NonFatalError::RowNotFound(
//             cf_pk.to_string(),
//             "call_forwarding".to_string(),
//         ));
//     }

//     protocol.scheduler.commit(&meta)?;

//     Ok("ok".to_string())
// }

// /// GetAccessData transaction.
// pub fn get_access_data(
//     params: GetAccessData,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     let columns = ["data_1", "data_2", "data_3", "data_4"];

//     let pk = Tatp(AccessInfo(params.s_id, params.ai_type.into()));

//     let meta = protocol.scheduler.register().unwrap();

//     protocol
//         .scheduler
//         .read("access_info", Some("access_idx"), &pk, &columns, &meta)?;

//     protocol.scheduler.commit(&meta)?;

//     Ok("ok".to_string())
// }

// /// Update subscriber transaction.
// pub fn update_subscriber_data(
//     params: UpdateSubscriberData,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     let pk1 = Tatp(Subscriber(params.s_id));
//     let pk2 = Tatp(SpecialFacility(params.s_id, params.sf_type.into()));

//     let columns1 = ["bit_1"];
//     let columns2 = ["data_a"];

//     let values1 = vec![Data::Int(params.bit_1.into())];
//     let values2 = vec![Data::Int(params.data_a.into())];

//     let update = |_current: Option<Vec<Data>>,
//                   params: Option<&[Data]>|
//      -> Result<Vec<Data>, NonFatalError> {
//         Ok(vec![params.unwrap()[0].clone().clone()])
//     };

//     let meta = protocol.scheduler.register().unwrap();

//     protocol.scheduler.update(
//         "subscriber",
//         Some("sub_idx"),
//         &pk1,
//         &columns1,
//         None,
//         Some(&values1),
//         &update,
//         &meta,
//     )?;

//     protocol.scheduler.update(
//         "special_facility",
//         Some("special_idx"),
//         &pk2,
//         &columns2,
//         None,
//         Some(&values2),
//         &update,
//         &meta,
//     )?;

//     protocol.scheduler.commit(&meta)?;

//     Ok("ok".to_string())
// }

// /// Update location transaction.
// pub fn update_location(
//     params: UpdateLocationData,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     let columns = ["vlr_location"];
//     let pk = Tatp(Subscriber(params.s_id));
//     let params = vec![Data::Int(params.vlr_location.into())];

//     let update_vlr = |_current: Option<Vec<Data>>,
//                       params: Option<&[Data]>|
//      -> Result<Vec<Data>, NonFatalError> {
//         Ok(vec![params.unwrap()[0].clone().clone()])
//     };

//     let meta = protocol.scheduler.register().unwrap();

//     protocol.scheduler.update(
//         "subscriber",
//         Some("sub_idx"),
//         &pk,
//         &columns,
//         None,
//         Some(&params),
//         &update_vlr,
//         &meta,
//     )?;

//     protocol.scheduler.commit(&meta)?;

//     Ok("ok".to_string())
// }
