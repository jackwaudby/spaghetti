use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::dummy::paramgen::Write;
use crate::workloads::IsolationLevel;

pub fn write_only<'a>(
    params: Write,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Dummy(_) => {
            let offset1 = params.offset1;
            let offset2 = params.offset2;
            let val = &mut Data::Uint(params.value);

            let meta = scheduler.begin(IsolationLevel::Serializable);

            scheduler.write_value(val, 0, 0, offset1, &meta, database)?;
            scheduler.write_value(val, 0, 0, offset2, &meta, database)?;

            scheduler.commit(&meta, database, TransactionType::WriteOnly)?;

            let res = Success::new(
                meta,
                None,
                Some(vec![(0, offset1), (0, offset2)]),
                None,
                None,
                None,
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}
