use crate::common::stats_bucket::StatsBucket;
use crate::common::{error::NonFatalError, value_id::ValueId};
use crate::scheduler::Scheduler;
use crate::storage::{datatype::Data, Database};
use crate::workloads::ycsb::paramgen::{Operation, Operations};

pub fn transaction<'a>(
    meta: &mut StatsBucket,
    operations: Operations,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    for operation in operations.get_operations() {
        match operation {
            Operation::Read(offset) => {
                let vid = ValueId::new(0, 2, offset);
                scheduler.read_value(vid, meta, database)?;
            }
            Operation::Update(offset, value) => {
                let vid = ValueId::new(0, 1, offset);
                let value = &mut Data::VarChar(value);
                scheduler.write_value(value, vid, meta, database)?;
            }
        }
    }

    Ok(())
}
