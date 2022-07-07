use crate::common::{
    error::NonFatalError, statistics::local::LocalStatistics, stats_bucket::StatsBucket,
    value_id::ValueId,
};
use crate::scheduler::Scheduler;
use crate::storage::{datatype::Data, Database};
use crate::workloads::ycsb::paramgen::{Operation, Operations};

pub fn transaction<'a>(
    meta: &mut StatsBucket,
    operations: Operations,
    scheduler: &'a Scheduler,
    database: &'a Database,
    stats: &mut LocalStatistics,
) -> Result<(), NonFatalError> {
    for operation in operations.get_operations() {
        match operation {
            Operation::Read(offset) => {
                let vid = ValueId::new(0, 2, offset);
                scheduler.read_value(vid, meta, database, stats)?;
            }
            Operation::Update(offset, value) => {
                let vid = ValueId::new(0, 1, offset);
                let value = &mut Data::VarChar(value);
                scheduler.write_value(value, vid, meta, database, stats)?;
            }
        }
    }

    Ok(())
}
