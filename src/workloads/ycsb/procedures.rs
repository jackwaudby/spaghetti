use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::ycsb::paramgen::{Operation, Operations};
use crate::workloads::IsolationLevel;

pub fn transaction<'a>(
    operations: Operations,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Ycsb(_) => {
            let meta = scheduler.begin(isolation);

            for operation in operations.get_operations() {
                use Operation::*;
                match operation {
                    Read(offset) => {
                        scheduler.read_value(0, 2, offset, &meta, database)?;
                    }
                    Update(offset, value) => {
                        let new_value = &mut Data::VarChar(value);
                        scheduler.write_value(new_value, 0, 1, offset, &meta, database)?;
                    }
                }
            }

            scheduler.commit(&meta, database, TransactionType::WriteOnly)?;

            Ok(Success::default(meta))
        }
        _ => panic!("unexpected database"),
    }
}
