use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::ycsb::paramgen::{Operation, Operations};
use crate::workloads::IsolationLevel;

use tracing::debug;

pub fn transaction<'a>(
    operations: Operations,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Ycsb(_) => {
            debug!("start");
            let meta = scheduler.begin(isolation);

            for op in operations.0 {
                match op {
                    // Read all columns
                    Operation::Read(offset) => {
                        debug!("read: {}", offset);
                        // for i in 1..10 {
                        //     scheduler.read_value(0, i, offset, &meta, database)?;
                        // }

                        scheduler.read_value(0, 2, offset, &meta, database)?;
                    }
                    // Update 1 column
                    Operation::Update(offset, value) => {
                        debug!("write: {}", offset);
                        let new = &mut Data::VarChar(value);
                        scheduler.write_value(new, 0, 1, offset, &meta, database)?;
                    }
                }
            }

            scheduler.commit(&meta, database, TransactionType::WriteOnly)?;

            let res = Success::new(meta, None, None, None, None, None);
            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}
