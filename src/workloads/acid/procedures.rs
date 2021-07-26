use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::acid::paramgen::{
    G0Read, G0Write, G1aRead, G1aWrite, G1cReadWrite, G2itemRead, G2itemWrite, ImpRead, ImpWrite,
    LostUpdateRead, LostUpdateWrite, Otv,
};
use crate::workloads::IsolationLevel;

use crossbeam_epoch as epoch;
use std::convert::TryFrom;
use std::{thread, time};

/// Dirty Write (G0) TW.
///
/// Append (unique) transaction id to Person pair's versionHistory.
pub fn g0_write<'a>(
    params: G0Write,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let delay = params.delay;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(IsolationLevel::Serializable); // register

            // Need to two copies as append operation uses append() which moves element out of the supplied vec
            let val1 = &mut Data::List(vec![Data::Uint(params.transaction_id.into())]);
            let val2 = &mut Data::List(vec![Data::Uint(params.transaction_id.into())]);
            scheduler.write_value(val1, 0, 4, offset1, &meta, database, guard)?;
            thread::sleep(time::Duration::from_millis(delay)); // --- artifical delay
            scheduler.write_value(val2, 0, 4, offset2, &meta, database, guard)?; // TODO: check append operation
            scheduler.commit(&meta, database, guard, TransactionType::WriteOnly)?; // commit

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

/// Dirty Write (G0) TR
///
/// Return the version history of person pair.
pub fn g0_read<'a>(
    params: G0Read,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let pid1 = scheduler.read_value(0, 0, offset1, &meta, database, guard)?; // get p_id1
            let pid2 = scheduler.read_value(0, 0, offset2, &meta, database, guard)?; // get p_id2

            let version_history1 = scheduler.read_value(0, 4, offset1, &meta, database, guard)?; // get versionhistory1
            let version_history2 = scheduler.read_value(0, 4, offset2, &meta, database, guard)?; // get versionhistory2

            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = Success::new(
                meta,
                None,
                None,
                None,
                Some(&["p1_id", "p1_version_history", "p2_id", "p2_version_history"]),
                Some(&vec![pid1, version_history1, pid2, version_history2]),
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Aborted Read (G1a) TR
///
/// Returns the version of a given persion
pub fn g1a_read<'a>(
    params: G1aRead,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let pid = scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get p_id
            let version = scheduler.read_value(0, 1, offset, &meta, database, guard)?; // get version
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = Success::new(
                meta,
                None,
                None,
                None,
                Some(&["p_id", "version"]),
                Some(&vec![pid, version]),
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Aborted Read (G1a) TW
///
/// Set the version to an even number before aborting.
pub fn g1a_write<'a>(
    params: G1aWrite,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let delay = params.delay;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let val = &mut Data::Uint(params.version);
            scheduler.write_value(val, 0, 1, offset, &meta, database, guard)?; // set to 2
            thread::sleep(time::Duration::from_millis(delay)); // --- artifical delay
            scheduler.abort(&meta, database, guard);
            Err(NonFatalError::NonSerializable)
        }
        _ => panic!("unexpected database"),
    }
}

/// Circular Information Flow (G1c) TRW
///
/// This transaction writes a version of a person then read the value of another person, returning the version read.
/// From this a WR dependency can be inferred and a dependency graph constructed.
pub fn g1c_read_write<'a>(
    params: G1cReadWrite,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let mut tid = Data::Uint(params.transaction_id as u64);

            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            scheduler.write_value(&mut tid, 0, 1, offset1, &meta, database, guard)?; // set to txn id
            let version = scheduler.read_value(0, 1, offset2, &meta, database, guard)?; // read txn id
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            let res = Success::new(
                meta,
                None,
                None,
                None,
                Some(&["version", "transaction_id"]),
                Some(&vec![version, tid]),
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Item-Many-Preceders (IMP) Read Transaction.
///
/// Returns the version of a given person.
pub fn imp_read<'a>(
    params: ImpRead,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let read1 = scheduler.read_value(0, 1, offset, &meta, database, guard)?;
            thread::sleep(time::Duration::from_millis(params.delay)); // --- artifical delay
            let read2 = scheduler.read_value(0, 1, offset, &meta, database, guard)?;
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = Success::new(
                meta,
                None,
                None,
                None,
                Some(&["first_read", "second_read"]),
                Some(&vec![read1, read2]),
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Item-Many-Preceders (IMP) Write Transaction.
///
/// Increments the version of a person.
pub fn imp_write<'a>(
    params: ImpWrite,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let version = scheduler.read_value(0, 1, offset, &meta, database, guard)?; // get friends
            let new = u64::try_from(version)? + 1;
            scheduler.write_value(&mut Data::Uint(new), 0, 1, offset, &meta, database, guard)?; // increment
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            let res = Success::new(meta, None, Some(vec![(0, offset)]), None, None, None);

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Observed Transaction Vanishes (OTV) Write Transaction.
///
/// Selects a disjoint person sequence (length 4) and increments `version` property.
pub fn otv_write<'a>(
    params: Otv,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let offset3 = params.p3_id as usize;
            let offset4 = params.p4_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let version1 = scheduler.read_value(0, 1, offset1, &meta, database, guard)?;
            let version2 = scheduler.read_value(0, 1, offset2, &meta, database, guard)?;
            let version3 = scheduler.read_value(0, 1, offset3, &meta, database, guard)?;
            let version4 = scheduler.read_value(0, 1, offset4, &meta, database, guard)?;

            let new1 = u64::try_from(version1)? + 1;
            let new2 = u64::try_from(version2)? + 1;
            let new3 = u64::try_from(version3)? + 1;
            let new4 = u64::try_from(version4)? + 1;

            scheduler.write_value(&mut Data::Uint(new1), 0, 1, offset1, &meta, database, guard)?;
            scheduler.write_value(&mut Data::Uint(new2), 0, 1, offset2, &meta, database, guard)?;
            scheduler.write_value(&mut Data::Uint(new3), 0, 1, offset3, &meta, database, guard)?;
            scheduler.write_value(&mut Data::Uint(new4), 0, 1, offset4, &meta, database, guard)?;

            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            let res = Success::new(
                meta,
                None,
                Some(vec![(0, offset1), (0, offset2), (0, offset3), (0, offset4)]),
                None,
                None,
                None,
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Observed Transaction Vanishes (OTV) Read Transaction.
///
/// Person nodes are arranged in disjoint sequences of length 4, e.g., (p1, p2, p3, p4).
/// This transaction reads the `version` property of each person in some disjoint person sequence.
pub fn otv_read<'a>(
    params: Otv,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let offset3 = params.p3_id as usize;
            let offset4 = params.p4_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let version1 = scheduler.read_value(0, 1, offset1, &meta, database, guard)?;
            let version2 = scheduler.read_value(0, 1, offset2, &meta, database, guard)?;
            let version3 = scheduler.read_value(0, 1, offset3, &meta, database, guard)?;
            let version4 = scheduler.read_value(0, 1, offset4, &meta, database, guard)?;
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = Success::new(
                meta,
                None,
                None,
                None,
                Some(&["p1_version", "p2_version", "p3_version", "p4_version"]),
                Some(&vec![version1, version2, version3, version4]),
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Lost Update (LU) Write Transaction.
///
/// Increments the number of friends by 1.
pub fn lu_write<'a>(
    params: LostUpdateWrite,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let friends = scheduler.read_value(0, 2, offset, &meta, database, guard)?; // get friends
            let new = u64::try_from(friends)? + 1;
            scheduler.write_value(&mut Data::Uint(new), 0, 2, offset, &meta, database, guard)?; // increment
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            // Note; the person id is needed for the anomaly check so embedding it in the updated field as a workaround

            // TODO: this now breaks
            let res = Success::new(
                meta,
                None,
                Some(vec![(0, params.p_id as usize)]),
                None,
                None,
                None,
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Lost Update (LU) Read Transaction.
///
/// Returns the person id and number of friends.
pub fn lu_read<'a>(
    params: LostUpdateRead,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let pid = scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get p_id
            let friends = scheduler.read_value(0, 2, offset, &meta, database, guard)?; // get version
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = Success::new(
                meta,
                None,
                None,
                None,
                Some(&["p_id", "num_friends"]),
                Some(&vec![pid, friends]),
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Write Skew (G2-item) Write Transaction.
///
/// Selects a disjoint person pair, retrieves `value` properties, if sum >= 100; then decrement a person in the pair's
/// value by 100.
pub fn g2_item_write<'a>(
    params: G2itemWrite,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let delay = params.delay;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let bal1 = i64::try_from(scheduler.read_value(0, 3, offset1, &meta, database, guard)?)?; // get p1 balance
            let bal2 = i64::try_from(scheduler.read_value(0, 3, offset2, &meta, database, guard)?)?; // get p2 balance
            let sum = bal1 + bal2;

            // if sum across accounts if less than 100 abort
            if sum < 100 {
                scheduler.abort(&meta, database, guard);
                return Err(NonFatalError::NonSerializable);
            }

            thread::sleep(time::Duration::from_millis(delay)); // --- artifical delay

            // else subtract 100 from one person
            let updated;

            if params.p_id_update == params.p1_id {
                let new = &mut Data::Int(bal1 - 100); // subtract 100 from p1
                scheduler.write_value(new, 0, 3, offset1, &meta, database, guard)?;
                updated = offset1;
            } else {
                let new = &mut Data::Int(bal2 - 100); // subtract 100 from p2
                scheduler.write_value(new, 0, 3, offset2, &meta, database, guard)?;
                updated = offset2;
            }

            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?;

            let res = Success::new(meta, None, Some(vec![(0, updated)]), None, None, None);
            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}

/// Write Skew (G2-item) Read Transaction.
///
/// Reads the balances of two persons.
pub fn g2_item_read<'a>(
    params: G2itemRead,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(IsolationLevel::Serializable); // register
            let bal1 = scheduler.read_value(0, 3, offset1, &meta, database, guard)?; // get p1 balance
            let bal2 = scheduler.read_value(0, 3, offset2, &meta, database, guard)?; // get p2 balance
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = Success::new(
                meta,
                None,
                None,
                None,
                Some(&["p1_value", "p2_value"]),
                Some(&vec![bal1, bal2]),
            );

            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}
