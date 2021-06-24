use crate::common::error::NonFatalError;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::{self, Data};
use crate::storage::Database;
use crate::workloads::acid::paramgen::{
    G0Read, G0Write, G1aRead, G1aWrite, G1cReadWrite, G2itemRead, G2itemWrite, ImpRead, ImpWrite,
    LostUpdateRead, LostUpdateWrite, Otv,
};

use crossbeam_epoch as epoch;
use std::convert::TryFrom;
use std::sync::Arc;
use std::{thread, time};

/// Dirty Write (G0) TW.
///
/// Append (unique) transaction id to person pair version history.
pub fn g0_write<'a>(
    params: G0Write,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    // let pk_p1 = Acid(Person(params.p1_id));
    // let pk_p2 = Acid(Person(params.p2_id));

    // // flip for knows edge; 1,0 becomes 0,1
    // let min;
    // let max;
    // if params.p1_id > params.p2_id {
    //     min = params.p2_id;
    //     max = params.p1_id;
    // } else {
    //     min = params.p1_id;
    //     max = params.p2_id;
    // }
    // let pk_knows = Acid(Knows(min, max));

    // let value = Data::Uint(params.transaction_id as u64);

    // let meta = protocol.scheduler.register().unwrap();

    // protocol.scheduler.append(
    //     "person",
    //     Some("person_idx"),
    //     &pk_p1,
    //     "version_history",
    //     value.clone(),
    //     &meta,
    // )?;

    // protocol.scheduler.append(
    //     "person",
    //     Some("person_idx"),
    //     &pk_p2,
    //     "version_history",
    //     value.clone(),
    //     &meta,
    // )?;

    // protocol.scheduler.append(
    //     "knows",
    //     Some("knows_idx"),
    //     &pk_knows,
    //     "version_history",
    //     value.clone(),
    //     &meta,
    // )?;

    // protocol.scheduler.commit(&meta)?; // --- commit

    // let res = datatype::to_result(None, Some(3), None, None, None).unwrap();

    // Ok(res)
    Err(NonFatalError::NonSerializable)
}

/// Dirty Write (G0) TR
///
/// Return the version history of person pair.
pub fn g0_read<'a>(
    params: G0Read,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    // let person_columns: Vec<&str> = vec!["p_id", "version_history"];
    // let knows_columns: Vec<&str> = vec!["p1_id", "p2_id", "version_history"];

    // let pk_p1 = Acid(Person(params.p1_id));
    // let pk_p2 = Acid(Person(params.p2_id));
    // let pk_knows = Acid(Knows(params.p1_id, params.p2_id));

    // let meta = protocol.scheduler.register().unwrap(); // --- register

    // let mut r1 =
    //     protocol
    //         .scheduler
    //         .read("person", Some("person_idx"), &pk_p1, &person_columns, &meta)?; // --- read

    // let mut r2 =
    //     protocol
    //         .scheduler
    //         .read("person", Some("person_idx"), &pk_p2, &person_columns, &meta)?; // --- read

    // let mut r3 = protocol.scheduler.read(
    //     "knows",
    //     Some("person_idx"),
    //     &pk_knows,
    //     &knows_columns,
    //     &meta,
    // )?; // --- read

    // protocol.scheduler.commit(&meta)?; // --- commit

    // let columns: Vec<&str> = vec![
    //     "p1_id",
    //     "p1_version_history",
    //     "p2_id",
    //     "p2_version_history",
    //     "knows_p1_id",
    //     "knows_p2_id",
    //     "knows_version_history",
    // ];
    // r1.append(&mut r2);
    // r1.append(&mut r3);

    // let res = datatype::to_result(None, None, None, Some(&columns), Some(&r1)).unwrap();

    // Ok(res)
    Err(NonFatalError::NonSerializable)
}

/// Aborted Read (G1a) TR
///
/// Returns the version of a given persion
pub fn g1a_read<'a>(
    params: G1aRead,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(); // register
            let pid = scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get p_id
            let version = scheduler.read_value(0, 1, offset, &meta, database, guard)?; // get version
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = datatype::to_result(
                None,
                None,
                None,
                Some(&["p_id", "version"]),
                Some(&vec![pid, version]),
            )
            .unwrap();
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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let delay = params.delay;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(); // register
            let val = Data::Uint(params.version);
            scheduler.write_value(&val, 0, 1, offset, &meta, database, guard)?; // set to 2
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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let tid = Data::Uint(params.transaction_id as u64);

            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            scheduler.write_value(&tid, 0, 1, offset1, &meta, database, guard)?; // set to txn id
            let version = scheduler.read_value(0, 1, offset2, &meta, database, guard)?; // read txn id
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            let res = datatype::to_result(
                None,
                None,
                None,
                Some(&["version", "transaction_id"]),
                Some(&vec![version, tid]),
            )
            .unwrap();

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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let read1 = scheduler.read_value(0, 1, offset, &meta, database, guard)?;
            thread::sleep(time::Duration::from_millis(params.delay)); // --- artifical delay
            let read2 = scheduler.read_value(0, 1, offset, &meta, database, guard)?;
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = datatype::to_result(
                None,
                None,
                None,
                Some(&["first_read", "second_read"]),
                Some(&vec![read1, read2]),
            )
            .unwrap();

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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let version = scheduler.read_value(0, 1, offset, &meta, database, guard)?; // get friends
            let new = u64::try_from(version)? + 1;
            scheduler.write_value(&Data::Uint(new), 0, 1, offset, &meta, database, guard)?; // increment
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            let res = datatype::to_result(None, Some(1), None, None, None).unwrap();
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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let offset3 = params.p3_id as usize;
            let offset4 = params.p4_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let version1 = scheduler.read_value(0, 1, offset1, &meta, database, guard)?;
            let version2 = scheduler.read_value(0, 1, offset2, &meta, database, guard)?;
            let version3 = scheduler.read_value(0, 1, offset3, &meta, database, guard)?;
            let version4 = scheduler.read_value(0, 1, offset4, &meta, database, guard)?;

            let new1 = u64::try_from(version1)? + 1;
            let new2 = u64::try_from(version2)? + 1;
            let new3 = u64::try_from(version3)? + 1;
            let new4 = u64::try_from(version4)? + 1;

            scheduler.write_value(&Data::Uint(new1), 0, 1, offset1, &meta, database, guard)?;
            scheduler.write_value(&Data::Uint(new2), 0, 1, offset2, &meta, database, guard)?;
            scheduler.write_value(&Data::Uint(new3), 0, 1, offset3, &meta, database, guard)?;
            scheduler.write_value(&Data::Uint(new4), 0, 1, offset4, &meta, database, guard)?;

            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            let res = datatype::to_result(None, Some(4), None, None, None).unwrap();

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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let offset3 = params.p3_id as usize;
            let offset4 = params.p4_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let version1 = scheduler.read_value(0, 1, offset1, &meta, database, guard)?;
            let version2 = scheduler.read_value(0, 1, offset2, &meta, database, guard)?;
            let version3 = scheduler.read_value(0, 1, offset3, &meta, database, guard)?;
            let version4 = scheduler.read_value(0, 1, offset4, &meta, database, guard)?;
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = datatype::to_result(
                None,
                None,
                None,
                Some(&["p1_version", "p2_version", "p3_version", "p4_version"]),
                Some(&vec![version1, version2, version3, version4]),
            )
            .unwrap();

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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let friends = scheduler.read_value(0, 2, offset, &meta, database, guard)?; // get friends
            let new = u64::try_from(friends)? + 1;
            let version =
                scheduler.write_value(&Data::Uint(new), 0, 2, offset, &meta, database, guard)?; // increment
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?; // commit

            // Note; the person id is needed for the anomaly check so embedding it in the updated field as a workaround
            let res = datatype::to_result(None, Some(params.p_id), None, None, None).unwrap();
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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset = params.p_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let pid = scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get p_id
            let friends = scheduler.read_value(0, 2, offset, &meta, database, guard)?; // get version
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = datatype::to_result(
                None,
                None,
                None,
                Some(&["p_id", "num_friends"]),
                Some(&vec![pid, friends]),
            )
            .unwrap();

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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let bal1 = u64::try_from(scheduler.read_value(0, 3, offset1, &meta, database, guard)?)?; // get p1 balance
            let bal2 = u64::try_from(scheduler.read_value(0, 3, offset2, &meta, database, guard)?)?; // get p2 balance
            let sum = bal1 + bal2;

            // if sum across accounts if less than 100 abort
            if sum < 100 {
                scheduler.abort(&meta, database, guard);
                return Err(NonFatalError::NonSerializable);
            }
            // else subtract 100 from one person
            if params.p_id_update == params.p1_id {
                let new = bal1 - sum; // subtract 100 from p1
                scheduler.write_value(&Data::from(sum), 0, 3, offset1, &meta, database, guard)?;
            } else {
                let new = bal2 - sum; // subtract 100 from p2
                scheduler.write_value(&Data::from(sum), 0, 3, offset2, &meta, database, guard)?;
            }

            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?;

            let res = datatype::to_result(None, Some(1), None, None, None).unwrap();
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
) -> Result<String, NonFatalError> {
    match &*database {
        Database::Acid(_) => {
            let offset1 = params.p1_id as usize;
            let offset2 = params.p2_id as usize;
            let guard = &epoch::pin(); // pin thread

            let meta = scheduler.begin(); // register
            let bal1 = scheduler.read_value(0, 3, offset1, &meta, database, guard)?; // get p1 balance
            let bal2 = scheduler.read_value(0, 3, offset2, &meta, database, guard)?; // get p2 balance
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            let res = datatype::to_result(
                None,
                None,
                None,
                Some(&["p1_value", "p2_value"]),
                Some(&vec![bal1, bal2]),
            )
            .unwrap();
            Ok(res)
        }
        _ => panic!("unexpected database"),
    }
}
