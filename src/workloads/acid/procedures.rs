use crate::common::error::NonFatalError;
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::acid::keys::AcidPrimaryKey::*;
use crate::workloads::acid::paramgen::{
    G0Read, G0Write, G1aRead, G1aWrite, G1cReadWrite, G2itemRead, G2itemWrite, ImpRead, ImpWrite,
    LostUpdateRead, LostUpdateWrite, Otv,
};
use crate::workloads::PrimaryKey::Acid;

use std::convert::TryFrom;
use std::sync::Arc;
use std::{thread, time};

/// Dirty Write (G0) TW.
///
/// Append (unique) transaction id to person pair version history.
pub fn g0_write(params: G0Write, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk_p1 = Acid(Person(params.p1_id));
    let pk_p2 = Acid(Person(params.p2_id));

    // flip for knows edge; 1,0 becomes 0,1
    let min;
    let max;
    if params.p1_id > params.p2_id {
        min = params.p2_id;
        max = params.p1_id;
    } else {
        min = params.p1_id;
        max = params.p2_id;
    }
    let pk_knows = Acid(Knows(min, max));

    let value = Data::Uint(params.transaction_id as u64);

    let meta = protocol.scheduler.register().unwrap();

    protocol
        .scheduler
        .append("person", &pk_p1, "version_history", value.clone(), &meta)?;

    protocol
        .scheduler
        .append("person", &pk_p2, "version_history", value.clone(), &meta)?;

    protocol
        .scheduler
        .append("knows", &pk_knows, "version_history", value.clone(), &meta)?;

    protocol.scheduler.commit(&meta)?; // --- commit

    let res = datatype::to_result(None, Some(3), None, None, None).unwrap();

    Ok(res)
}

/// Dirty Write (G0) TR
///
/// Return the version history of person pair.
pub fn g0_read(params: G0Read, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let person_columns: Vec<&str> = vec!["p_id", "version_history"];
    let knows_columns: Vec<&str> = vec!["p1_id", "p2_id", "version_history"];

    let pk_p1 = Acid(Person(params.p1_id));
    let pk_p2 = Acid(Person(params.p2_id));
    let pk_knows = Acid(Knows(params.p1_id, params.p2_id));

    let meta = protocol.scheduler.register().unwrap(); // --- register

    let mut r1 = protocol
        .scheduler
        .read("person", &pk_p1, &person_columns, &meta)?; // --- read

    let mut r2 = protocol
        .scheduler
        .read("person", &pk_p2, &person_columns, &meta)?; // --- read

    let mut r3 = protocol
        .scheduler
        .read("knows", &pk_knows, &knows_columns, &meta)?; // --- read

    protocol.scheduler.commit(&meta)?; // --- commit

    let columns: Vec<&str> = vec![
        "p1_id",
        "p1_version_history",
        "p2_id",
        "p2_version_history",
        "knows_p1_id",
        "knows_p2_id",
        "knows_version_history",
    ];
    r1.append(&mut r2);
    r1.append(&mut r3);

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&r1)).unwrap();

    Ok(res)
}

/// Aborted Read (G1a) TR
///
/// Returns the version of a given persion
pub fn g1a_read(params: G1aRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns = ["p_id", "version"];
    let pk = Acid(Person(params.p_id));

    let meta = protocol.scheduler.register().unwrap(); // --- register

    let values = protocol.scheduler.read("person", &pk, &columns, &meta)?; // --- read

    protocol.scheduler.commit(&meta)?; // --- commit

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();

    Ok(res)
}

/// Aborted Read (G1a) TW
///
/// Set the version to an even number before aborting.
pub fn g1a_write(params: G1aWrite, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk = Acid(Person(params.p_id));
    let columns = ["version"];
    let delay = params.delay;
    let params = vec![Data::Uint(params.version)];

    let update_version = |columns: &[&str],
                          _current: Option<Vec<Data>>,
                          params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let new_values = vec![params.unwrap()[0].clone()];
        Ok((new_columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap(); // --- register

    protocol.scheduler.update(
        "person",
        &pk,
        &columns,
        false,
        Some(&params),
        &update_version,
        &meta,
    )?; // --- update

    thread::sleep(time::Duration::from_millis(delay)); // --- artifical delay

    protocol.scheduler.abort(&meta).unwrap(); // --- abort

    Err(NonFatalError::NonSerializable)
}

/// Circular Information Flow (G1c) TRW
///
/// This transaction writes a version a person then read the value of another person, returning the version read.
/// From this a WR dependency can be inferred and a dependency graph constructed.
pub fn g1c_read_write(
    params: G1cReadWrite,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let pk1 = Acid(Person(params.p1_id)); // --- setup
    let pk2 = Acid(Person(params.p2_id));
    let columns = ["version"];
    let tid = params.transaction_id as i64;
    let params = vec![Data::Uint(params.transaction_id as u64)];

    let update_version = |columns: &[&str],
                          _current: Option<Vec<Data>>,
                          params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let new_values = vec![params.unwrap()[0].clone()];
        Ok((new_columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap(); // --- register

    protocol.scheduler.update(
        "person",
        &pk1,
        &columns,
        false,
        Some(&params),
        &update_version,
        &meta,
    )?; // --- update

    let mut values = protocol.scheduler.read("person", &pk2, &columns, &meta)?; // --- read

    protocol.scheduler.commit(&meta)?; // --- commit

    values.push(Data::Int(tid)); // --- result
    let frcolumns: Vec<&str> = vec!["version", "transaction_id"];
    let res = datatype::to_result(None, None, None, Some(&frcolumns), Some(&values)).unwrap();

    Ok(res)
}

/// Item-Many-Preceders (IMP) Read Transaction.
///
/// Returns the version of a given person.
pub fn imp_read(params: ImpRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns = ["version"]; // --- setup
    let pk = Acid(Person(params.p_id));

    let meta = protocol.scheduler.register().unwrap(); // --- register

    let mut read1 = protocol.scheduler.read("person", &pk, &columns, &meta)?; // --- read 1

    thread::sleep(time::Duration::from_millis(params.delay)); // --- artifical delay

    let mut read2 = protocol.scheduler.read("person", &pk, &columns, &meta)?; // --- read 2

    protocol.scheduler.commit(&meta)?; // --- commit

    let columns: Vec<&str> = vec!["first_read", "second_read"]; // --- result
    read1.append(&mut read2);
    let res = datatype::to_result(None, None, None, Some(&columns), Some(&read1)).unwrap();

    Ok(res)
}

/// Item-Many-Preceders (IMP) Write Transaction.
///
/// Increments the version of a person.
pub fn imp_write(params: ImpWrite, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk = Acid(Person(params.p_id)); // --- setup
    let columns = ["version"];

    let inc_version = |columns: &[&str],
                       current: Option<Vec<Data>>,
                       _params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let current_value = u64::try_from(current.unwrap()[0].clone())?;
        let new_values = vec![Data::Uint(current_value + 1)];
        Ok((new_columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap(); // --- register

    protocol
        .scheduler
        .update("person", &pk, &columns, true, None, &inc_version, &meta)?; //  ---  update

    protocol.scheduler.commit(&meta)?; // --- commit

    let res = datatype::to_result(None, Some(1), None, None, None).unwrap(); // --- result
    Ok(res)
}

/// Observed Transaction Vanishes (OTV) Write Transaction.
///
/// Selects a disjoint person sequence (length 4) and increments `version` property.
pub fn otv_write(params: Otv, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk1 = Acid(Person(params.p1_id)); // keys
    let pk2 = Acid(Person(params.p2_id));
    let pk3 = Acid(Person(params.p3_id));
    let pk4 = Acid(Person(params.p4_id));

    let keys = vec![pk1, pk2, pk3, pk4];

    let column = ["version"];

    let inc_version = |columns: &[&str],
                       current: Option<Vec<Data>>,
                       _params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let current_value = u64::try_from(current.unwrap()[0].clone())?;
        let new_values = vec![Data::Uint(current_value + 1)];
        Ok((new_columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap(); // register

    for pk in keys {
        protocol
            .scheduler
            .update("person", &pk, &column, true, None, &inc_version, &meta)?;
    }

    protocol.scheduler.commit(&meta)?; // commit

    let res = datatype::to_result(None, Some(4), None, None, None).unwrap();
    Ok(res)
}

/// Observed Transaction Vanishes (OTV) Read Transaction.
///
/// Person nodes are arranged in disjoint sequences of length 4, e.g., (p1, p2, p3, p4).
/// This transaction reads the `version` property of each person in some disjoint person sequence.
pub fn otv_read(params: Otv, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk1 = Acid(Person(params.p1_id)); // keys
    let pk2 = Acid(Person(params.p2_id));
    let pk3 = Acid(Person(params.p3_id));
    let pk4 = Acid(Person(params.p4_id));
    let keys = vec![pk1, pk2, pk3, pk4];

    let column = ["version"]; // columns to read

    let meta = protocol.scheduler.register().unwrap(); // register

    let mut reads = vec![];

    for key in keys {
        let mut read = protocol.scheduler.read("person", &key, &column, &meta)?; // read
        reads.append(&mut read);
    }

    protocol.scheduler.commit(&meta)?; // commit

    let columns: Vec<&str> = vec!["p1_version", "p2_version", "p3_version", "p4_version"];
    let res = datatype::to_result(None, None, None, Some(&columns), Some(&reads)).unwrap();

    Ok(res)
}

/// Lost Update (LU) Write Transaction.
pub fn lu_write(params: LostUpdateWrite, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk = Acid(Person(params.p_id)); // key
    let columns = ["num_friends"]; // columns

    let inc_version = |columns: &[&str],
                       current: Option<Vec<Data>>,
                       _params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let current_value = u64::try_from(current.unwrap()[0].clone())?;
        let new_values = vec![Data::Uint(current_value + 1)];
        Ok((new_columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap(); // register

    protocol
        .scheduler
        .update("person", &pk, &columns, true, None, &inc_version, &meta)?; //  update

    protocol.scheduler.commit(&meta)?; // commit

    // Note; the person id is needed for the anomaly check so embedding it in the updated field as a workaround
    let res = datatype::to_result(None, Some(params.p_id), None, None, None).unwrap();
    Ok(res)
}

/// Lost Update (LU) Read Transaction.
pub fn lu_read(params: LostUpdateRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["p_id", "num_friends"]; // columns to read
    let pk = Acid(Person(params.p_id)); // pk

    let meta = protocol.scheduler.register().unwrap(); // register

    let read = protocol.scheduler.read("person", &pk, &columns, &meta)?; // read

    protocol.scheduler.commit(&meta)?; // commit

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&read)).unwrap();

    Ok(res)
}

/// Write Skew (G2-item) Write Transaction.
///
/// Selects a disjoint person pair, retrieves `value` properties, if sum >= 100; then decrement a person in the pair's
/// value by 100.
pub fn g2_item_write(
    params: G2itemWrite,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let pk1 = Acid(Person(params.p1_id)); // key
    let pk2 = Acid(Person(params.p2_id));
    let column = ["value"]; // column

    let meta = protocol.scheduler.register().unwrap(); // register

    // takes in current value of p1 and value of p2 via params
    let deduct = |columns: &[&str],
                  current: Option<Vec<Data>>,
                  params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let current_value = i64::try_from(current.unwrap()[0].clone())?;
        let other_value = i64::try_from(params.unwrap()[0].clone())?;

        if current_value + other_value < 100 {
            return Err(NonFatalError::NonSerializable);
        }

        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let new_values = vec![Data::Int(current_value - 100)];
        Ok((new_columns, new_values))
    };

    if params.p_id_update == params.p1_id {
        let read = protocol.scheduler.read("person", &pk2, &column, &meta)?;

        protocol
            .scheduler
            .update("person", &pk1, &column, true, Some(&read), &deduct, &meta)?;
    } else {
        let read = protocol.scheduler.read("person", &pk1, &column, &meta)?;

        protocol
            .scheduler
            .update("person", &pk2, &column, true, Some(&read), &deduct, &meta)?;
    }

    protocol.scheduler.commit(&meta)?; // commit

    let res = datatype::to_result(None, Some(1), None, None, None).unwrap();
    Ok(res)
}

/// Write Skew (G2-item) Read Transaction.
///
/// TODO
pub fn g2_item_read(params: G2itemRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns = ["value"]; // columns to read
    let pk1 = Acid(Person(params.p1_id)); // pk
    let pk2 = Acid(Person(params.p2_id)); // pk

    let meta = protocol.scheduler.register().unwrap(); // register

    let mut read1 = protocol.scheduler.read("person", &pk1, &columns, &meta)?; // read

    let mut read2 = protocol.scheduler.read("person", &pk2, &columns, &meta)?; // read

    protocol.scheduler.commit(&meta)?; // commit

    read1.append(&mut read2);

    let columns: Vec<&str> = vec!["p1_value", "p2_value"];
    let res = datatype::to_result(None, None, None, Some(&columns), Some(&read1)).unwrap();

    Ok(res)
}
