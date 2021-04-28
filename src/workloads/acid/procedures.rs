use crate::common::error::NonFatalError;
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::acid::paramgen::{
    G0Read, G0Write, G1aRead, G1aWrite, G1cReadWrite, G2itemRead, G2itemWrite, ImpRead, ImpWrite,
    LostUpdateRead, LostUpdateWrite, Otv,
};
use crate::workloads::PrimaryKey;

use std::convert::TryFrom;
use std::sync::Arc;
use std::{thread, time};
use tracing::debug;

/// Dirty Write (G0) TW.
///
/// Append (unique) transaction id to person pair version history.
pub fn g0_write(params: G0Write, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk_p1 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p1_id));
    let pk_p2 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p2_id));
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

    let pk_knows = PrimaryKey::Acid(AcidPrimaryKey::Knows(min, max));
    let value = params.transaction_id.to_string();
    let meta = protocol.scheduler.register().unwrap(); // register

    protocol
        .scheduler
        .append("person", pk_p1, "version_history", &value, meta.clone())?;
    protocol
        .scheduler
        .append("person", pk_p2, "version_history", &value, meta.clone())?;
    protocol
        .scheduler
        .append("knows", pk_knows, "version_history", &value, meta.clone())?;

    protocol.scheduler.commit(meta.clone())?; // commit

    let res = datatype::to_result(None, Some(3), None, None, None).unwrap();

    Ok(res)
}

/// Dirty Write (G0) TR
///
/// Return the version hisotry of person pair.
pub fn g0_read(params: G0Read, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let person_columns: Vec<&str> = vec!["p_id", "version_history"];
    let knows_columns: Vec<&str> = vec!["p1_id", "p2_id", "version_history"];

    let pk_p1 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p1_id));
    let pk_p2 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p2_id));
    let pk_knows = PrimaryKey::Acid(AcidPrimaryKey::Knows(params.p1_id, params.p2_id));

    let meta = protocol.scheduler.register().unwrap();

    let mut r1 = protocol
        .scheduler
        .read("person", pk_p1, &person_columns, meta.clone())?;
    let mut r2 = protocol
        .scheduler
        .read("person", pk_p2, &person_columns, meta.clone())?;
    let mut r3 = protocol
        .scheduler
        .read("knows", pk_knows, &knows_columns, meta.clone())?;

    protocol.scheduler.commit(meta.clone())?;

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
pub fn g1a_read(params: G1aRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["p_id", "version"]; // --- setup
    let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id));

    let meta = protocol.scheduler.register().unwrap(); // --- register
    let values = protocol
        .scheduler
        .read("person", pk, &columns, meta.clone())?; // --- read
    protocol.scheduler.commit(meta.clone())?; // --- commit

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();

    Ok(res)
}

/// Aborted Read (G1a) TW
pub fn g1a_write(params: G1aWrite, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk_sb = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id)); // --- setup
    let columns_sb: Vec<String> = vec!["version".to_string()];
    let values_sb = vec![Data::Int(params.version as i64)];
    let update = |_columns: Vec<String>,
                  _current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let value = match i64::try_from(params[0].clone()) {
            Ok(value) => value,
            Err(e) => {
                return Err(e);
            }
        };

        let new_values = vec![value.to_string()];
        let columns = vec!["version".to_string()];
        Ok((columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap(); // --- register

    protocol.scheduler.update(
        "person",
        pk_sb,
        columns_sb,
        false,
        values_sb,
        &update,
        meta.clone(),
    )?; // --- update

    thread::sleep(time::Duration::from_millis(params.delay)); // artifical delay

    protocol.scheduler.abort(meta.clone()).unwrap(); // --- abort

    Err(NonFatalError::NonSerializable)
}

/// Circular Information Flow (G1c) TRW
pub fn g1c_read_write(
    params: G1cReadWrite,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    // register transaction
    let meta = protocol.scheduler.register().unwrap();

    let pk1 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p1_id));
    let pk2 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p2_id));

    // update
    let columns: Vec<String> = vec!["version".to_string()];
    let values = vec![Data::Int(params.transaction_id as i64)];
    let update = |columns: Vec<String>,
                  _current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let value = match i64::try_from(params[0].clone()) {
            Ok(value) => value,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        }; // parse to i64 from spaghetti data type

        let new_values = vec![value.to_string()]; // convert to string
        Ok((columns, new_values)) // new values for columns
    };

    protocol.scheduler.update(
        "person",
        pk1,
        columns.clone(),
        false,
        values,
        &update,
        meta.clone(),
    )?;

    // read
    let rcolumns: Vec<&str> = vec!["version"];
    let mut values = protocol
        .scheduler
        .read("person", pk2, &rcolumns, meta.clone())?;

    // Commit transaction.
    protocol.scheduler.commit(meta.clone())?;

    values.push(Data::Int(params.transaction_id.into()));

    let frcolumns: Vec<&str> = vec!["version", "transaction_id"];

    let res = datatype::to_result(None, None, None, Some(&frcolumns), Some(&values)).unwrap();
    Ok(res)
}

/// Item-Many-Preceders (IMP) TR
pub fn imp_read(params: ImpRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["version"]; // columns to read
    let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id)); // pk

    let meta = protocol.scheduler.register().unwrap(); // register

    let mut read1 = protocol
        .scheduler
        .read("person", pk.clone(), &columns, meta.clone())?; // read 1

    thread::sleep(time::Duration::from_millis(params.delay)); // artifical delay

    let mut read2 = protocol
        .scheduler
        .read("person", pk, &columns, meta.clone())?; // read 2

    protocol.scheduler.commit(meta.clone())?; // commit

    // merge reads and rename
    let columns: Vec<&str> = vec!["first_read", "second_read"];
    read1.append(&mut read2);

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&read1)).unwrap();

    Ok(res)
}

/// Item-Many-Preceders (IMP) TW
pub fn imp_write(params: ImpWrite, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let meta = protocol.scheduler.register().unwrap(); // register
    let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id)); // key
    let columns: Vec<String> = vec!["version".to_string()]; // columns
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  _params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let current_value = match i64::try_from(current.unwrap()[0].clone()) {
            Ok(value) => value,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        }; // parse to i64 from spaghetti data type
        let nv = current_value + 1; // increment current value
        let new_values = vec![nv.to_string()]; // convert to string
        Ok((columns, new_values)) // new values for columns
    }; // update computation

    let values = vec![Data::Int(params.p_id as i64)]; // TODO: placeholder

    protocol
        .scheduler
        .update("person", pk, columns, true, values, &update, meta.clone())?; //  update

    protocol.scheduler.commit(meta.clone())?; // commit

    let res = datatype::to_result(None, Some(1), None, None, None).unwrap();
    Ok(res)
}

/// Observed Transaction Vanishes (OTV) Write Transaction.
///
/// Selects a disjoint person sequence (length 4) and increments `version` property.
pub fn otv_write(params: Otv, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk1 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p1_id)); // keys
    let pk2 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p2_id));
    let pk3 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p3_id));
    let pk4 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p4_id));
    let keys = vec![pk1, pk2, pk3, pk4];

    let column: Vec<String> = vec!["version".to_string()]; // column to update

    let meta = protocol.scheduler.register().unwrap(); // register

    // calculate new values -- read current value and increment
    let calc = |columns: Vec<String>,
                current: Option<Vec<Data>>,
                _params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let current_value = match i64::try_from(current.unwrap()[0].clone()) {
            Ok(value) => value,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        }; // parse to i64 from spaghetti data type
        let nv = current_value + 1; // increment current value
        let new_values = vec![nv.to_string()]; // convert to string
        Ok((columns, new_values)) // new values for columns
    };

    // TODO: unneeded
    let params = vec![Data::Int(params.p1_id as i64)]; // convert parameters to Vec<Data>

    for pk in keys {
        protocol.scheduler.update(
            "person",
            pk,
            column.clone(),
            true,
            params.clone(),
            &calc,
            meta.clone(),
        )?;
    }
    protocol.scheduler.commit(meta.clone())?; // commit

    let res = datatype::to_result(None, Some(4), None, None, None).unwrap();
    Ok(res)
}

/// Observed Transaction Vanishes (OTV) Read Transaction.
///
/// Person nodes are arranged in disjoint sequences of length 4, e.g., (p1, p2, p3, p4).
/// This transaction reads the `version` property of each person in some disjoint person sequence.
pub fn otv_read(params: Otv, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let pk1 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p1_id)); // keys
    let pk2 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p2_id));
    let pk3 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p3_id));
    let pk4 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p4_id));
    let keys = vec![pk1, pk2, pk3, pk4];

    let column: Vec<&str> = vec!["version"]; // columns to read

    let meta = protocol.scheduler.register().unwrap(); // register

    let mut reads = vec![];

    for key in keys {
        let mut read = protocol
            .scheduler
            .read("person", key.clone(), &column, meta.clone())?; // read
        reads.append(&mut read);
    }
    protocol.scheduler.commit(meta.clone())?; // commit

    let columns: Vec<&str> = vec!["p1_version", "p2_version", "p3_version", "p4_version"];
    let res = datatype::to_result(None, None, None, Some(&columns), Some(&reads)).unwrap();

    Ok(res)
}

pub fn lu_write(params: LostUpdateWrite, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let meta = protocol.scheduler.register().unwrap(); // register
    let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id)); // key
    let columns: Vec<String> = vec!["num_friends".to_string()]; // columns
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  _params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let current_value = match i64::try_from(current.unwrap()[0].clone()) {
            Ok(value) => value,
            Err(e) => {
                return Err(e);
            }
        }; // parse to i64 from spaghetti data type
        let nv = current_value + 1; // increment current value
        let new_values = vec![nv.to_string()]; // convert to string
        Ok((columns, new_values)) // new values for columns
    }; // update computation

    let values = vec![Data::Int(params.p_id as i64)]; // TODO: placeholder

    protocol
        .scheduler
        .update("person", pk, columns, true, values, &update, meta.clone())?; //  update

    protocol.scheduler.commit(meta.clone())?; // commit

    // Note; the person id is needed for the anomaly check so embedding it in the updated field as a workaround
    let res = datatype::to_result(None, Some(params.p_id), None, None, None).unwrap();
    Ok(res)
}

pub fn lu_read(params: LostUpdateRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["p_id", "num_friends"]; // columns to read
    let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id)); // pk

    let meta = protocol.scheduler.register().unwrap(); // register
    let read = protocol
        .scheduler
        .read("person", pk.clone(), &columns, meta.clone())?; // read
    protocol.scheduler.commit(meta.clone())?; // commit

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&read)).unwrap();

    Ok(res)
}

/// G2-item/Write Skew Write Transaction.
///
/// Selects a disjoint person pair, retrieves `value` properties, if sum >= 100; then decrement a person in the pair's
/// value by 100.
pub fn g2_item_write(
    params: G2itemWrite,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let pk1 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p1_id)); // key
    let pk2 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p2_id));
    let read_column: Vec<&str> = vec!["value"]; // column
    let write_column: Vec<String> = vec!["value".to_string()]; // column

    let meta = protocol.scheduler.register().unwrap(); // register

    if params.p_id_update == params.p1_id {
        // read p2 for value
        let read = protocol
            .scheduler
            .read("person", pk2.clone(), &read_column, meta.clone())?; // read 1

        // takes in current value of p1 and value of p2 via params
        let calc = |columns: Vec<String>,
                    current: Option<Vec<Data>>,
                    params: Vec<Data>|
         -> Result<(Vec<String>, Vec<String>), NonFatalError> {
            let current_value = match i64::try_from(current.unwrap()[0].clone()) {
                Ok(value) => value,
                Err(e) => {
                    return Err(e);
                }
            };

            let other_value = match i64::try_from(params[0].clone()) {
                Ok(value) => value,
                Err(e) => {
                    return Err(e);
                }
            };

            if current_value + other_value < 100 {
                return Err(NonFatalError::NonSerializable);
            }

            // TODO: sleep

            let nv = current_value - 100;
            let new_values = vec![nv.to_string()]; // convert to string
            Ok((columns, new_values)) // new values for columns
        };

        protocol.scheduler.update(
            "person",
            pk1.clone(),
            write_column,
            true,
            read,
            &calc,
            meta.clone(),
        )?;
    } else {
        let read = protocol
            .scheduler
            .read("person", pk1.clone(), &read_column, meta.clone())?; // read 1

        // takes in current value of p1 and value of p2 via params
        let calc = |columns: Vec<String>,
                    current: Option<Vec<Data>>,
                    params: Vec<Data>|
         -> Result<(Vec<String>, Vec<String>), NonFatalError> {
            let current_value = match i64::try_from(current.unwrap()[0].clone()) {
                Ok(value) => value,
                Err(e) => {
                    return Err(e);
                }
            };

            let other_value = match i64::try_from(params[0].clone()) {
                Ok(value) => value,
                Err(e) => {
                    return Err(e);
                }
            };

            if current_value + other_value < 100 {
                return Err(NonFatalError::NonSerializable);
            }

            // TODO: sleep

            let nv = current_value - 100;
            let new_values = vec![nv.to_string()]; // convert to string
            Ok((columns, new_values)) // new values for columns
        };

        protocol.scheduler.update(
            "person",
            pk2.clone(),
            write_column,
            true,
            read,
            &calc,
            meta.clone(),
        )?;
    }

    protocol.scheduler.commit(meta.clone())?; // commit

    let res = datatype::to_result(None, Some(1), None, None, None).unwrap();
    Ok(res)
}

pub fn g2_item_read(params: G2itemRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["value"]; // columns to read
    let pk1 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p1_id)); // pk
    let pk2 = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p2_id)); // pk

    let meta = protocol.scheduler.register().unwrap(); // register
    let mut read1 = protocol
        .scheduler
        .read("person", pk1.clone(), &columns, meta.clone())?; // read
    let mut read2 = protocol
        .scheduler
        .read("person", pk2.clone(), &columns, meta.clone())?; // read
    protocol.scheduler.commit(meta.clone())?; // commit

    read1.append(&mut read2);

    let columns: Vec<&str> = vec!["p1_value", "p2_value"];
    let res = datatype::to_result(None, None, None, Some(&columns), Some(&read1)).unwrap();

    Ok(res)
}
