use crate::common::error::NonFatalError;
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::acid::paramgen::{
    G1aRead, G1aWrite, G1cReadWrite, ImpRead, ImpWrite, LostUpdateRead, LostUpdateWrite,
};
use crate::workloads::PrimaryKey;

use std::convert::TryFrom;
use std::sync::Arc;
use std::{thread, time};
use tracing::debug;

/// Aborted Read (G1a) TR
pub fn g1a_read(params: G1aRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["p_id", "version"];
    let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id));

    let handle = thread::current();
    debug!("Thread {}: register", handle.name().unwrap());
    let meta = protocol.scheduler.register().unwrap();
    debug!("Thread {}: read", handle.name().unwrap());
    let values = protocol
        .scheduler
        .read("person", pk, &columns, meta.clone())?;
    debug!("Thread {}: commit", handle.name().unwrap());

    protocol.scheduler.commit(meta.clone())?;

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();

    Ok(res)
}

/// Aborted Read (G1a) TW
pub fn g1a_write(params: G1aWrite, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let meta = protocol.scheduler.register().unwrap();

    let pk_sb = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id));
    let columns_sb: Vec<String> = vec!["version".to_string()];
    let values_sb = vec![Data::Int(params.version as i64)];
    let update = |_columns: Vec<String>,
                  _current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let value = match i64::try_from(params[0].clone()) {
            Ok(value) => value,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };

        let new_values = vec![value.to_string()];
        let columns = vec!["version".to_string()];
        Ok((columns, new_values))
    };

    protocol.scheduler.update(
        "person",
        pk_sb,
        columns_sb,
        false,
        values_sb,
        &update,
        meta.clone(),
    )?;

    thread::sleep(time::Duration::from_millis(params.delay)); // artifical delay

    // Abort transaction.
    protocol.scheduler.abort(meta.clone()).unwrap();

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

pub fn lu_read(params: LostUpdateRead, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["p_id", "num_friends"]; // columns to read
    let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(params.p_id)); // pk

    let meta = protocol.scheduler.register().unwrap(); // register
    let mut read = protocol
        .scheduler
        .read("person", pk.clone(), &columns, meta.clone())?; // read
    protocol.scheduler.commit(meta.clone())?; // commit

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&read)).unwrap();

    Ok(res)
}
