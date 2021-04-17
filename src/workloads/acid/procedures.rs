use crate::common::error::NonFatalError;
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::acid::paramgen::{G1aRead, G1aWrite};
use crate::workloads::PrimaryKey;

use std::convert::TryFrom;
use std::sync::Arc;
use std::{thread, time};

use tracing::debug;

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

    let ten_millis = time::Duration::from_millis(100);

    thread::sleep(ten_millis);

    // Abort transaction.
    protocol.scheduler.abort(meta.clone()).unwrap();

    Err(NonFatalError::RowDirty("1".to_string(), "2".to_string()))

    // Ok("{\"updated 1 row.\"}".to_string())
}
