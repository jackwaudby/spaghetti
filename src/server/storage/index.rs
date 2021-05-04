use crate::common::error::NonFatalError;
use crate::server::scheduler::TransactionInfo;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{OperationResult, Row, State as RowState};
use crate::workloads::PrimaryKey;

use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::fmt;

/// An `Index` is used to access data.
///
/// Each table has at least 1 `Index`, which owns all `Row`s stored in that table.
#[derive(Debug)]
pub struct Index {
    /// Index name.
    name: String,

    /// Concurrrent hashmap.
    map: DashMap<PrimaryKey, Mutex<Row>>,
}

impl Index {
    /// Create a new `Index`.
    pub fn init(name: &str) -> Self {
        Index {
            name: String::from(name),
            map: DashMap::new(),
        }
    }

    /// Get shared reference to map.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Get shared reference to map.
    pub fn get_map(&self) -> &DashMap<PrimaryKey, Mutex<Row>> {
        &self.map
    }

    /// Check if a key exists in the index.
    pub fn key_exists(&self, key: PrimaryKey) -> bool {
        self.map.contains_key(&key)
    }

    /// Insert a row with key into the index.
    pub fn insert(&self, key: &PrimaryKey, row: Row) -> Result<(), NonFatalError> {
        let res = self.map.insert(key.clone(), Mutex::new(row));

        match res {
            Some(existing_row) => {
                self.map.insert(key.clone(), existing_row); // row exists for key so put back
                Err(NonFatalError::RowAlreadyExists(
                    key.to_string(),
                    self.get_name(),
                ))
            }
            None => Ok(()),
        }
    }

    /// Mark row as deleted in the index.
    pub fn delete(
        &self,
        key: &PrimaryKey,
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let row = &mut *rh.lock();
        let res = row.delete(tid)?;
        Ok(res)
    }

    /// Remove a `Row` from the index.
    ///
    /// Called at commit time, removing the row from the index.
    pub fn remove(&self, key: &PrimaryKey) -> Result<Row, NonFatalError> {
        let row = self.map.remove(key);
        match row {
            Some((_, row)) => Ok(row.into_inner()),
            None => Err(NonFatalError::RowNotFound(key.to_string(), self.get_name())),
        }
    }

    /// Read columns from a row with the given key.
    pub fn read(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;

        let row = &mut *rh.lock();
        let res = row.get_values(columns, tid)?;
        Ok(res)
    }

    /// Get a handle to row with key.
    pub fn get_lock_on_row(
        &self,
        key: &PrimaryKey,
    ) -> Result<Ref<PrimaryKey, Mutex<Row>>, NonFatalError> {
        self.map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))
    }

    /// Write values to columns in a row with the given key.
    pub fn update<F>(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
        read: bool,
        params: Option<&[Data]>,
        f: F,
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError>
    where
        F: Fn(
            &[&str],
            Option<Vec<Data>>,
            Option<&[Data]>,
        ) -> Result<(Vec<String>, Vec<Data>), NonFatalError>,
    {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let row = &mut *rh.lock();

        let current_values;
        if read {
            let res = row.get_values(columns, tid)?;
            current_values = Some(res.get_values());
        } else {
            current_values = None;
        }

        let (new_columns, new_values) = f(columns, current_values, params)?;

        let res = row.set_values(&new_columns, &new_values, tid)?;
        Ok(res)
    }

    /// Append value to column in a row with the given key.
    pub fn append(
        &self,
        key: &PrimaryKey,
        column: &str,
        value: Data,
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let row = &mut *rh.lock();
        let res = row.append_value(column, value, tid)?;
        Ok(res)
    }

    /// Set values in columns in a row with the given key, returning the old values.
    pub fn read_and_update(
        &self,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        tid: &TransactionInfo,
    ) -> Result<OperationResult, NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let row = &mut *rh.lock();
        let res = row.get_and_set_values(columns, values, tid)?;
        Ok(res)
    }

    /// Commit modifications to a row - rows marked for delete are removed.
    pub fn commit(&self, key: &PrimaryKey, tid: &TransactionInfo) -> Result<(), NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;

        let row_state = rh.lock().get_state();

        if let RowState::Deleted = row_state {
            drop(rh); // drop handle
            self.remove(key).unwrap(); // remove
        } else {
            let row = &mut *rh.lock(); // lock row
            row.commit(tid); // commit
        }
        Ok(())
    }

    /// Revert modifications to a row.
    pub fn revert(&self, key: &PrimaryKey, tid: &TransactionInfo) -> Result<(), NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let row = &mut *rh.lock();
        row.revert(tid);
        Ok(())
    }

    /// Revert reads to a `Row`.
    pub fn revert_read(
        &self,
        key: &PrimaryKey,
        tid: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let rh = self
            .map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
        let row = &mut *rh.lock();
        row.revert_read(tid);
        Ok(())
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.map).unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::storage::datatype;
    use crate::workloads::tatp::keys::TatpPrimaryKey;
    use crate::workloads::Workload;
    use config::Config;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::Arc;

    #[test]
    fn index_test() {
        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-tpl.toml"))
            .unwrap();
        let config = Arc::new(c);
        // Initalise workload.
        let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());
        let mut rng = StdRng::seed_from_u64(42);
        workload.populate_tables(&mut rng).unwrap();

        // 1. Insert entry that already exists.
        // Create dummy row in table
        let table = workload.get_internals().get_table("subscriber").unwrap();
        let row = Row::new(Arc::clone(&table), "2pl");
        assert_eq!(
            format!(
                "{}",
                workload
                    .get_internals()
                    .get_index("sub_idx")
                    .unwrap()
                    .insert(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), row)
                    .unwrap_err()
            ),
            format!("already exists: Subscriber(1) in sub_idx")
        );

        // 2. Remove entry that is not there.
        assert_eq!(
            format!(
                "{}",
                workload
                    .get_internals()
                    .get_index("sub_idx")
                    .unwrap()
                    .remove(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(0)))
                    .unwrap_err()
            ),
            format!("not found: Subscriber(0) in sub_idx")
        );

        // 3. Check entry exists.
        assert_eq!(
            workload
                .get_internals()
                .get_index("sub_idx")
                .unwrap()
                .key_exists(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(0))),
            false
        );

        // 5. Successful read of entry.
        let cols = vec!["bit_4", "byte_2_5"];
        assert_eq!(
            datatype::to_result(
                None,
                None,
                None,
                Some(&cols),
                Some(
                    &workload
                        .get_internals()
                        .get_index("sub_idx")
                        .unwrap()
                        .read(
                            PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                            &cols,
                            "2pl",
                            "t1"
                        )
                        .unwrap()
                        .get_values()
                        .unwrap()
                )
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"bit_4\":\"1\",\"byte_2_5\":\"205\"}}"
        );

        // 6. Successful write of entry.
        // let cols = vec!["bit_4", "byte_2_5"];
        // let vals = vec!["0", "69"];
        // workload
        //     .get_internals()
        //     .get_index("sub_idx")
        //     .unwrap()
        //     .update(
        //         PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
        //         &cols,
        //         &vals,
        //         "2pl",
        //         "t1",
        //     )
        //     .unwrap();

        // let cols = vec!["bit_4", "byte_2_5"];
        // assert_eq!(
        //     datatype::to_result(
        //         &cols,
        //         &workload
        //             .get_internals()
        //             .get_index("sub_idx")
        //             .unwrap()
        //             .read(
        //                 PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
        //                 &cols,
        //                 "2pl",
        //                 "t1"
        //             )
        //             .unwrap()
        //             .get_values()
        //             .unwrap()
        //     )
        //     .unwrap(),
        //     "{bit_4=\"0\", byte_2_5=\"69\"}"
        // );
    }
}
