use crate::common::error::NonFatalError;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{OperationResult, Row, State as RowState};
use crate::workloads::PrimaryKey;

//use chashmap::{CHashMap, ReadGuard};
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
    // map: CHashMap<PrimaryKey, Mutex<Row>>,
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

    /// Insert a `Row` into the index.
    ///
    /// # Errors
    ///
    /// - Row already exists with `key`.
    pub fn insert(&self, key: PrimaryKey, row: Row) -> Result<(), NonFatalError> {
        let res = self.map.insert(key.clone(), Mutex::new(row));

        match res {
            Some(existing_row) => {
                // A row already existed for this key, which has now been overwritten, put back.
                self.map.insert(key.clone(), existing_row);
                Err(NonFatalError::RowAlreadyExists(
                    format!("{}", key),
                    self.get_name(),
                ))
            }
            None => Ok(()),
        }
    }

    /// Delete a `Row` in the index.
    ///
    /// Note, this does not delete anything, merely marking the `Row` as to be deleted.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    /// - Row already dirty or marked for delete.
    pub fn delete(
        &self,
        key: PrimaryKey,
        protocol: &str,
    ) -> Result<OperationResult, NonFatalError> {
        let read_guard = self
            .map
            .get(&key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?;

        let row = &mut *read_guard.lock();

        let res = row.delete(protocol)?;
        Ok(res)
    }

    /// Remove a `Row` from the index.
    ///
    /// Called at commit time, removing the row from the index.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    pub fn remove(&self, key: PrimaryKey) -> Result<Row, NonFatalError> {
        // Remove the row from the map.
        let row = self.map.remove(&key);
        match row {
            Some((_, row)) => Ok(row.into_inner()),
            None => Err(NonFatalError::RowNotFound(
                format!("{}", key),
                self.get_name(),
            )),
        }
    }

    /// Read `columns` from a `Row` with the given `key`.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    /// - The row is marked for delete.
    pub fn read(
        &self,
        key: PrimaryKey,
        columns: &[&str],
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        let read_guard = self
            .map
            .get(&key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?;

        let row = &mut *read_guard.lock();

        let res = row.get_values(columns, protocol, tid)?;
        Ok(res)
    }

    /// Attempt to get a read handle to a row with `key`.
    pub fn get_lock_on_row(
        &self,
        key: &PrimaryKey,
    ) -> Result<Ref<PrimaryKey, Mutex<Row>>, NonFatalError> {
        self.map
            .get(key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))
    }

    /// Write `values` to `columns` in a `Row` with the given `key`.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    /// - The row is already dirty.
    /// - The row is marked for delete.
    pub fn update<F>(
        &self,
        key: PrimaryKey,
        columns: Vec<String>,
        read: bool,
        params: Vec<Data>,
        f: F,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError>
    where
        F: Fn(
            Vec<String>,
            Option<Vec<Data>>,
            Vec<Data>,
        ) -> Result<(Vec<String>, Vec<String>), NonFatalError>,
    {
        let read_guard = self
            .map
            .get(&key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?;
        let row = &mut *read_guard.lock();

        let c: Vec<&str> = columns.iter().map(|s| s as &str).collect();
        let current;
        if read {
            let res = row.get_values(&c, protocol, tid)?;
            current = res.get_values();
        } else {
            current = None;
        }

        let (_, new_values) = f(columns.clone(), current, params)?;
        let nv: Vec<&str> = new_values.iter().map(|s| s as &str).collect();

        let res = row.set_values(&c, &nv, protocol, tid)?;
        Ok(res)
    }

    /// Append `value` to `column` in a `Row` with the given `key`.
    ///
    /// # NonFatalErrors
    ///
    /// (i) Row does not exist with `key`, (ii) row is dirty, or (iii) row is marked for delete
    pub fn append(
        &self,
        key: PrimaryKey,
        column: &str,
        value: &str,
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        let read_guard = self
            .map
            .get(&key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?; //  attempt to get read guard
        let row = &mut *read_guard.lock(); // deref to row
        let res = row.append_value(column, value, protocol, tid)?; // execute append
        Ok(res)
    }

    /// Set `values` in `columns` in a `Row` with the given `key`, returning the old values.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    /// - The row is already dirty.
    /// - The row is marked for delete.
    pub fn read_and_update(
        &self,
        key: PrimaryKey,
        columns: &[&str],
        values: &[&str],
        protocol: &str,
        tid: &str,
    ) -> Result<OperationResult, NonFatalError> {
        let read_guard = self
            .map
            .get(&key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?;

        let row = &mut *read_guard.lock();

        let res = row.get_and_set_values(columns, values, protocol, tid)?;
        Ok(res)
    }

    /// Commit modifications to a `Row`.
    ///
    /// If the row was marked for deletion it is removed.
    /// Else it was updated and these changes are made permanent.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    pub fn commit(&self, key: PrimaryKey, protocol: &str, tid: &str) -> Result<(), NonFatalError> {
        let row_state = self
            .map
            .get(&key)
            .unwrap_or_else(|| panic!("No entry for {}", key))
            .lock()
            .get_state();

        if let RowState::Deleted = row_state {
            self.remove(key).unwrap();
        } else {
            let read_guard = self
                .map
                .get(&key)
                .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?;

            let row = &mut *read_guard.lock();

            row.commit(protocol, tid);
        }
        Ok(())
    }

    /// Revert modifications to a `Row`.
    ///
    /// This is handled at the row level.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    pub fn revert(&self, key: PrimaryKey, protocol: &str, tid: &str) -> Result<(), NonFatalError> {
        // attempt to get read guard
        let read_guard = self
            .map
            .get(&key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?;

        let row = &mut *read_guard.lock(); // Deref to row.
        row.revert(protocol, tid); // Revert changes.
        Ok(())
    }

    /// Revert reads to a `Row`.
    ///
    /// SGT only.
    ///
    /// # Errors
    ///
    /// - Row does not exist with `key`.
    pub fn revert_read(&self, key: PrimaryKey, tid: &str) -> Result<(), NonFatalError> {
        let read_guard = self
            .map
            .get(&key)
            .ok_or_else(|| NonFatalError::RowNotFound(format!("{}", key), self.get_name()))?;

        let row = &mut *read_guard.lock();

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

        // 4. Test format.
        assert_eq!(
            format!("{}", workload.get_internals().get_index("sub_idx").unwrap()),
            "[sub_idx,10]"
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
