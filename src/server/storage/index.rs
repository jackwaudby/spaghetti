use crate::common::error::SpaghettiError;
use crate::server::storage::row::OperationResult;
use crate::server::storage::row::Row;
use crate::workloads::PrimaryKey;
use crate::Result;

use chashmap::CHashMap;
use std::fmt;
use std::sync::Mutex;

/// An `Index` is used to access data.
///
/// Each table has at least 1 `Index`, which owns all `Row`s stored in that table.
#[derive(Debug)]
pub struct Index {
    /// Index name.
    name: String,
    /// Concurrrent hashmap.
    map: CHashMap<PrimaryKey, Mutex<Row>>,
}

impl Index {
    /// Create a new `Index`.
    pub fn init(name: &str) -> Self {
        Index {
            name: String::from(name),
            map: CHashMap::new(),
        }
    }

    /// Get shared reference to map.
    pub fn get_map(&self) -> &CHashMap<PrimaryKey, Mutex<Row>> {
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
    /// If `Row` already exists for key an `RowAlreadyExists` entry is returned.
    pub fn index_insert(&self, key: PrimaryKey, row: Row) -> Result<()> {
        let res = self.map.insert(key, Mutex::new(row));

        match res {
            Some(existing_row) => {
                // A row already existed for this pk, which has now been overwritten, put back
                self.map.insert(key, existing_row);
                Err(Box::new(SpaghettiError::RowAlreadyExists))
            }
            None => Ok(()),
        }
    }

    /// Remove a `Row` from the index.
    ///
    /// # Errors
    ///
    /// If `Row` does not exists for key a `RowDoesNotExist` error is returned.
    pub fn index_remove(&self, key: PrimaryKey, protocol: &str) -> Result<OperationResult> {
        // Remove the row from the map.
        let row = self.map.remove(&key);

        match row {
            Some(row) => {
                match protocol {
                    "sgt" => {
                        // Get access history.
                        let ah = row.lock().unwrap().get_access_history()?;
                        let res = OperationResult::new(None, Some(ah));
                        return Ok(res);
                    }
                    _ => {
                        let res = OperationResult::new(None, None);
                        return Ok(res);
                    }
                };
            }
            None => return Err(Box::new(SpaghettiError::RowDoesNotExist)),
        }
    }

    /// Read `columns` from a `Row` with the given `key`.
    ///
    /// # Errors
    ///
    /// `RowDoesNotexist` if the row does not exist in the index.
    pub fn index_read(
        &self,
        key: PrimaryKey,
        columns: &Vec<&str>,
        protocol: &str,
        transaction_id: &str,
    ) -> Result<OperationResult> {
        // Attempt to get read guard.
        let read_guard = self
            .map
            .get(&key)
            .ok_or(Box::new(SpaghettiError::RowDoesNotExist))?;
        // Deref to row.
        let row = &mut *read_guard.lock().unwrap();
        // Execute read operation.
        let res = row.get_values(columns, protocol, transaction_id)?;

        Ok(res)
    }

    /// Write `values` to `columns`.
    pub fn index_write(
        &self,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        protocol: &str,
        transaction_id: &str,
    ) -> Result<OperationResult> {
        // Attempt to get write guard.
        let write_guard = self
            .map
            .get(&key)
            .ok_or(Box::new(SpaghettiError::RowDoesNotExist))?;
        // Deref to row.
        let row = &mut *write_guard.lock().unwrap();
        // Execute write operation.
        let res = row.set_values(columns, values, protocol, transaction_id)?;

        Ok(res)
    }

    pub fn index_commit(
        &self,
        key: PrimaryKey,
        protocol: &str,
        transaction_id: &str,
    ) -> Result<()> {
        // Attempt to get read guard.
        let read_guard = self
            .map
            .get(&key)
            .ok_or(Box::new(SpaghettiError::RowDoesNotExist))?;
        // Deref to row.
        let row = &mut *read_guard.lock().unwrap();
        // Commit changes.
        row.commit(protocol, transaction_id);

        Ok(())
    }

    pub fn index_revert(
        &self,
        key: PrimaryKey,
        protocol: &str,
        transaction_id: &str,
    ) -> Result<()> {
        // Attempt to get read guard.
        let read_guard = self
            .map
            .get(&key)
            .ok_or(Box::new(SpaghettiError::RowDoesNotExist))?;
        // Deref to row.
        let row = &mut *read_guard.lock().unwrap();
        // Commit changes.
        row.revert(protocol, transaction_id);

        Ok(())
    }
}

impl fmt::Display for Index {
    /// Format: [name,num_rows].
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{},{}]", self.name, self.get_map().len())
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
        c.merge(config::File::with_name("Test.toml")).unwrap();
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
                    .index_insert(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), row)
                    .unwrap_err()
            ),
            format!("row already exists in index.")
        );

        // 2. Remove entry that is not there.
        assert_eq!(
            format!(
                "{}",
                workload
                    .get_internals()
                    .get_index("sub_idx")
                    .unwrap()
                    .index_remove(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(0)), "2pl")
                    .unwrap_err()
            ),
            format!("row does not exist in index.")
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
            "[sub_idx,1]"
        );

        // 5. Successful read of entry.
        let cols = vec!["bit_4", "byte_2_5"];
        assert_eq!(
            datatype::to_result(
                &cols,
                &workload
                    .get_internals()
                    .get_index("sub_idx")
                    .unwrap()
                    .index_read(
                        PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                        &cols,
                        "2pl",
                        "t1"
                    )
                    .unwrap()
                    .get_values()
                    .unwrap()
            )
            .unwrap(),
            "{bit_4=\"1\", byte_2_5=\"205\"}"
        );

        // 6. Successful write of entry.
        let cols = vec!["bit_4", "byte_2_5"];
        let vals = vec!["0", "69"];
        workload
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .index_write(
                PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                &cols,
                &vals,
                "2pl",
                "t1",
            )
            .unwrap();

        let cols = vec!["bit_4", "byte_2_5"];
        assert_eq!(
            datatype::to_result(
                &cols,
                &workload
                    .get_internals()
                    .get_index("sub_idx")
                    .unwrap()
                    .index_read(
                        PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                        &cols,
                        "2pl",
                        "t1"
                    )
                    .unwrap()
                    .get_values()
                    .unwrap()
            )
            .unwrap(),
            "{bit_4=\"0\", byte_2_5=\"69\"}"
        );
    }
}
