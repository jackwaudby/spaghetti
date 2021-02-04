use crate::common::error::SpaghettiError;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::Row;
use crate::workloads::PrimaryKey;
use crate::Result;

use chashmap::CHashMap;
use std::fmt;

/// An `Index` is used to access data.
///
/// Each table has at least 1 `Index`, which owns all `Row`s stored in that table.
#[derive(Debug)]
pub struct Index {
    /// Index name.
    name: String,
    /// Concurrrent hashmap.
    i: CHashMap<PrimaryKey, Row>,
}

impl Index {
    /// Create a new `Index`.
    pub fn init(name: &str) -> Self {
        Index {
            name: String::from(name),
            i: CHashMap::new(),
        }
    }

    /// Check if a key exists in the index.
    pub fn key_exists(&self, key: PrimaryKey) -> bool {
        self.i.contains_key(&key)
    }

    /// Insert a `Row` into the index.
    ///
    /// # Errors
    ///
    /// If `Row` already exists for key an `RowAlreadyExists` entry is returned.
    pub fn index_insert(&self, key: PrimaryKey, row: Row) -> Result<()> {
        let res = self.i.insert(key, row);

        match res {
            Some(existing_row) => {
                // A row already existed for this pk, which has now been overwritten, put back
                self.i.insert(key, existing_row);
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
    pub fn index_remove(&self, key: PrimaryKey) -> Result<()> {
        let res = self.i.remove(&key);

        match res {
            Some(_) => Ok(()),
            None => Err(SpaghettiError::RowDoesNotExist.into()),
        }
    }

    /// Read `columns` from a `Row` with the given `key`.
    ///
    /// # Errors
    ///
    /// `RowDoesNotexist` if the row does not exist in the index.
    pub fn index_read(&self, key: PrimaryKey, columns: &Vec<&str>) -> Result<Vec<Data>> {
        // Attempt to get read guard.
        let read_guard = self
            .i
            .get(&key)
            .ok_or(Box::new(SpaghettiError::RowDoesNotExist))?;
        // Deref to row.
        let row = &*read_guard;

        let mut res = Vec::new();
        for column in columns {
            let value = row.get_value(column)?;
            res.push(value);
        }

        Ok(res)
    }

    /// Write `values` to `columns`.
    pub fn index_write(
        &self,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
    ) -> Result<()> {
        // Attempt to get write guard.
        let mut write_guard = self
            .i
            .get_mut(&key)
            .ok_or(Box::new(SpaghettiError::RowDoesNotExist))?;
        // Deref to row.
        let row = &mut *write_guard;

        for (i, name) in columns.iter().enumerate() {
            row.set_value(name, values[i])?;
        }
        Ok(())
    }
}

impl fmt::Display for Index {
    /// Format: [name,num_rows].
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{},{}]", self.name, self.i.len())
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
        let row = Row::new(Arc::clone(&table));
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
            format!("Row already exists in index.")
        );

        // 2. Remove entry that is not there.
        assert_eq!(
            format!(
                "{}",
                workload
                    .get_internals()
                    .get_index("sub_idx")
                    .unwrap()
                    .index_remove(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(0)))
                    .unwrap_err()
            ),
            format!("Row does not exist in index.")
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
                    .index_read(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), &cols)
                    .unwrap()
            )
            .unwrap(),
            "[bit_4=1, byte_2_5=205]"
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
                    .index_read(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), &cols)
                    .unwrap()
            )
            .unwrap(),
            "[bit_4=0, byte_2_5=69]"
        );
    }
}
