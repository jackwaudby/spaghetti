use std::fmt::{self, Write};

use crate::server::storage::row::Row;
use chashmap::CHashMap;

/// An `Index` is used to access data.
///
/// Each table has at least 1 `Index`, which owns all `Row`s stored in that table.
#[derive(Debug)]
pub struct Index {
    /// Index name.
    name: String,
    /// Concurrrent hashmap.
    i: CHashMap<u64, Row>,
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
    pub fn key_exists(&self, key: u64) -> bool {
        self.i.contains_key(&key)
    }

    /// Insert a `Row` into the index
    pub fn index_insert(&self, key: u64, row: Row) -> Option<Row> {
        self.i.insert(key, row) // Returns old entry if exists - TODO: map to error.
    }

    /// Read `columns` from a `Row` with the given `key`.
    pub fn index_read(&self, key: u64, columns: Vec<&str>) -> String {
        let read_guard = self.i.get(&key).unwrap(); // None if doesn't exist - TODO: map to error.
        let row = &*read_guard;

        let mut res: String;

        res = "[".to_string();

        for column in columns {
            let value = row.get_value(column).unwrap().unwrap(); // ColumnNotFound, None is no value - TODO: map to error.
            write!(res, "{}={}, ", column, value).unwrap();
        }
        res.truncate(res.len() - 2);
        write!(res, "]").unwrap();
        res
    }

    /// Write `values` to `columns`.
    pub fn index_write(&self, key: u64, columns: Vec<&str>, values: Vec<&str>) -> () {
        let mut write_guard = self.i.get_mut(&key).unwrap(); //  TODO: map to error.
        let row = &mut *write_guard;

        for (i, name) in columns.iter().enumerate() {
            row.set_value(name, values[i]).unwrap();
        }
    }
}

// [name,num_rows]
impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{},{}]", self.name, self.i.len())
    }
}

#[cfg(test)]
mod tests {
    use crate::workloads::Workload;
    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::Arc;
    use std::sync::Once;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;
    static LOG: Once = Once::new();

    fn logging(on: bool) {
        if on {
            LOG.call_once(|| {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting default subscriber failed");
            });
        }
    }

    lazy_static! {
        static ref WORKLOAD: Arc<Workload> = {
            // Initialise configuration.
            let mut c = Config::default();
            c.merge(config::File::with_name("Test.toml")).unwrap();
            let config = Arc::new(c);
            // Initalise workload.
            let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());
            let mut rng = StdRng::seed_from_u64(42);
            workload.populate_tables(&mut rng).unwrap();
            workload
        };
    }

    #[test]
    fn index_test() {
        logging(false);

        assert_eq!(
            WORKLOAD
                .get_internals()
                .get_index("sub_idx")
                .unwrap()
                .key_exists(0),
            true
        );
        assert_eq!(
            format!("{}", WORKLOAD.get_internals().get_index("sub_idx").unwrap()),
            "[sub_idx,1]"
        );

        let cols = vec!["bit_4", "byte_2_5"];
        assert_eq!(
            WORKLOAD
                .get_internals()
                .get_index("sub_idx")
                .unwrap()
                .index_read(0, cols),
            "[bit_4=1, byte_2_5=205]"
        );

        let cols = vec!["bit_4", "byte_2_5"];
        let vals = vec!["0", "69"];
        WORKLOAD
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .index_write(0, cols, vals);

        let cols = vec!["bit_4", "byte_2_5"];
        assert_eq!(
            WORKLOAD
                .get_internals()
                .get_index("sub_idx")
                .unwrap()
                .index_read(0, cols),
            "[bit_4=0, byte_2_5=69]"
        );
    }
}
