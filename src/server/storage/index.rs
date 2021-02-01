extern crate chashmap;

use std::fmt;
use std::fmt::Write;

use crate::server::storage::row::Row;
use chashmap::CHashMap;
use chashmap::WriteGuard;

#[derive(Debug)]
pub struct Index {
    name: String,
    i: CHashMap<u64, Row>,
}

impl Index {
    pub fn init(name: &str) -> Self {
        Index {
            name: String::from(name),
            i: CHashMap::new(),
        }
    }
    pub fn index_exists(&self, key: u64) -> bool {
        self.i.contains_key(&key)
    }
    pub fn index_insert(&self, key: u64, row: Row) -> Option<Row> {
        self.i.insert(key, row)
    }
    pub fn index_read(&self, key: u64, colums: Vec<&str>) -> String {
        let read_guard = self.i.get(&key).unwrap(); // None if doesn't exist.
        let row = &*read_guard;

        let mut res: String;

        res = "[".to_string();

        for column in colums {
            let value = row.get_value(column).unwrap().unwrap(); // ColumnNotFound, None is no value.
            write!(res, "{}={}, ", column, value).unwrap();
        }
        res.truncate(res.len() - 2);
        write!(res, "]").unwrap();
        res
    }

    pub fn index_read_mut(&self, key: u64) -> Option<WriteGuard<u64, Row>> {
        self.i.get_mut(&key)
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
        let cols = vec!["s_id"];
        assert_eq!(
            WORKLOAD
                .get_internals()
                .get_index("sub_idx")
                .unwrap()
                .index_read(0, cols),
            "[s_id=0]"
        );
    }
}
