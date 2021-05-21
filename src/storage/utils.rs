use crate::scheduler::TransactionInfo;

use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct RwTable(CachePadded<Mutex<AccessHistory>>);

#[derive(Debug, Clone)]
pub struct AccessHistory {
    prv: u64,
    entries: VecDeque<(u64, Access)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionInfo),
    Write(TransactionInfo),
}

#[derive(Debug)]
pub struct LogSequenceNumber(AtomicU64);

// impl Index {
//     /// Create a new index.
//     pub fn init(name: &str) -> Self {
//         Index {
//             name: String::from(name),
//             //    data: IntMap::default(),
//             data: Vec::new(),
//             lsns: Vec::new(),
//             rws: Vec::new(),
//         }
//     }

//     /// Get index name.
//     pub fn get_name(&self) -> String {
//         self.name.clone()
//     }

//     /// Insert a row with key into the index.
//     pub fn insert(&mut self, key: &PrimaryKey, row: Row) {
//         //    self.data.insert(key.clone(), Arc::new(Mutex::new(row)));
//         self.data.push(Arc::new(Mutex::new(row)));
//         self.lsns.push(Arc::new(LogSequenceNumber::new()));
//         self.rws.push(Arc::new(Mutex::new(RwTable::new())));
//     }

//     /// Get a handle to row with key.
//     pub fn get_row(&self, key: &PrimaryKey) -> Arc<Mutex<Row>> {
//         // self.data
//         //     .get(key)
//         //     .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))
//         let offset: usize = key.into();
//         Arc::clone(&self.data[offset])
//     }

//     pub fn get_lsn(&self, key: &PrimaryKey) -> &Arc<LogSequenceNumber> {
//         let offset: usize = key.into();
//         &self.lsns[offset]
//     }

//     pub fn get_rw_table(&self, key: &PrimaryKey) -> Arc<Mutex<RwTable>> {
//         let offset: usize = key.into();
//         Arc::clone(&self.rws[offset])
//     }

//     /// Read columns from a row with the given key.
//     pub fn read(
//         &self,
//         key: &PrimaryKey,
//         columns: &[&str],
//     ) -> Result<OperationResult, NonFatalError> {
//         // let rh = self
//         //     .data
//         //     .get(key)
//         //     .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
//         let rh = self.get_row(key);
//         let mut row = rh.lock();
//         let res = row.get_values(columns)?;
//         Ok(res)
//     }

//     /// Write values to columns in a row with the given key.
//     pub fn update<F>(
//         &self,
//         key: &PrimaryKey,
//         columns: &[&str],
//         read: Option<&[&str]>,
//         params: Option<&[Data]>,
//         f: F,
//     ) -> Result<OperationResult, NonFatalError>
//     where
//         F: Fn(Option<Vec<Data>>, Option<&[Data]>) -> Result<Vec<Data>, NonFatalError>,
//     {
//         // let rh = self
//         //     .data
//         //     .get(key)
//         //     .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
//         let rh = self.get_row(key);

//         let mut row = rh.lock();
//         let current_values;

//         if let Some(columns) = read {
//             let mut res = row.get_values(columns)?;
//             current_values = Some(res.get_values());
//         } else {
//             current_values = None;
//         }

//         let new_values = f(current_values, params)?;

//         let res = row.set_values(&columns, &new_values)?;
//         Ok(res)
//     }

//     /// Append value to column in a row with the given key.
//     pub fn append(
//         &self,
//         key: &PrimaryKey,
//         column: &str,
//         value: Data,
//     ) -> Result<OperationResult, NonFatalError> {
//         // let rh = self
//         //     .data
//         //     .get(key)
//         //     .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
//         let rh = self.get_row(key);

//         let mut row = rh.lock();
//         let res = row.append_value(column, value)?;
//         Ok(res)
//     }

//     /// Commit modifications to a row - rows marked for delete are removed.
//     pub fn commit(&self, key: &PrimaryKey) -> Result<(), NonFatalError> {
//         // let rh = self
//         //     .data
//         //     .get(key)
//         //     .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
//         let rh = self.get_row(key);

//         let mut row = rh.lock(); // lock row
//         row.commit(); // commit

//         Ok(())
//     }

//     /// Revert modifications to a row.
//     pub fn revert(&self, key: &PrimaryKey) -> Result<(), NonFatalError> {
//         // let rh = self
//         //     .data
//         //     .get(key)
//         //     .ok_or_else(|| NonFatalError::RowNotFound(key.to_string(), self.get_name()))?;
//         let rh = self.get_row(key);
//         let mut row = rh.lock();
//         row.revert();
//         Ok(())
//     }
// }

impl Default for LogSequenceNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl LogSequenceNumber {
    pub fn new() -> Self {
        LogSequenceNumber(AtomicU64::new(0))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    pub fn replace(&self, prv: u64) {
        self.0.store(prv, Ordering::SeqCst);
    }

    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Default for RwTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RwTable {
    pub fn new() -> Self {
        RwTable(CachePadded::new(Mutex::new(AccessHistory::new())))
    }

    pub fn get_lock(&self) -> MutexGuard<AccessHistory> {
        self.0.lock()
    }
}

impl AccessHistory {
    fn new() -> Self {
        AccessHistory {
            prv: 0,
            entries: VecDeque::new(),
        }
    }

    pub fn push_front(&mut self, access: Access) -> u64 {
        let id = self.prv;
        self.prv += 1;
        self.entries.push_front((id, access));
        id
    }

    pub fn snapshot(&self) -> VecDeque<(u64, Access)> {
        self.entries.clone()
    }

    pub fn erase(&mut self, entry: (u64, Access)) {
        let index = match self.entries.iter().position(|r| r == &entry) {
            Some(index) => index,
            None => panic!("entry: {:?} not found in {:?}", entry, self.entries),
        };
        self.entries.remove(index);
    }

    pub fn erase_all(&mut self, entry: Access) {
        self.entries.retain(|(_, y)| y != &entry);
    }
}

// impl fmt::Display for Index {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{:#?}", self.data).unwrap();
//         Ok(())
//     }
// }

impl fmt::Display for RwTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // let mut rw = String::new();
        // let n = self.entries.len();

        // if n > 0 {
        //     rw.push_str("[");

        //     for (prv, access) in &self.entries {
        //         rw.push_str(&format!("{}-{}", prv, access));
        //         rw.push_str(", ");
        //     }
        //     let len = rw.len();
        //     rw.truncate(len - 2);
        //     let (_, _) = self.entries[n - 1].clone();
        //     rw.push_str(&format!("]"));
        // } else {
        //     rw.push_str("[]");
        // }

        // write!(f, "prv: {}, rw: {}", self.prv, rw).unwrap();
        write!(f, "TODO").unwrap();
        Ok(())
    }
}

/// Returns true if the access types are the same.
pub fn access_eq(a: &Access, b: &Access) -> bool {
    matches!(
        (a, b),
        (&Access::Read(..), &Access::Read(..)) | (&Access::Write(..), &Access::Write(..))
    )
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Access::*;
        match &self {
            Read(id) => write!(f, "r-{}", id),
            Write(id) => write!(f, "w-{}", id),
        }
    }
}
