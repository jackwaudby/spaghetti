use crate::server::scheduler::opt_hit_list::error::OptimisedHitListError;
use crate::server::scheduler::opt_hit_list::terminated_list::{
    Operation, PredecessorUpon, ThreadState, TransactionState,
};
use crate::server::scheduler::NonFatalError;
use crate::server::scheduler::Scheduler;
use crate::server::scheduler::TransactionInfo;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{Access, Row};
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use std::sync::RwLock;
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use tracing::info;

pub mod error;

pub mod terminated_list;

/// Optimised Hit List Protocol.
///
/// Contains a fixed sized list of `TerminatedList`s, one for each core.
#[derive(Debug)]
pub struct OptimisedHitList {
    /// Terminated lists.
    terminated_lists: Vec<Arc<RwLock<ThreadState>>>,

    /// Handle to storage layer.
    data: Arc<Workload>,
}

impl Scheduler for OptimisedHitList {
    /// Register a transaction.
    ///
    /// Transaction gets the id (thread id + sequence num).
    fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        let thread_id = thread::current()
            .name()
            .unwrap()
            .to_string()
            .parse::<usize>()
            .unwrap(); // get thread id
        let seq_num = self.get_exculsive_lock(thread_id).new_transaction(); // get sequence number
        let transaction_id = format!("{}-{}", thread_id, seq_num); // create transaction id

        Ok(TransactionInfo::new(Some(transaction_id), None))
    }

    /// Create row in table.
    ///
    /// The row is immediately inserted into its table and marked as dirty. No predecessors
    /// collected by this operation.
    ///
    /// # Aborts
    ///
    /// A transaction aborts if:
    /// - Table or index does not exist
    /// - Incorrect column or value
    fn create(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let (thread_id, seq_num) = parse_id(meta.get_id().unwrap()); // split
        let table = self.get_table(table, meta.clone())?; // get table
        let mut row = Row::new(Arc::clone(&table), "hit"); // create new row
        row.set_primary_key(key.clone()); // set pk

        // init values
        for (i, column) in columns.iter().enumerate() {
            match row.init_value(column, &values[i].to_string()) {
                Ok(_) => {}
                Err(e) => {
                    self.abort(meta.clone()).unwrap();
                    return Err(e);
                }
            }
        }

        let index = self.get_index(table, meta.clone())?; // get index

        // Set values - Needed to make the row "dirty"
        match row.set_values(columns, values, "hit", &meta.get_id().unwrap()) {
            Ok(_) => {}
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(e);
            }
        }

        let pair = (index.get_name(), key.clone());
        self.get_shared_lock(thread_id)
            .add_key(seq_num, pair, Operation::Create);

        // attempt to insert row
        match index.insert(key, row) {
            Ok(_) => Ok(()),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(e)
            }
        }
    }

    /// Execute a read operation.
    ///
    /// A transaction aborts if:
    /// - Table or index does not exist
    /// - Incorrect column or value
    fn read(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        let (thread_id, seq_num) = parse_id(meta.get_id().unwrap()); // split transaction_id
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        match index.read(key.clone(), columns, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap(); // get access history
                for access in access_history {
                    // WR conflict
                    if let Access::Write(predecessor_id) = access {
                        self.get_shared_lock(thread_id).add_predecessor(
                            seq_num,
                            predecessor_id,
                            PredecessorUpon::Read,
                        );
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.get_shared_lock(thread_id)
                    .add_key(seq_num, pair, Operation::Read); // register operation; used to clean up.
                let vals = res.get_values().unwrap(); // get the values
                Ok(vals)
            }
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(e);
            }
        }
    }

    /// Execute a write operation.
    fn read_and_update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        let (thread_id, seq_num) = parse_id(meta.get_id().unwrap()); // transaction_id
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        match index.read_and_update(key.clone(), columns, values, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(predecessor_id) => {
                            self.get_shared_lock(thread_id).add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(predecessor_id) => {
                            self.get_shared_lock(thread_id).add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                    }
                }

                // TODO: register read operation as well
                let pair = (index.get_name(), key.clone());
                self.get_shared_lock(thread_id)
                    .add_key(seq_num, pair, Operation::Update); // register operation; used to clean up.
                let vals = res.get_values().unwrap(); // get vals
                Ok(vals)
            }
            Err(e) => {
                self.abort(meta).unwrap();
                return Err(e);
            }
        }
    }

    /// Execute a write operation.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: Vec<String>,
        read: bool,
        params: Vec<Data>,
        // (columns, current_values, parameters) -> (columns,new_values)
        f: &dyn Fn(
            Vec<String>,
            Option<Vec<Data>>,
            Vec<Data>,
        ) -> Result<(Vec<String>, Vec<String>), NonFatalError>,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let (thread_id, seq_num) = parse_id(meta.get_id().unwrap());
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        match index.update(
            key.clone(),
            columns,
            read,
            params,
            f,
            "hit",
            &meta.get_id().unwrap(),
        ) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(predecessor_id) => {
                            self.get_shared_lock(thread_id).add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(predecessor_id) => {
                            self.get_shared_lock(thread_id).add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.get_shared_lock(thread_id)
                    .add_key(seq_num, pair, Operation::Update);
                Ok(())
            }
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(e);
            }
        }
    }

    /// Delete record with `key` from `table`.
    fn delete(
        &self,
        table: &str,
        key: PrimaryKey,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let (thread_id, seq_num) = parse_id(meta.get_id().unwrap());
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        match index.delete(key.clone(), "hit") {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(predecessor_id) => {
                            self.get_shared_lock(thread_id).add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(predecessor_id) => {
                            self.get_shared_lock(thread_id).add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.get_shared_lock(thread_id)
                    .add_key(seq_num, pair, Operation::Delete);
                Ok(())
            }
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(e)
            }
        }
    }

    /// Abort a transaction.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        let transaction_id = meta.get_id().unwrap(); // get transaction id
        let (thread_id, seq_num) = parse_id(transaction_id); // split into thread id + seq

        self.get_shared_lock(thread_id)
            .set_state(seq_num, TransactionState::Aborted); // set state.

        let rg = self.get_shared_lock(thread_id);
        let inserted = rg.get_keys(seq_num, Operation::Create);
        let read = rg.get_keys(seq_num, Operation::Read);
        let updated = rg.get_keys(seq_num, Operation::Update);
        let deleted = rg.get_keys(seq_num, Operation::Delete);

        for (index, key) in inserted {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.remove(key.clone()).unwrap();
        }
        for (index, key) in read {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index
                .revert_read(key.clone(), &meta.get_id().unwrap())
                .unwrap();
        }
        for (index, key) in &deleted {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index
                .revert(key.clone(), "hit", &meta.get_id().unwrap())
                .unwrap();
        }
        for (index, key) in &updated {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index
                .revert(key.clone(), "hit", &meta.get_id().unwrap())
                .unwrap();
        }

        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        let transaction_id = meta.get_id().unwrap(); // get transaction id
        let (thread_id, seq_num) = parse_id(transaction_id); // split into thread id + seq

        // CHECK //
        if let TransactionState::Aborted = self.get_shared_lock(thread_id).get_state(seq_num) {
            self.abort(meta.clone()).unwrap(); // abort txn
            return Err(OptimisedHitListError::Hit(meta.get_id().unwrap()).into());
        }

        // HIT PHASE //
        let mut hit_list = self.get_shared_lock(thread_id).get_hit_list(seq_num); // get hit list

        while !hit_list.is_empty() {
            let predecessor = hit_list.pop().unwrap(); // take a predecessor
            let (p_thread_id, p_seq_num) = parse_id(predecessor); // split ids

            // if active then hit
            if let TransactionState::Active = self.get_shared_lock(p_thread_id).get_state(p_seq_num)
            {
                self.get_shared_lock(p_thread_id)
                    .set_state(p_seq_num, TransactionState::Aborted);
            }
        }

        // CHECK //
        if let TransactionState::Aborted = self.get_shared_lock(thread_id).get_state(seq_num) {
            self.abort(meta.clone()).unwrap(); // abort txn
            return Err(OptimisedHitListError::Hit(meta.get_id().unwrap()).into());
        }

        // WAIT PHASE //
        let wait_list = self.get_shared_lock(thread_id).get_hit_list(seq_num); // get wait list

        while !wait_list.is_empty() {
            let predecessor = hit_list.pop().unwrap(); // take a predecessor
            let (p_thread_id, p_seq_num) = parse_id(predecessor); // split ids

            match self.get_shared_lock(p_thread_id).get_state(p_seq_num) {
                TransactionState::Active => {
                    return Err(
                        OptimisedHitListError::PredecessorActive(meta.get_id().unwrap()).into(),
                    )
                }
                TransactionState::Aborted => {
                    return Err(
                        OptimisedHitListError::PredecessorAborted(meta.get_id().unwrap()).into(),
                    )
                }
                TransactionState::Committed => {}
            }
        }

        // CHECK //
        if let TransactionState::Aborted = self.get_shared_lock(thread_id).get_state(seq_num) {
            self.abort(meta.clone()).unwrap(); // abort txn
            return Err(OptimisedHitListError::Hit(meta.get_id().unwrap()).into());
        }

        // TRY COMMIT //
        match self.get_shared_lock(thread_id).try_commit(seq_num) {
            Ok(_) => {
                // commit changes
                let rg = self.get_shared_lock(thread_id);
                let inserted = rg.get_keys(seq_num, Operation::Create);
                let read = rg.get_keys(seq_num, Operation::Read);
                let updated = rg.get_keys(seq_num, Operation::Update);
                let deleted = rg.get_keys(seq_num, Operation::Delete);

                for (index, key) in inserted {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index
                        .commit(key, "hit", &meta.get_id().unwrap().to_string())
                        .unwrap();
                }
                for (index, key) in deleted {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index.remove(key).unwrap();
                }
                for (index, key) in updated {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index
                        .commit(key, "hit", &meta.get_id().unwrap().to_string())
                        .unwrap();
                }
                for (index, key) in read {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index.revert_read(key, &meta.get_id().unwrap()).unwrap();
                }
                Ok(())
                // TODO: garbage collection
            }
            Err(e) => {
                self.abort(meta.clone()).unwrap(); // abort txn
                return Err(e);
            }
        }
    }

    /// Get handle to storage layer.
    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}

impl OptimisedHitList {
    /// Create new optimised hit list.
    pub fn new(size: usize, data: Arc<Workload>) -> Self {
        info!("Initialise optimised hit list with {} threads", size);
        let mut terminated_lists = vec![];
        for _ in 0..size {
            let list = Arc::new(RwLock::new(ThreadState::new()));
            terminated_lists.push(list);
        }
        OptimisedHitList {
            terminated_lists: terminated_lists,
            data,
        }
    }

    /// Get shared lock on a thread's terminated list.
    ///
    /// # Panics
    ///
    /// Acquiring `RwLock` fails.
    fn get_shared_lock(&self, thread_id: usize) -> RwLockReadGuard<ThreadState> {
        let rg = self.terminated_lists[thread_id].read().unwrap();
        rg
    }

    /// Get shared lock on a thread's terminated list.
    ///
    /// # Panics
    ///
    /// Acquiring `RwLock` fails.
    fn get_exculsive_lock(&self, thread_id: usize) -> RwLockWriteGuard<ThreadState> {
        let wg = self.terminated_lists[thread_id].write().unwrap();
        wg
    }
}

fn parse_id(joint: String) -> (usize, u64) {
    let split = joint.split("-");
    let vec: Vec<usize> = split.map(|x| x.parse::<usize>().unwrap()).collect();
    (vec[0], vec[1] as u64)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::workloads::tatp;
    use crate::workloads::Internal;
    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
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
            // Load from test file.
            c.merge(config::File::with_name("Test-opt-hit.toml")).unwrap();
           let config = Arc::new(c);

            // Workload with fixed seed.
            let schema = config.get_str("schema").unwrap();
            let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
            let seed = config.get_int("seed").unwrap() as u64;
            let mut rng = StdRng::seed_from_u64(seed);
            tatp::loader::populate_tables(&internals, &mut rng).unwrap();
            Arc::new(Workload::Tatp(internals))
        };
    }

    #[test]
    fn test_optimised_hit_list() {
        logging(false);

        let _ohl = OptimisedHitList::new(5, Arc::clone(&WORKLOAD));
    }
}
