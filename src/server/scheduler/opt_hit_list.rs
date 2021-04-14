use crate::server::scheduler::opt_hit_list::error::OptimisedHitListError;
use crate::server::scheduler::opt_hit_list::garbage_collection::GarbageCollector;
use crate::server::scheduler::opt_hit_list::thread_state::{Operation, ThreadState};
use crate::server::scheduler::opt_hit_list::transaction::{PredecessorUpon, TransactionState};
use crate::server::scheduler::NonFatalError;
use crate::server::scheduler::Scheduler;
use crate::server::scheduler::TransactionInfo;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{Access, Row};
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use tracing::{debug, info};

pub mod epoch;

pub mod error;

pub mod transaction;

pub mod thread_state;

pub mod garbage_collection;

/// Optimised Hit List Protocol.
///
/// Contains a fixed sized list of `TerminatedList`s, one for each core.
#[derive(Debug)]
pub struct OptimisedHitList {
    /// Terminated lists.
    thread_states: Arc<Vec<Arc<ThreadState>>>,

    /// Handle to storage layer.
    data: Arc<Workload>,

    /// Epoch based garbage collector.
    garbage_collector: Option<GarbageCollector>,

    /// Channel to shutdown the garbage collector.
    sender: Option<Mutex<mpsc::Sender<()>>>,
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
        let seq_num = self.thread_states[thread_id].new_transaction(); // get sequence number
        let transaction_id = format!("{}-{}", thread_id, seq_num); // create transaction id

        debug!("Register: {}", transaction_id);

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
        debug!("Create: {}-{}", thread_id, seq_num);

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
        self.thread_states[thread_id].add_key(seq_num, pair, Operation::Create);

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
        debug!("Read: {}-{}", thread_id, seq_num);

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        match index.read(key.clone(), columns, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap(); // get access history
                for access in access_history {
                    // WR conflict
                    if let Access::Write(predecessor_id) = access {
                        self.thread_states[thread_id].add_predecessor(
                            seq_num,
                            predecessor_id,
                            PredecessorUpon::Read,
                        );
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.thread_states[thread_id].add_key(seq_num, pair, Operation::Read); // register operation; used to clean up.
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
                            self.thread_states[thread_id].add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(predecessor_id) => {
                            self.thread_states[thread_id].add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                    }
                }

                // TODO: register read operation as well
                let pair = (index.get_name(), key.clone());
                self.thread_states[thread_id].add_key(seq_num, pair, Operation::Update); // register operation; used to clean up.
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
        debug!("Update: {}-{}", thread_id, seq_num);

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
                            self.thread_states[thread_id].add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(predecessor_id) => {
                            self.thread_states[thread_id].add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.thread_states[thread_id].add_key(seq_num, pair, Operation::Update);
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
        debug!("Delete: {}-{}", thread_id, seq_num);

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        match index.delete(key.clone(), "hit") {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(predecessor_id) => {
                            self.thread_states[thread_id].add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(predecessor_id) => {
                            self.thread_states[thread_id].add_predecessor(
                                seq_num,
                                predecessor_id,
                                PredecessorUpon::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.thread_states[thread_id].add_key(seq_num, pair, Operation::Delete);
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
        let handle = thread::current();

        let transaction_id = meta.get_id().unwrap(); // get transaction id
        debug!(
            "Thread {}: abort {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );

        let (thread_id, seq_num) = parse_id(transaction_id.clone()); // split into thread id + seq
        debug!(
            "Thread {}: set state to abort {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );
        self.thread_states[thread_id].set_state(seq_num, TransactionState::Aborted); // set state.
        debug!(
            "Thread {}: state set to abort {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );

        debug!(
            "Thread {}: get keys {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );

        let rg = &self.thread_states[thread_id];
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

        debug!(
            "Thread {}: got keys {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );

        debug!(
            "Thread {}: add terminated {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );
        let se = rg.get_start_epoch(seq_num);
        rg.get_epoch_tracker().add_terminated(seq_num, se);
        debug!(
            "Thread {}:  terminated added{}",
            handle.name().unwrap(),
            transaction_id.clone()
        );
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        let handle = thread::current();
        let transaction_id = meta.get_id().unwrap(); // get transaction id
        debug!(
            "Thread {}: commit {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );

        let (thread_id, seq_num) = parse_id(transaction_id.clone()); // split into thread id + seq

        // CHECK //
        debug!(
            "Thread {}: check {}",
            handle.name().unwrap(),
            transaction_id.clone()
        );
        if let TransactionState::Aborted = self.thread_states[thread_id].get_state(seq_num) {
            self.abort(meta.clone()).unwrap(); // abort txn
            return Err(OptimisedHitListError::Hit(meta.get_id().unwrap()).into());
        }

        // HIT PHASE //
        debug!("Thread {}: start hit phase", handle.name().unwrap());
        let mut hit_list = self.thread_states[thread_id].get_hit_list(seq_num); // get hit list

        while !hit_list.is_empty() {
            let predecessor = hit_list.pop().unwrap(); // take a predecessor
            let (p_thread_id, p_seq_num) = parse_id(predecessor); // split ids

            // if active then hit
            if let TransactionState::Active = self.thread_states[p_thread_id].get_state(p_seq_num) {
                self.thread_states[p_thread_id].set_state(p_seq_num, TransactionState::Aborted);
            }
        }
        debug!("Thread {}: finish hit phase", handle.name().unwrap());

        // CHECK //
        if let TransactionState::Aborted = self.thread_states[thread_id].get_state(seq_num) {
            self.abort(meta.clone()).unwrap(); // abort txn
            return Err(OptimisedHitListError::Hit(meta.get_id().unwrap()).into());
        }

        // WAIT PHASE //
        debug!("Thread {}: start wait phase", handle.name().unwrap());

        let mut wait_list = self.thread_states[thread_id].get_wait_list(seq_num); // get wait list

        debug!(
            "Thread {}: wait list {:?}",
            handle.name().unwrap(),
            wait_list
        );

        while !wait_list.is_empty() {
            let predecessor = wait_list.pop().unwrap(); // take a predecessor
            let (p_thread_id, p_seq_num) = parse_id(predecessor); // split ids

            match self.thread_states[p_thread_id].get_state(p_seq_num) {
                TransactionState::Active => {
                    self.abort(meta.clone()).unwrap(); // abort txn

                    return Err(
                        OptimisedHitListError::PredecessorActive(meta.get_id().unwrap()).into(),
                    );
                }
                TransactionState::Aborted => {
                    self.abort(meta.clone()).unwrap(); // abort txn

                    return Err(
                        OptimisedHitListError::PredecessorAborted(meta.get_id().unwrap()).into(),
                    );
                }
                TransactionState::Committed => {}
            }
        }
        debug!("Thread {}: finish wait phase", handle.name().unwrap());

        // CHECK //
        if let TransactionState::Aborted = self.thread_states[thread_id].get_state(seq_num) {
            self.abort(meta.clone()).unwrap(); // abort txn
            return Err(OptimisedHitListError::Hit(meta.get_id().unwrap()).into());
        }

        // TRY COMMIT //
        debug!("Thread {}: try commit", handle.name().unwrap());

        let outcome = self.thread_states[thread_id].try_commit(seq_num);
        match outcome {
            Ok(_) => {
                debug!("Thread {}: committed", handle.name().unwrap());
                // commit changes
                debug!("Thread {}: get shared lock on list", handle.name().unwrap());
                let rg = &self.thread_states[thread_id];
                debug!("Thread {}: got shared lock on list", handle.name().unwrap());

                let inserted = rg.get_keys(seq_num, Operation::Create);
                let read = rg.get_keys(seq_num, Operation::Read);
                let updated = rg.get_keys(seq_num, Operation::Update);
                let deleted = rg.get_keys(seq_num, Operation::Delete);

                debug!("Thread {}: got keys", handle.name().unwrap());

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
                debug!("Thread {}: committed changed", handle.name().unwrap());

                debug!("Thread {}: add terminated", handle.name().unwrap());
                let se = rg.get_start_epoch(seq_num);
                rg.get_epoch_tracker().add_terminated(seq_num, se);
                debug!("Thread {}: added terminated", handle.name().unwrap());

                Ok(())
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
        let do_gc = data
            .get_internals()
            .get_config()
            .get_bool("garbage_collection")
            .unwrap();
        info!("Initialise optimised hit list with {} threads", size);
        let mut terminated_lists = vec![];
        // terminated list for each thread
        for _ in 0..size {
            let list = Arc::new(ThreadState::new(do_gc));
            terminated_lists.push(list);
        }
        let atomic_tl = Arc::new(terminated_lists);

        let garbage_collector;
        let sender;
        if do_gc {
            let (tx, rx) = mpsc::channel(); // shutdown channel
            sender = Some(Mutex::new(tx));
            let sleep = data
                .get_internals()
                .get_config()
                .get_int("garbage_collection_sleep")
                .unwrap() as u64; // seconds
            garbage_collector = Some(GarbageCollector::new(
                Arc::clone(&atomic_tl),
                rx,
                sleep,
                size,
            ));
        } else {
            garbage_collector = None;
            sender = None;
        }

        OptimisedHitList {
            thread_states: atomic_tl,
            data,
            garbage_collector,
            sender,
        }
    }
}

impl Drop for OptimisedHitList {
    fn drop(&mut self) {
        if let Some(ref mut gc) = self.garbage_collector {
            self.sender
                .take()
                .unwrap()
                .lock()
                .unwrap()
                .send(())
                .unwrap(); // send shutdown to gc

            if let Some(thread) = gc.thread.take() {
                match thread.join() {
                    Ok(_) => debug!("Garbage collector shutdown"),
                    Err(_) => debug!("Error shutting down garbage collector"),
                }
            }
        }
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
    use test_env_log::test;

    lazy_static! {
        static ref WORKLOAD: Arc<Workload> = {
            let mut c = Config::default();
            c.merge(config::File::with_name("./tests/Test-opt-hit.toml"))
                .unwrap();
            let config = Arc::new(c);

            let schema = "./schema/tatp_schema.txt".to_string();
            let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
            let seed = config.get_int("seed").unwrap() as u64;
            let mut rng = StdRng::seed_from_u64(seed);
            tatp::loader::populate_tables(&internals, &mut rng).unwrap();
            Arc::new(Workload::Tatp(internals))
        };
    }

    #[test]
    fn test_optimised_hit_list() {
        let _ohl = OptimisedHitList::new(5, Arc::clone(&WORKLOAD));
    }
}
