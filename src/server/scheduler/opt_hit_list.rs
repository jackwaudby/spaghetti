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
use std::{fmt, thread};
use tracing::{debug, info};

pub mod epoch;

pub mod error;

pub mod transaction;

pub mod terminated_list;

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

        Ok(TransactionInfo::OptimisticHitList {
            thread_id,
            txn_id: seq_num,
        })
    }

    /// Create row in table.
    ///
    /// The row is immediately inserted into its table and marked as dirty. No predecessors
    /// collected by this operation.
    fn create(
        &self,
        table: &str,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;
            let thread_id = *thread_id;

            let table = self.get_table(table, &meta)?; // get table
            let mut row = Row::new(key.clone(), Arc::clone(&table), true, true); // create new row

            // init values
            for (i, column) in columns.iter().enumerate() {
                match row.init_value(column, Data::from(values[i].clone())) {
                    Ok(_) => {}
                    Err(e) => {
                        self.abort(meta).unwrap();
                        return Err(e);
                    }
                }
            }

            let index = self.get_index(table, &meta)?; // get index

            // Set values - Needed to make the row "dirty"
            match row.set_values(columns, values, meta) {
                Ok(_) => {}
                Err(e) => {
                    self.abort(meta).unwrap();
                    return Err(e);
                }
            }

            // attempt to insert row
            match index.insert(key, row) {
                Ok(_) => {
                    let pair = (index.get_name(), key.clone());
                    self.thread_states[thread_id].add_key(seq_num, pair, Operation::Create); // register

                    Ok(())
                }
                Err(e) => {
                    self.abort(meta).unwrap();
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Execute a read operation.
    fn read(
        &self,
        table: &str,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;
            let table = self.get_table(table, &meta)?;
            let index = self.get_index(table, &meta)?;

            match index.read(key, columns, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        if let Access::Write(predecessor_id) = access {
                            if &predecessor_id != meta {
                                self.thread_states[*thread_id].add_predecessor(
                                    seq_num,
                                    predecessor_id.to_string(),
                                    PredecessorUpon::Read,
                                );
                            }
                        }
                    }
                    let pair = (index.get_name(), key.clone());
                    self.thread_states[*thread_id].add_key(seq_num, pair, Operation::Read); // register operation; used to clean up.
                    let vals = res.get_values(); // get the values
                    Ok(vals)
                }
                Err(e) => {
                    self.abort(meta).unwrap();
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Execute a write operation.
    fn read_and_update(
        &self,
        table: &str,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;
            let table = self.get_table(table, &meta)?;
            let index = self.get_index(Arc::clone(&table), &meta)?;

            match index.read_and_update(key, columns, values, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        match access {
                            Access::Write(predecessor_id) => {
                                if &predecessor_id != meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }

                            Access::Read(predecessor_id) => {
                                if &predecessor_id != meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }
                        }
                    }

                    // TODO: register read operation as well
                    let pair = (index.get_name(), key.clone());
                    self.thread_states[*thread_id].add_key(seq_num, pair, Operation::Update); // register operation; used to clean up.
                    let vals = res.get_values(); // get vals
                    Ok(vals)
                }
                Err(e) => {
                    self.abort(&meta).unwrap();
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Execute a write operation.
    fn update(
        &self,
        table: &str,
        key: &PrimaryKey,
        columns: &[&str],
        read: bool,
        params: Option<&[Data]>,
        f: &dyn Fn(
            &[&str],           // columns
            Option<Vec<Data>>, // current values
            Option<&[Data]>,   // parameters
        ) -> Result<(Vec<String>, Vec<Data>), NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;
            let table = self.get_table(table, &meta)?;
            let index = self.get_index(table, &meta)?;

            match index.update(key, columns, read, params, f, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        match access {
                            Access::Write(predecessor_id) => {
                                if &predecessor_id != meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }

                            Access::Read(predecessor_id) => {
                                if &predecessor_id != meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }
                        }
                    }
                    let pair = (index.get_name(), key.clone());
                    self.thread_states[*thread_id].add_key(seq_num, pair, Operation::Update);
                    Ok(())
                }
                Err(e) => {
                    self.abort(meta).unwrap();
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Execute an append operation.
    fn append(
        &self,
        table: &str,
        key: &PrimaryKey,
        column: &str,
        value: Data,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;

            let table = self.get_table(table, &meta)?;
            let index = self.get_index(Arc::clone(&table), &meta)?;

            match index.append(key, column, value, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        match access {
                            Access::Write(predecessor_id) => {
                                if &predecessor_id != meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }
                            // RW conflict
                            Access::Read(predecessor_id) => {
                                if predecessor_id != *meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }
                        }
                    }
                    let pair = (index.get_name(), key.clone());
                    self.thread_states[*thread_id].add_key(seq_num, pair, Operation::Update);
                    Ok(())
                }
                Err(e) => {
                    self.abort(meta).unwrap();
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Delete record with `key` from `table`.
    fn delete(
        &self,
        table: &str,
        key: &PrimaryKey,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;
            let table = self.get_table(table, &meta)?; // get table
            let index = self.get_index(Arc::clone(&table), &meta)?; // get index

            match index.delete(key, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        match access {
                            // WW conflict
                            Access::Write(predecessor_id) => {
                                if &predecessor_id != meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }
                            // RW conflict
                            Access::Read(predecessor_id) => {
                                if &predecessor_id != meta {
                                    self.thread_states[*thread_id].add_predecessor(
                                        seq_num,
                                        predecessor_id.to_string(),
                                        PredecessorUpon::Write,
                                    );
                                }
                            }
                        }
                    }
                    let pair = (index.get_name(), key.clone());
                    self.thread_states[*thread_id].add_key(seq_num, pair, Operation::Delete);
                    Ok(())
                }
                Err(e) => {
                    self.abort(meta).unwrap();
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Abort a transaction.
    fn abort(&self, meta: &TransactionInfo) -> crate::Result<()> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;
            self.thread_states[*thread_id].set_state(seq_num, TransactionState::Aborted); // set state.

            let rg = &self.thread_states[*thread_id];
            let x = rg.get_transaction_id(seq_num);

            assert_eq!(x, seq_num, "{} != {}", x, seq_num);

            let inserted = rg.get_keys(seq_num, Operation::Create);
            let read = rg.get_keys(seq_num, Operation::Read);
            let updated = rg.get_keys(seq_num, Operation::Update);
            let deleted = rg.get_keys(seq_num, Operation::Delete);

            for (index, key) in inserted {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.remove(&key).unwrap();
            }

            // This operation can fail of the transaction you read from has already aborted and removed the record.
            for (index, key) in read {
                let index = self.data.get_internals().get_index(&index).unwrap();
                match index.revert_read(&key, meta) {
                    Ok(()) => {} // record was there
                    Err(_) => {} // record already removed
                }
            }

            for (index, key) in &deleted {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.revert(key, meta).unwrap();
            }
            for (index, key) in &updated {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.revert(key, meta).unwrap();
            }

            let se = rg.get_start_epoch(seq_num);
            rg.get_epoch_tracker().add_terminated(seq_num, se);

            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit a transaction.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        if let TransactionInfo::BasicSerializationGraph { thread_id, txn_id } = meta {
            let seq_num = *txn_id;
            // CHECK //

            if let TransactionState::Aborted = self.thread_states[*thread_id].get_state(seq_num) {
                self.abort(&meta).unwrap(); // abort txn
                return Err(OptimisedHitListError::Hit(meta.to_string()).into());
            }

            // HIT PHASE //

            let mut hit_list = self.thread_states[*thread_id].get_hit_list(seq_num); // get hit list

            while !hit_list.is_empty() {
                let predecessor = hit_list.pop().unwrap(); // take a predecessor
                let (p_thread_id, p_seq_num) = parse_id(predecessor); // split ids

                // if active then hit
                if let TransactionState::Active =
                    self.thread_states[p_thread_id].get_state(p_seq_num)
                {
                    self.thread_states[p_thread_id].set_state(p_seq_num, TransactionState::Aborted);
                }
            }

            // CHECK //
            if let TransactionState::Aborted = self.thread_states[*thread_id].get_state(seq_num) {
                self.abort(&meta).unwrap(); // abort txn
                return Err(OptimisedHitListError::Hit(meta.to_string()).into());
            }

            // WAIT PHASE //

            let mut wait_list = self.thread_states[*thread_id].get_wait_list(seq_num); // get wait list

            while !wait_list.is_empty() {
                let predecessor = wait_list.pop().unwrap(); // take a predecessor
                let (p_thread_id, p_seq_num) = parse_id(predecessor); // split ids

                match self.thread_states[p_thread_id].get_state(p_seq_num) {
                    TransactionState::Active => {
                        self.abort(&meta).unwrap(); // abort txn

                        return Err(
                            OptimisedHitListError::PredecessorActive(meta.to_string()).into()
                        );
                    }
                    TransactionState::Aborted => {
                        self.abort(&meta).unwrap(); // abort txn

                        return Err(
                            OptimisedHitListError::PredecessorAborted(meta.to_string()).into()
                        );
                    }
                    TransactionState::Committed => {}
                }
            }

            // CHECK //
            if let TransactionState::Aborted = self.thread_states[*thread_id].get_state(seq_num) {
                self.abort(&meta).unwrap(); // abort txn
                return Err(OptimisedHitListError::Hit(meta.to_string()).into());
            }

            // TRY COMMIT //

            let outcome = self.thread_states[*thread_id].try_commit(seq_num);
            match outcome {
                Ok(_) => {
                    // commit changes

                    let rg = &self.thread_states[*thread_id];

                    let x = rg.get_transaction_id(seq_num);
                    assert_eq!(x, seq_num, "{} != {}", x, seq_num);
                    let inserted = rg.get_keys(seq_num, Operation::Create);
                    let read = rg.get_keys(seq_num, Operation::Read);
                    let updated = rg.get_keys(seq_num, Operation::Update);
                    let deleted = rg.get_keys(seq_num, Operation::Delete);

                    for (index, key) in inserted {
                        let index = self.data.get_internals().get_index(&index).unwrap();
                        index.commit(&key, meta).unwrap();
                    }
                    for (index, key) in deleted {
                        let index = self.data.get_internals().get_index(&index).unwrap();
                        index.remove(&key).unwrap();
                    }
                    for (index, key) in updated {
                        let index = self.data.get_internals().get_index(&index).unwrap();
                        index.commit(&key, meta).unwrap();
                    }
                    // TODO: bug here
                    // would imply I have read from uncommitted data but if so should have not got to this point.
                    for (index, key) in read {
                        let index = self.data.get_internals().get_index(&index).unwrap();
                        index.revert_read(&key, meta).unwrap();
                    }

                    let se = rg.get_start_epoch(seq_num);
                    rg.get_epoch_tracker().add_terminated(seq_num, se);

                    Ok(())
                }
                Err(e) => {
                    self.abort(&meta).unwrap(); // abort txn
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
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
        let config = data.get_internals().get_config();
        let do_gc = config.get_bool("garbage_collection").unwrap();
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
            let sleep = config.get_int("garbage_collection_sleep").unwrap() as u64; // seconds
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
    let split = joint.split('-');
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

impl fmt::Display for OptimisedHitList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OPT_HIT")
    }
}
