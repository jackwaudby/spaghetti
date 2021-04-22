use crate::common::error::NonFatalError;
use crate::server::scheduler::hit_list::active_transaction::{
    ActiveTransactionTracker, Operation, Predecessor,
};
use crate::server::scheduler::hit_list::error::HitListError;
use crate::server::scheduler::hit_list::shared::{
    AtomicSharedResources, SharedResources, TransactionOutcome,
};
use crate::server::scheduler::{Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{Access, Row};
use crate::workloads::{PrimaryKey, Workload};

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tracing::{debug, info};

pub mod shared;

pub mod epoch;

pub mod error;

pub mod active_transaction;

/// HIT Scheduler.
pub struct HitList {
    // /// Map of transaction ids to neccessary runtime information.
    // active_transactions: Arc<CHashMap<u64, ActiveTransaction>>,
    /// Each worker thread has an id that corresponds to a slot in the vector.
    /// This slot contains the runtime information for a transaction that thread is executing.
    ///
    /// # Safety
    ///
    /// Arc to share across worker threads.
    /// Mutex so threads can mutate state.
    active_transactions: ActiveTransactionTracker,

    /// Resources shared between transactions, e.g. hit list, terminated list.
    asr: AtomicSharedResources,

    /// Handle to storage layer.
    data: Arc<Workload>,

    /// Epoch based garbage collector.
    garbage_collector: Option<GarbageCollector>,

    /// Channel to shutdown the garbage collector.
    sender: Option<Mutex<mpsc::Sender<()>>>,
}

/// Garbage Collector.
struct GarbageCollector {
    thread: Option<thread::JoinHandle<()>>,
}

impl Scheduler for HitList {
    /// Register a transaction.
    fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        // Get thread name.
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        debug!(
            "Thread {} registering a transaction",
            handle.name().unwrap()
        );
        debug!("Thread {} getting id", handle.name().unwrap());
        let id = self.asr.get_next_id(); // get id
        debug!("Thread {} got id: {}", handle.name().unwrap(), id);
        debug!("Thread {} requesting lock", handle.name().unwrap());
        let mut resources = self.asr.get_lock(); // get lock on resources
        debug!("Thread {} received lock", handle.name().unwrap());
        let start_epoch = resources.get_mut_epoch_tracker().get_current_id(); // get start epoch
        debug!(
            "Thread {} started in epoch: {}",
            handle.name().unwrap(),
            start_epoch
        );
        resources.get_mut_epoch_tracker().add_started(id); // register txn in this epoch
        debug!("Thread {} dropping lock", handle.name().unwrap());
        drop(resources); // drop lock
        debug!("Thread {} dropped lock", handle.name().unwrap());

        debug!(
            "Thread {} inserting into active transactions",
            handle.name().unwrap()
        );
        self.active_transactions
            .start_tracking(worker_id, id, start_epoch);

        let info = TransactionInfo::new(Some(id.to_string()), None);
        debug!("Thread {} registered a transaction", handle.name().unwrap());

        Ok(info)
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
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        debug!("Thread {} executing create", handle.name().unwrap());

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
        self.active_transactions
            .add_key(worker_id, pair, Operation::Create);

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
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        debug!("Thread {} executing read", handle.name().unwrap());

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        // execute read
        match index.read(key.clone(), columns, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap(); // get access history
                for access in access_history {
                    // WR conflict
                    if let Access::Write(tid) = access {
                        let tid = tid.parse::<u64>().unwrap();
                        self.active_transactions
                            .add_predecessor(worker_id, tid, Predecessor::Read);
                    }
                }

                let pair = (index.get_name(), key.clone());
                self.active_transactions
                    .add_key(worker_id, pair, Operation::Read);

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
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        debug!(
            "Thread {} executing read and update",
            handle.name().unwrap()
        );
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        // execute read and update
        match index.read_and_update(key.clone(), columns, values, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.active_transactions
                    .add_key(worker_id, pair, Operation::Update);
                // TODO: register as read as well?

                let vals = res.get_values().unwrap();

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
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        debug!("Thread {} executing  update", handle.name().unwrap());

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
                        Access::Write(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.active_transactions
                    .add_key(worker_id, pair, Operation::Update);
                Ok(())
            }
            Err(e) => {
                self.abort(meta).unwrap();
                return Err(e);
            }
        }
    }

    /// Execute an append operation.
    fn append(
        &self,
        table: &str,
        key: PrimaryKey,
        column: &str,
        value: &str,
        meta: TransactionInfo,
    ) -> Result<(), NonFatalError> {
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        debug!("Thread {} executing update", handle.name().unwrap());

        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        match index.append(key.clone(), column, value, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.active_transactions
                    .add_key(worker_id, pair, Operation::Update);
                Ok(())
            }
            Err(e) => {
                self.abort(meta).unwrap();
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
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        debug!("Thread {} executing delete", handle.name().unwrap());

        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index

        match index.delete(key.clone(), "hit") {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            self.active_transactions.add_predecessor(
                                worker_id,
                                tid,
                                Predecessor::Write,
                            );
                        }
                    }
                }
                let pair = (index.get_name(), key.clone());
                self.active_transactions
                    .add_key(worker_id, pair, Operation::Delete);

                Ok(())
            }
            Err(e) => {
                self.abort(meta).unwrap();
                Err(e)
            }
        }
    }

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        let id = meta.get_id().unwrap().parse::<u64>().unwrap(); // get id
        debug!("Thread {} aborting {}", handle.name().unwrap(), id);

        debug!("Thread {} requesting lock", handle.name().unwrap());
        let mut lock = self.asr.get_lock(); // get lock on resources
        debug!("Thread {} received lock", handle.name().unwrap());

        lock.remove_from_hit_list(id); // remove aborted txn from hit list
        lock.add_to_terminated_list(id, TransactionOutcome::Aborted); // add txn to terminated list
        let se = self.active_transactions.get_start_epoch(worker_id); // register txn with gc
        lock.get_mut_epoch_tracker().add_terminated(id, se); // add to epoch terminated

        // remove inserts/reads/updates/deletes
        let inserted = self
            .active_transactions
            .get_keys(worker_id, Operation::Create);
        let read = self
            .active_transactions
            .get_keys(worker_id, Operation::Read);
        let updated = self
            .active_transactions
            .get_keys(worker_id, Operation::Update);
        let deleted = self
            .active_transactions
            .get_keys(worker_id, Operation::Delete);

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

        drop(lock); // drop lock on resources
        debug!("Thread {} dropped lock", handle.name().unwrap());
        self.active_transactions.clear(worker_id);
        Ok(())
    }

    /// Commit a transaction.
    ///
    /// There are two-phases to the commit procedure:
    /// 1) Wait-phase; for each predecessor upon read; abort if active or aborted.
    /// 2) Hit-phase; for each predecessor upon write; if in hit list then abort; else hit predecessors if they are active
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        let tname = thread::current().name().unwrap().to_string();
        let worker_id = tname.parse::<usize>().unwrap();

        let handle = thread::current();
        let id = meta.get_id().unwrap().parse::<u64>().unwrap(); // get id
        debug!("Thread {} committing txn {}", handle.name().unwrap(), id);
        debug!("Thread {} requesting lock", handle.name().unwrap());
        let mut lock = self.asr.get_lock(); // get lock on resources
        debug!("Thread {} received lock", handle.name().unwrap());

        debug!(
            "Thread {} request write guard on entry in active transactions for txn {}",
            handle.name().unwrap(),
            id
        );

        debug!(
            "Thread {} received write guard on entry in active transactions for txn {}",
            handle.name().unwrap(),
            id
        );

        let pur = self
            .active_transactions
            .get_predecessors(worker_id, Predecessor::Read);
        let mut pur: Vec<u64> = pur.iter().cloned().collect(); // convert to vec
        debug!("Thread {} entering wait phase", handle.name().unwrap());

        // while pur is not empty;
        // for each predecessor upon read;
        while !pur.is_empty() {
            let predecessor = pur.pop().unwrap(); // take a predecessor

            // if terminated but aborted; then abort
            if lock.has_terminated(predecessor) {
                if lock.get_terminated_outcome(predecessor) == TransactionOutcome::Aborted {
                    drop(lock); // drop lock on shared resources
                    debug!("Thread {} dropped lock", handle.name().unwrap());
                    self.abort(meta).unwrap(); // abort txn
                    return Err(HitListError::PredecessorAborted(id).into());
                } // else; terminated and committed, continue
            } else {
                // if not terminated and committed; then abort
                drop(lock); // drop lock on shared resources
                debug!("Thread {} dropped lock", handle.name().unwrap());
                self.abort(meta).unwrap();
                return Err(HitListError::PredecessorActive(id).into());
            }
        }

        debug!("Thread {} wait phase complete", handle.name().unwrap());
        debug!("Thread {} entering hit phase", handle.name().unwrap());

        // if txn is not in hit list; then commit txn
        if !lock.is_in_hit_list(id) {
            debug!("Thread {} not in hit list", handle.name().unwrap());

            debug!("Thread {} removed txn from active", handle.name().unwrap());

            // commit changes
            let inserted = self
                .active_transactions
                .get_keys(worker_id, Operation::Create);
            let read = self
                .active_transactions
                .get_keys(worker_id, Operation::Read);
            let updated = self
                .active_transactions
                .get_keys(worker_id, Operation::Update);
            let deleted = self
                .active_transactions
                .get_keys(worker_id, Operation::Delete);

            for (index, key) in inserted {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.commit(key, "hit", &id.to_string()).unwrap();
            }
            for (index, key) in deleted {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.remove(key).unwrap();
            }
            for (index, key) in updated {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.commit(key, "hit", &id.to_string()).unwrap();
            }
            for (index, key) in read {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.revert_read(key, &meta.get_id().unwrap()).unwrap();
            }
            debug!("Thread {} committed changes", handle.name().unwrap());
            // merge active transactions in PuW into hit list.
            let puw = self
                .active_transactions
                .get_predecessors(worker_id, Predecessor::Write);
            for predecessor in puw {
                if !lock.has_terminated(predecessor) {
                    lock.add_to_hit_list(predecessor);
                }
            }
            debug!("Thread {} merged in hit list", handle.name().unwrap());

            let se = self.active_transactions.get_start_epoch(worker_id);
            lock.get_mut_epoch_tracker().add_terminated(id, se);
            lock.add_to_terminated_list(id, TransactionOutcome::Committed);
            debug!(
                "Thread {} added to terminated  list",
                handle.name().unwrap()
            );
            self.active_transactions.clear(worker_id);
            drop(lock);
            debug!("Thread {} dropped lock", handle.name().unwrap());
            return Ok(());
        } else {
            debug!("Thread {} in hit list", handle.name().unwrap());
            drop(lock);
            debug!("Thread {} dropped lock", handle.name().unwrap());
            self.abort(meta).unwrap();
            return Err(HitListError::TransactionInHitList(id).into());
        }
    }

    /// Get handle to storage layer.
    fn get_data(&self) -> Arc<Workload> {
        Arc::clone(&self.data)
    }
}

impl HitList {
    /// Create a `HitList` scheduler.
    pub fn new(data: Arc<Workload>) -> HitList {
        let config = data.get_internals().get_config();
        let workers = config.get_int("workers").unwrap() as usize;
        let gc = config.get_bool("garbage_collection").unwrap();

        info!("Initialise hit list with {} workers", workers);
        info!("Garbage collection: {}", gc);

        let active_transactions = ActiveTransactionTracker::new(workers); // active transactions
        let asr = AtomicSharedResources::new(); // shared resources

        let garbage_collector;
        let sender;
        if gc {
            let (tx, rx) = mpsc::channel(); // shutdown channel
            sender = Some(Mutex::new(tx));
            let sleep = config.get_int("garbage_collection_sleep").unwrap() as u64;
            garbage_collector = Some(GarbageCollector::new(asr.get_ref(), rx, sleep));
        } else {
            garbage_collector = None;
            sender = None;
        }

        HitList {
            active_transactions,
            asr,
            data,
            garbage_collector,
            sender,
        }
    }
}

// channel between hit list scheduler and garbage collection thread
impl GarbageCollector {
    fn new(
        shared: Arc<Mutex<SharedResources>>,
        receiver: mpsc::Receiver<()>,
        sleep: u64,
    ) -> GarbageCollector {
        let builder = thread::Builder::new().name("garbage_collector".to_string().into()); // thread name
        let thread = builder
            .spawn(move || {
                debug!("Starting garbage collector");
                let handle = thread::current();

                loop {
                    // attempt to receive shutdown notification without blocking
                    if let Ok(()) = receiver.try_recv() {
                        break; // exit loop
                    }
                    thread::sleep(Duration::from_millis(sleep * 1000)); // sleep garbage collector
                    debug!("Thread {} requesting lock", handle.name().unwrap());
                    let mut lock = shared.lock().unwrap(); // lock shared resources
                    debug!("Thread {} receieved lock", handle.name().unwrap());
                    debug!("Current epoch tracker:\n{}", lock.get_mut_epoch_tracker());
                    lock.get_mut_epoch_tracker().new_epoch(); // get next epoch
                    lock.get_mut_epoch_tracker().update_alpha(); // update alpha
                    let to_remove = lock
                        .get_mut_epoch_tracker()
                        .get_transactions_to_garbage_collect(); // transaction to remove
                    for id in to_remove {
                        lock.remove_from_terminated_list(id);
                    }
                    debug!("Updated epoch tracker:\n{}", lock.get_mut_epoch_tracker());
                    drop(lock); // drop lock on shared resources
                    debug!("Thread {} dropped lock", handle.name().unwrap());
                }
            })
            .unwrap();

        GarbageCollector {
            thread: Some(thread),
        }
    }
}

impl Drop for HitList {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::storage::datatype;
    use crate::workloads::tatp::keys::TatpPrimaryKey;
    use crate::workloads::tatp::loader;
    use crate::workloads::Internal;

    use config::Config;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::convert::TryInto;
    use test_env_log::test;

    // single transaction that commits.
    #[test]
    fn hit_list_commit_test() {
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-hit.toml"))
            .unwrap();
        let config = Arc::new(c);

        // workload with fixed seed
        let schema = "./schema/tatp_schema.txt".to_string();
        let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
        let seed = config.get_int("seed").unwrap();
        let mut rng = StdRng::seed_from_u64(seed.try_into().unwrap());
        loader::populate_tables(&internals, &mut rng).unwrap();
        let workload = Arc::new(Workload::Tatp(internals));

        // Initialise scheduler.
        let scheduler = Arc::new(HitList::new(workload));

        let h = thread::Builder::new().name("1".to_string()).spawn(move || {
            let txn = scheduler.register().unwrap(); // register
            assert_eq!(txn, TransactionInfo::new(Some("0".to_string()), None));

            let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(3)); // pk
            let columns: Vec<&str> = vec!["bit_1"];

            let values = scheduler
                .read("subscriber", pk.clone(), &columns, txn.clone())
                .unwrap();
            let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();
            assert_eq!(
                res,
                "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"bit_1\":\"0\"}}"
            );
            scheduler.commit(txn).unwrap();
            drop(scheduler);
        });

        h.unwrap().join().unwrap();
    }
}
