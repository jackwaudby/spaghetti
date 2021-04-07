use crate::common::error::NonFatalError;
use crate::server::scheduler::hit_list::active_transaction::ActiveTransaction;
use crate::server::scheduler::hit_list::error::HitListError;
use crate::server::scheduler::hit_list::shared::{
    AtomicSharedResources, SharedResources, TransactionOutcome,
};
use crate::server::scheduler::{Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::row::{Access, Row};
use crate::workloads::{PrimaryKey, Workload};

use chashmap::CHashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tracing::debug;

pub mod shared;

pub mod epoch;

pub mod error;

pub mod active_transaction;

/// HIT Scheduler.
pub struct HitList {
    /// Transaction id counter.
    id: Arc<Mutex<u64>>,

    /// Map of transaction ids to neccessary runtime information.
    active_transactions: Arc<CHashMap<u64, ActiveTransaction>>,

    /// Resources shared between transactions, e.g. hit list, terminated list.
    asr: AtomicSharedResources,

    /// Handle to storage layer.
    data: Arc<Workload>,

    /// Epoch based garbage collector.
    garbage_collector: GarbageCollector,

    /// Channel to shutdown the garbage collector.
    sender: Mutex<mpsc::Sender<()>>,
}

/// Garbage Collector.
struct GarbageCollector {
    thread: Option<thread::JoinHandle<()>>,
}

impl Scheduler for HitList {
    /// Register a transaction.
    fn register(&self) -> Result<TransactionInfo, NonFatalError> {
        let counter = Arc::clone(&self.id);
        let mut lock = counter.lock().unwrap();
        let id = *lock;
        *lock += 1;

        // Get start epoch and add to epoch tracker.
        let mut resources = self.asr.get_lock();
        let start_epoch = resources.get_current_epoch();
        resources.add_started(id);
        drop(resources);

        // Register with active transactions.
        let at = ActiveTransaction::new(id, start_epoch);
        self.active_transactions.insert(id, at);
        let info = TransactionInfo::new(Some(id.to_string()), None);

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
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        let table = self.get_table(table, meta.clone())?;
        let mut row = Row::new(Arc::clone(&table), "hit");
        row.set_primary_key(key.clone());
        for (i, column) in columns.iter().enumerate() {
            match row.init_value(column, &values[i].to_string()) {
                Ok(_) => {}
                Err(e) => {
                    self.abort(meta.clone()).unwrap();
                    return Err(e);
                }
            }
        }
        // Get Index
        let index = self.get_index(table, meta.clone())?;
        // Set values - Needed to make the row "dirty"
        match row.set_values(columns, values, "hit", &meta.get_id().unwrap()) {
            Ok(_) => {}
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                return Err(e);
            }
        }

        // Record insert - used to rollback if transaction is aborted.
        let mut wg = self.active_transactions.get_mut(&id).unwrap();
        wg.add_key_inserted((index.get_name(), key.clone()));

        // Attempt to insert row.
        match index.insert(key, row) {
            Ok(_) => {
                drop(wg);
                Ok(())
            }
            Err(e) => {
                drop(wg);
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
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        let mut wg = self.active_transactions.get_mut(&id).unwrap();

        // Execute read.
        match index.read(key.clone(), columns, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    // WR conflict
                    if let Access::Write(tid) = access {
                        let tid = tid.parse::<u64>().unwrap();
                        wg.add_pur(tid);
                    }
                }
                wg.add_key_read((index.get_name(), key));
                let vals = res.get_values().unwrap();
                drop(wg);
                Ok(vals)
            }
            Err(e) => {
                drop(wg);
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
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        let mut wg = self.active_transactions.get_mut(&id).unwrap();

        match index.read_and_update(key.clone(), columns, values, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_puw(tid);
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_puw(tid);
                        }
                    }
                }
                wg.add_key_updated((index.get_name(), key));
                let vals = res.get_values().unwrap();
                drop(wg);
                Ok(vals)
            }
            Err(e) => {
                drop(wg);
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
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        let table = self.get_table(table, meta.clone())?;
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        let mut wg = self.active_transactions.get_mut(&id).unwrap();

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
                            wg.add_puw(tid);
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_pur(tid);
                        }
                    }
                }
                wg.add_key_updated((index.get_name(), key));
                drop(wg);
                Ok(())
            }
            Err(e) => {
                drop(wg);
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
        let id = meta.get_id().unwrap().parse::<u64>().unwrap(); // get id
        let table = self.get_table(table, meta.clone())?; // get table
        let index = self.get_index(Arc::clone(&table), meta.clone())?; // get index
        let mut wg = self.active_transactions.get_mut(&id).unwrap(); // get mut ref to active transaction
        match index.delete(key.clone(), "hit") {
            Ok(res) => {
                let access_history = res.get_access_history().unwrap();
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_puw(tid);
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_puw(tid);
                        }
                    }
                }
                wg.add_key_deleted((index.get_name(), key)); // add to deleted
                drop(wg);
                Ok(())
            }
            Err(e) => {
                drop(wg);
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
        let id = meta.get_id().unwrap().parse::<u64>().unwrap(); // get id
        let mut at = self.active_transactions.remove(&id).unwrap(); // remove txn
        let mut lock = self.asr.get_lock(); // lock shared
        lock.remove_from_hit_list(id); // remove aborted txn from hit list
        lock.add_to_terminated_list(id, TransactionOutcome::Aborted); // add txn to terminated list
        let se = at.get_start_epoch(); // register txn with gc
        lock.add_terminated(id, se); // add to epoch terminated

        // remove inserts/reads/updates/deletes
        let inserted = at.get_keys_inserted();
        let read = at.get_keys_read();
        let updated = at.get_keys_updated();
        let deleted = at.get_keys_deleted();

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
        drop(lock);
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), NonFatalError> {
        let id = meta.get_id().unwrap().parse::<u64>().unwrap(); // get id
        let mut lock = self.asr.get_lock(); // get lock on resources
        let mut at = self.active_transactions.get_mut(&id).unwrap(); // get mut ref on txn
        let pur = at.get_pur(); // get pred on read

        // while pur is not empty;
        // for each predecessor upon read;
        let mut pur: Vec<u64> = pur.iter().cloned().collect();
        while !pur.is_empty() {
            let predecessor = pur.pop().unwrap();
            if lock.has_terminated(predecessor) {
                // if terminated but aborted; then abort
                if lock.get_terminated_outcome(predecessor) == TransactionOutcome::Aborted {
                    drop(at); // need to drop as abort removes from list.
                    drop(lock);
                    self.abort(meta).unwrap();
                    let _err = HitListError::IdInHitList(id);
                    return Err(NonFatalError::NonSerializable);
                }
            } else {
                // if not terminated and committed; then abort
                drop(at);
                drop(lock);
                self.abort(meta).unwrap();
                let _err = HitListError::IdInHitList(id);
                return Err(NonFatalError::NonSerializable);
            }
        }

        // if txn is not in hit list; then commit txn
        if !lock.is_in_hit_list(id) {
            let mut at = self.active_transactions.remove(&id).unwrap(); // remove from active
            let inserted = at.get_keys_inserted(); // get keys touched
            let read = at.get_keys_read();
            let updated = at.get_keys_updated();
            let deleted = at.get_keys_deleted();
            // commit changes
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

            // merge active transactions in PuW into hit list.
            let puw = at.get_puw();
            for predecessor in puw {
                if !lock.has_terminated(predecessor) {
                    lock.add_to_hit_list(predecessor);
                }
            }

            let se = at.get_start_epoch();
            lock.add_terminated(id, se);
            lock.add_to_terminated_list(id, TransactionOutcome::Committed);
            drop(lock);

            return Ok(());
        } else {
            drop(lock);
            self.abort(meta).unwrap();
            //TODO
            let _err = HitListError::IdInHitList(id);
            return Err(NonFatalError::NonSerializable);
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
        let id = Arc::new(Mutex::new(0)); // id generator
        let active_transactions = Arc::new(CHashMap::<u64, ActiveTransaction>::new()); // active transactions
        let asr = AtomicSharedResources::new(); // shared resources
        let (sender, receiver) = mpsc::channel(); // shutdown channel
        let sleep = data.get_internals().get_config().get_int("hit_gc").unwrap() as u64;
        let garbage_collector = GarbageCollector::new(asr.get_ref(), receiver, sleep);

        HitList {
            id,
            active_transactions,
            asr,
            data,
            garbage_collector,
            sender: Mutex::new(sender),
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
                loop {
                    // attempt to receive shutdown notification without blocking
                    if let Ok(()) = receiver.try_recv() {
                        break; // exit loop
                    }
                    thread::sleep(Duration::from_millis(sleep)); // sleep garbage collector
                    let mut lock = shared.lock().unwrap(); // lock shared resources
                    debug!("Current epoch tracker:\n{}", lock);
                    lock.new_epoch();
                    lock.update_alpha();
                    debug!("Updated epoch tracker:\n{}", lock);
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
        self.sender.lock().unwrap().send(()).unwrap(); // send shutdown to gc

        if let Some(thread) = self.garbage_collector.thread.take() {
            match thread.join() {
                Ok(_) => debug!("Garbage collector shutdown"),
                Err(_) => debug!("Error shutting down garbage collector"),
            }
        }
    }
}
