use crate::common::error::NonFatalError;
use crate::scheduler::hit_list::active_transaction::{
    ActiveTransactionTracker, Operation, Predecessor,
};
use crate::scheduler::hit_list::error::HitListError;
use crate::scheduler::hit_list::shared::{
    AtomicSharedResources, SharedResources, TransactionOutcome,
};
use crate::scheduler::{Scheduler, TransactionInfo};
use crate::storage::datatype::Data;
use crate::storage::row::Access;
use crate::workloads::{PrimaryKey, Workload};

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fmt, thread};
use tracing::{debug, info};

pub mod shared;

pub mod epoch;

pub mod error;

pub mod active_transaction;

/// HIT Scheduler.
#[derive(Debug)]
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
#[derive(Debug)]
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

        let info = TransactionInfo::HitList { txn_id: id };
        debug!("Thread {} registered a transaction", handle.name().unwrap());

        Ok(info)
    }

    /// Execute a read operation.
    fn read(
        &self,
        table: &str,
        _index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        if let TransactionInfo::HitList { txn_id: _ } = meta {
            let worker_id = 0;

            let table = self.get_table(table, &meta)?; // get table
            let index = self.get_index(table, &meta)?; // get index

            // execute read
            match index.read(key, columns, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        // WR conflict
                        if let Access::Write(tid) = access {
                            if let TransactionInfo::HitList { txn_id } = tid {
                                self.active_transactions.add_predecessor(
                                    worker_id,
                                    txn_id,
                                    Predecessor::Read,
                                );
                            }
                        }
                    }

                    let pair = (index.get_name(), key.clone());
                    self.active_transactions
                        .add_key(worker_id, pair, Operation::Read);

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
        _index: Option<&str>,
        key: &PrimaryKey,
        columns: &[&str],
        values: &[Data],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        if let TransactionInfo::HitList { txn_id: _ } = meta {
            let worker_id = 0;
            let table = self.get_table(table, &meta)?; // get table
            let index = self.get_index(table, &meta)?; // get index

            // execute read and update
            match index.read_and_update(key, columns, values, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();

                    for access in access_history {
                        match access {
                            Access::Write(tid) => {
                                if let TransactionInfo::HitList { txn_id } = tid {
                                    self.active_transactions.add_predecessor(
                                        worker_id,
                                        txn_id,
                                        Predecessor::Write,
                                    );
                                }
                            }

                            Access::Read(tid) => {
                                if let TransactionInfo::HitList { txn_id } = tid {
                                    self.active_transactions.add_predecessor(
                                        worker_id,
                                        txn_id,
                                        Predecessor::Write,
                                    );
                                }
                            }
                        }
                    }
                    let pair = (index.get_name(), key.clone());
                    self.active_transactions
                        .add_key(worker_id, pair, Operation::Update);
                    // TODO: register as read as well?

                    let vals = res.get_values();

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
        _index: Option<&str>,
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
        if let TransactionInfo::HitList { txn_id: _ } = meta {
            let worker_id = 0;
            let table = self.get_table(table, &meta)?;
            let index = self.get_index(Arc::clone(&table), &meta)?;

            match index.update(key, columns, read, params, f, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        match access {
                            // WW conflict
                            Access::Write(tid) => {
                                if let TransactionInfo::HitList { txn_id } = tid {
                                    self.active_transactions.add_predecessor(
                                        worker_id,
                                        txn_id,
                                        Predecessor::Write,
                                    );
                                }
                            }
                            // RW conflict
                            Access::Read(tid) => {
                                if let TransactionInfo::HitList { txn_id } = tid {
                                    self.active_transactions.add_predecessor(
                                        worker_id,
                                        txn_id,
                                        Predecessor::Write,
                                    );
                                }
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
        _index: Option<&str>,
        key: &PrimaryKey,
        column: &str,
        value: Data,
        meta: &TransactionInfo,
    ) -> Result<(), NonFatalError> {
        if let TransactionInfo::HitList { txn_id: _ } = meta {
            let worker_id = 0; // TODO

            let table = self.get_table(table, &meta)?;
            let index = self.get_index(Arc::clone(&table), &meta)?;

            match index.append(key, column, value, meta) {
                Ok(res) => {
                    let access_history = res.get_access_history();
                    for access in access_history {
                        match access {
                            // WW conflict
                            Access::Write(tid) => {
                                if let TransactionInfo::HitList { txn_id } = tid {
                                    self.active_transactions.add_predecessor(
                                        worker_id,
                                        txn_id,
                                        Predecessor::Write,
                                    );
                                }
                            }
                            // RW conflict
                            Access::Read(tid) => {
                                if let TransactionInfo::HitList { txn_id } = tid {
                                    self.active_transactions.add_predecessor(
                                        worker_id,
                                        txn_id,
                                        Predecessor::Write,
                                    );
                                }
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
                    Err(e)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Abort a transaction.
    fn abort(&self, meta: &TransactionInfo) -> crate::Result<()> {
        if let TransactionInfo::HitList { txn_id } = meta {
            let id = *txn_id;
            let worker_id = 0;

            let mut lock = self.asr.get_lock(); // get lock on resources

            lock.remove_from_hit_list(id); // remove aborted txn from hit list
            lock.add_to_terminated_list(id, TransactionOutcome::Aborted); // add txn to terminated list
            let se = self.active_transactions.get_start_epoch(worker_id); // register txn with gc
            lock.get_mut_epoch_tracker().add_terminated(id, se); // add to epoch terminated

            // remove inserts/reads/updates/deletes
            let read = self
                .active_transactions
                .get_keys(worker_id, Operation::Read);
            let updated = self
                .active_transactions
                .get_keys(worker_id, Operation::Update);

            for (index, key) in read {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.revert_read(&key, meta).unwrap();
            }

            for (index, key) in &updated {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.revert(&key, meta).unwrap();
            }

            drop(lock); // drop lock on resources

            self.active_transactions.clear(worker_id);
            Ok(())
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit a transaction.
    ///
    /// There are two-phases to the commit procedure:
    /// 1) Wait-phase; for each predecessor upon read; abort if active or aborted.
    /// 2) Hit-phase; for each predecessor upon write; if in hit list then abort; else hit predecessors if they are active
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        if let TransactionInfo::HitList { txn_id } = meta {
            let id = *txn_id;
            let worker_id = 0;

            let mut lock = self.asr.get_lock(); // get lock on resources

            let pur = self
                .active_transactions
                .get_predecessors(worker_id, Predecessor::Read);
            let mut pur: Vec<u64> = pur.iter().cloned().collect(); // convert to vec

            // while pur is not empty;
            // for each predecessor upon read;
            while !pur.is_empty() {
                let predecessor = pur.pop().unwrap(); // take a predecessor

                // if terminated but aborted; then abort
                if lock.has_terminated(predecessor) {
                    if lock.get_terminated_outcome(predecessor) == TransactionOutcome::Aborted {
                        drop(lock); // drop lock on shared resources

                        self.abort(&meta).unwrap(); // abort txn
                        return Err(HitListError::PredecessorAborted(id).into());
                    } // else; terminated and committed, continue
                } else {
                    // if not terminated and committed; then abort
                    drop(lock); // drop lock on shared resources

                    self.abort(&meta).unwrap();
                    return Err(HitListError::PredecessorActive(id).into());
                }
            }

            // if txn is not in hit list; then commit txn
            if !lock.is_in_hit_list(id) {
                let read = self
                    .active_transactions
                    .get_keys(worker_id, Operation::Read);
                let updated = self
                    .active_transactions
                    .get_keys(worker_id, Operation::Update);

                for (index, key) in updated {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index.commit(&key, meta).unwrap();
                }
                for (index, key) in read {
                    let index = self.data.get_internals().get_index(&index).unwrap();
                    index.revert_read(&key, meta).unwrap();
                }

                // merge active transactions in PuW into hit list.
                let puw = self
                    .active_transactions
                    .get_predecessors(worker_id, Predecessor::Write);
                for predecessor in puw {
                    if !lock.has_terminated(predecessor) {
                        lock.add_to_hit_list(predecessor);
                    }
                }

                let se = self.active_transactions.get_start_epoch(worker_id);
                lock.get_mut_epoch_tracker().add_terminated(id, se);
                lock.add_to_terminated_list(id, TransactionOutcome::Committed);

                self.active_transactions.clear(worker_id);
                drop(lock);

                Ok(())
            } else {
                drop(lock);

                self.abort(&meta).unwrap();
                Err(HitListError::TransactionInHitList(id).into())
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
        let builder = thread::Builder::new().name("garbage_collector".to_string()); // thread name
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::server::storage::datatype;
//     use crate::workloads::tatp::keys::TatpPrimaryKey;
//     use crate::workloads::tatp::loader;
//     use crate::workloads::Internal;

//     use config::Config;
//     use rand::rngs::StdRng;
//     use rand::SeedableRng;
//     use std::convert::TryInto;
//     use test_env_log::test;

//     // single transaction that commits.
//     #[test]
//     fn hit_list_commit_test() {
//         let mut c = Config::default();
//         c.merge(config::File::with_name("./tests/Test-hit.toml"))
//             .unwrap();
//         let config = Arc::new(c);

//         // workload with fixed seed
//         let schema = "./schema/tatp_schema.txt".to_string();
//         let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
//         let seed = config.get_int("seed").unwrap();
//         let mut rng = StdRng::seed_from_u64(seed.try_into().unwrap());
//         loader::populate_tables(&internals, &mut rng).unwrap();
//         let workload = Arc::new(Workload::Tatp(internals));

//         // Initialise scheduler.
//         let scheduler = Arc::new(HitList::new(workload));

//         let h = thread::Builder::new().name("1".to_string()).spawn(move || {
//             let txn = scheduler.register().unwrap(); // register
//             assert_eq!(txn, TransactionInfo::new(Some("0".to_string()), None));

//             let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(3)); // pk
//             let columns: Vec<&str> = vec!["bit_1"];

//             let values = scheduler
//                 .read("subscriber", pk.clone(), &columns, &txn)
//                 .unwrap();
//             let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();
//             assert_eq!(
//                 res,
//                 "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"bit_1\":\"0\"}}"
//             );
//             scheduler.commit(&txn).unwrap();
//             drop(scheduler);
//         });

//         h.unwrap().join().unwrap();
//     }
// }

impl fmt::Display for HitList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HIT")
    }
}
