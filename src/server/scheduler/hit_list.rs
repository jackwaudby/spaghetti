use crate::server::scheduler::hit_list::active_transaction::ActiveTransaction;
use crate::server::scheduler::hit_list::epoch::AtomicSharedResources;
use crate::server::scheduler::hit_list::error::HitListError;
use crate::server::scheduler::{Aborted, Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::row::{Access, Row};
use crate::server::storage::table::Table;
use crate::workloads::{PrimaryKey, Workload};

use chashmap::CHashMap;
use std::sync::{Arc, Mutex};
use std::{thread, time};
use tracing::debug;

pub mod epoch;

pub mod error;

pub mod active_transaction;

pub struct HitList {
    /// Transaction ID counter.
    id: Mutex<u64>,

    /// Map of transaction ids to neccessary runtime information.
    active_transactions: Arc<CHashMap<u64, ActiveTransaction>>,

    /// (Hit list, terminated)
    asr: AtomicSharedResources,

    /// Handle to storage layer.
    data: Arc<Workload>,

    /// Garbage collector.
    _garbage_collector: Option<thread::JoinHandle<()>>,
}

impl Scheduler for HitList {
    /// Register a transaction with the hit list.
    ///

    fn register(&self) -> Result<TransactionInfo, Aborted> {
        // Get transaction ID.
        let id = self.get_id();
        debug!("Register transaction {}", id);

        // Get start epoch and add to epoch tracker.
        debug!("Requesting lock on ASR");
        let mut resources = self.asr.get_lock();
        let start_epoch = resources.get_current_epoch();
        resources.add_started(id);
        drop(resources);
        debug!("Dropped lock on ASR");

        // Register with active transactions.
        let at = ActiveTransaction::new(id, start_epoch);
        self.active_transactions.insert(id, at);
        debug!("Inserted {} into active transaction", id);
        // Create transaction infomation.
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
    ) -> Result<(), Aborted> {
        // Transaction id.
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        debug!("Insert by transaction {}", id);
        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Init row.
        let mut row = Row::new(Arc::clone(&table), "hit");
        row.set_primary_key(key);
        // Init values.
        for (i, column) in columns.iter().enumerate() {
            match row.init_value(column, &values[i].to_string()) {
                Ok(_) => {}
                Err(e) => {
                    self.abort(meta.clone()).unwrap();
                    return Err(Aborted {
                        reason: format!("{}", e),
                    });
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
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        }

        // Record insert - used to rollback if transaction is aborted.
        debug!("Request WG on transaction {} in active transactions", id);
        let mut wg = self.active_transactions.get_mut(&id).unwrap();
        wg.add_key_inserted((index.get_name(), key));

        // Attempt to insert row.
        match index.insert(key, row) {
            Ok(_) => {
                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                Ok(())
            }
            Err(e) => {
                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                debug!("Insert by transaction {} failed", id);
                self.abort(meta.clone()).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
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
    ) -> Result<Vec<Data>, Aborted> {
        // Transaction id.
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        debug!("Read by transaction {}", id);

        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Get write guard on active transaction entry.
        debug!("Request WG on transaction {} in active transactions", id);
        let mut wg = self.active_transactions.get_mut(&id).unwrap();

        // Execute read.
        match index.read(key, columns, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Detect conflicts and insert to predecessor list.
                for access in access_history {
                    // WR conflict
                    if let Access::Write(tid) = access {
                        // Insert predecessor
                        let tid = tid.parse::<u64>().unwrap();
                        wg.add_predecessor(tid);
                    }
                }
                // Add to keys read.
                wg.add_key_read((index.get_name(), key));

                // Get values
                let vals = res.get_values().unwrap();

                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                Ok(vals)
            }
            Err(e) => {
                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                debug!("Read by transaction {} failed", id);
                self.abort(meta.clone()).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        }
    }

    /// Execute a write operation.
    fn update(
        &self,
        table: &str,
        key: PrimaryKey,
        columns: &Vec<&str>,
        values: &Vec<&str>,
        meta: TransactionInfo,
    ) -> Result<(), Aborted> {
        // Transaction id.
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        debug!("Update by transaction {}", id);

        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Get write guard on active transaction entry.
        debug!("Request WG on transaction {} in active transactions", id);
        let mut wg = self.active_transactions.get_mut(&id).unwrap();

        match index.update(key, columns, values, "hit", &meta.get_id().unwrap()) {
            Ok(res) => {
                // Get access history.
                let access_history = res.get_access_history().unwrap();
                // Detect conflicts.
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            // Insert predecessor
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_predecessor(tid);
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_predecessor(tid);
                        }
                    }
                }

                // Add to keys read.
                wg.add_key_updated((index.get_name(), key));
                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                Ok(())
            }
            Err(e) => {
                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                debug!("Update by transaction {} failed", id);
                self.abort(meta).unwrap();
                return Err(Aborted {
                    reason: format!("{}", e),
                });
            }
        }
    }

    /// Delete from row.
    fn delete(&self, table: &str, key: PrimaryKey, meta: TransactionInfo) -> Result<(), Aborted> {
        // Transaction id.
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        debug!("Delete by transaction {}", id);

        // Get table.
        let table = self.get_table(table, meta.clone())?;
        // Get index for this key's table.
        let index = self.get_index(Arc::clone(&table), meta.clone())?;

        // Get write guard on active transaction entry.
        debug!("Request WG on transaction {} in active transactions", id);
        let mut wg = self.active_transactions.get_mut(&id).unwrap();

        // Execute remove op.
        match index.delete(key, "hit") {
            Ok(res) => {
                // Get the access history
                let access_history = res.get_access_history().unwrap();
                // Detect conflicts.
                for access in access_history {
                    match access {
                        // WW conflict
                        Access::Write(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_predecessor(tid);
                        }
                        // RW conflict
                        Access::Read(tid) => {
                            let tid = tid.parse::<u64>().unwrap();
                            wg.add_predecessor(tid);
                        }
                    }
                }

                // Add to keys read.
                wg.add_key_deleted((index.get_name(), key));
                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                Ok(())
            }
            Err(e) => {
                drop(wg);
                debug!("Dropped WG on transaction {} in active transactions", id);
                debug!("Delete by transaction {} failed", id);
                self.abort(meta).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Abort a transaction.
    ///
    /// # Panics
    /// - RWLock or Mutex error.
    fn abort(&self, meta: TransactionInfo) -> crate::Result<()> {
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        debug!("Aborting transaction {}", id);

        // Remove from active transaction.
        debug!("Remove transaction {} from active transactions", id);
        let mut at = self.active_transactions.remove(&id).unwrap();

        // Get exculsive lock on the hit list
        debug!("Transaction {} requesting lock on ASR", id);
        let mut lock = self.asr.get_lock();
        debug!("Transaction {} received lock on ASR", id);

        // Remove from hit list
        debug!("Remove transaction {} from hit list", id);
        debug!("Hit list before: {:?}", lock.hit_list);
        lock.hit_list.remove(&id);
        debug!("Hit list after: {:?}", lock.hit_list);

        // Add to terminated.
        debug!("Register transaction {} with terminated list", id);
        lock.terminated_list.insert(id);
        debug!("Terminated list: {}", id);

        let se = at.get_start_epoch();
        lock.add_terminated(id, se);

        // Revert updates/deletes.
        debug!("Revert updates/deleted for transaction: {}", id);
        let deleted = at.get_keys_deleted();
        for (index, key) in &deleted {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.revert(*key, "hit", &meta.get_id().unwrap()).unwrap();
        }

        debug!("Revert updates/deleted for transaction: {}", id);
        let updated = at.get_keys_updated();
        for (index, key) in &updated {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.revert(*key, "hit", &meta.get_id().unwrap()).unwrap();
        }
        // Remove read accesses from rows read.
        debug!("Revert reads for transaction: {}", id);
        let read = at.get_keys_read();
        for (index, key) in read {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.revert_read(key, &meta.get_id().unwrap()).unwrap();
        }
        // Remove inserted values.
        debug!("Revert inserted for transaction: {}", id);
        let inserted = at.get_keys_inserted();
        for (index, key) in inserted {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.remove(key).unwrap();
        }
        debug!("All changes reverted for transaction: {}", id);
        debug!("Hit list: {:?}", lock.hit_list);
        debug!("Terminated: {:?}", lock.terminated_list);
        drop(lock);
        debug!("Dropped lock on ASR");
        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), Aborted> {
        // Transaction id.
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        debug!("Committing transaction {:?}", id);

        // Get exculsive lock on the hit list
        debug!("Transaction {} requesting lock on ASR", id);
        let mut lock = self.asr.get_lock();
        debug!("Transaction {} received lock on ASR", id);

        // Not in hit list.
        if !lock.hit_list.contains(&id) {
            debug!("Transaction {} not in hitlist", id);
            // Remove from active transactions.
            debug!("Transaction {} removed from active transactions", id);
            let mut at = self.active_transactions.remove(&id).unwrap();

            debug!("Commit inserted for transaction: {}", id);
            let inserted = at.get_keys_inserted();
            for (index, key) in inserted {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.commit(key, "hit", &id.to_string()).unwrap();
            }

            debug!("Commit deleted for transaction: {}", id);
            let deleted = at.get_keys_deleted();
            for (index, key) in deleted {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.remove(key).unwrap();
            }

            debug!("Commit updated for transaction: {}", id);
            let updated = at.get_keys_updated();
            for (index, key) in updated {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.commit(key, "hit", &id.to_string()).unwrap();
            }

            debug!("Revert reads for transaction: {}", id);
            let read = at.get_keys_read();
            for (index, key) in read {
                let index = self.data.get_internals().get_index(&index).unwrap();
                index.revert_read(key, &meta.get_id().unwrap()).unwrap();
            }

            // Get predecessors
            let predecessors = at.get_predecessors();
            debug!(
                "Add transaction {}'s predecessors {:?} to hit list",
                id, predecessors
            );
            debug!("Filter already terminated transactions");
            for predecessor in predecessors {
                // If predecessor is active add to hit list.
                if !lock.terminated_list.contains(&predecessor) {
                    lock.hit_list.insert(predecessor);
                }
            }

            let se = at.get_start_epoch();
            lock.add_terminated(id, se);
            debug!("Add {} to terminated list", id);
            lock.terminated_list.insert(id);
            debug!("Hit list: {:?}", lock.hit_list);
            debug!("Terminated: {:?}", lock.terminated_list);
            drop(lock);
            debug!("Dropped lock on ASR");

            return Ok(());
        } else {
            drop(lock);
            self.abort(meta).unwrap();
            let err = HitListError::IdInHitList(id);
            return Err(Aborted {
                reason: format!("{}", err),
            });
        }
    }
}

impl HitList {
    /// Creates a new scheduler with an empty hit list.
    pub fn new(workload: Arc<Workload>) -> HitList {
        let id = Mutex::new(0);
        let active_transactions = Arc::new(CHashMap::<u64, ActiveTransaction>::new());
        let asr = AtomicSharedResources::new();

        // Set thread name to id.
        let builder = thread::Builder::new().name("garbage_collector".to_string().into());

        let asr_h = asr.get_ref();

        let thread = builder
            .spawn(move || {
                debug!("Starting garbage collector");

                loop {
                    let ten_millis = time::Duration::from_millis(5000);
                    thread::sleep(ten_millis);
                    debug!("Collected garbage");
                    let mut lock = asr_h.lock().unwrap();
                    debug!("Current epoch tracker:\n{}", lock);

                    lock.new_epoch();
                    lock.update_alpha();
                    debug!("Updated epoch tracker:\n{}", lock);

                    debug!("Hit list: {:?}", lock.hit_list);
                    debug!("Terminated: {:?}", lock.terminated_list);
                }
            })
            .unwrap();

        HitList {
            id,
            active_transactions,
            asr,
            _garbage_collector: Some(thread),
            data: workload,
        }
    }

    fn get_id(&self) -> u64 {
        let mut id = self.id.lock().unwrap();
        let new_id = *id;
        *id += 1;
        new_id
    }

    /// Get shared reference to a table.
    fn get_table(&self, table: &str, meta: TransactionInfo) -> Result<Arc<Table>, Aborted> {
        // Get table.
        let res = self.data.get_internals().get_table(table);
        match res {
            Ok(table) => Ok(table),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get primary index name on a table.
    fn get_index_name(&self, table: Arc<Table>, meta: TransactionInfo) -> Result<String, Aborted> {
        let res = table.get_primary_index();
        match res {
            Ok(index_name) => Ok(index_name),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }

    /// Get shared reference to index for a table.
    fn get_index(&self, table: Arc<Table>, meta: TransactionInfo) -> Result<Arc<Index>, Aborted> {
        // Get index name.
        let index_name = self.get_index_name(table, meta.clone())?;

        // Get index for this key's table.
        let res = self.data.get_internals().get_index(&index_name);
        match res {
            Ok(index) => Ok(index),
            Err(e) => {
                self.abort(meta.clone()).unwrap();
                Err(Aborted {
                    reason: format!("{}", e),
                })
            }
        }
    }
}
