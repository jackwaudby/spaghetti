use crate::server::scheduler::hit_list::active_transaction::ActiveTransaction;
use crate::server::scheduler::hit_list::error::HitListError;
use crate::server::scheduler::{Aborted, Scheduler, TransactionInfo};
use crate::server::storage::datatype::Data;
use crate::server::storage::index::Index;
use crate::server::storage::row::{Access, Row};
use crate::server::storage::table::Table;
use crate::workloads::{PrimaryKey, Workload};

use chashmap::CHashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tracing::debug;

pub mod error;

pub mod active_transaction;

pub struct HitList {
    /// Transaction ID counter.
    id: Mutex<u64>,

    /// Map of transaction ids to neccessary runtime information.
    active_transactions: Arc<CHashMap<u64, ActiveTransaction>>,

    /// (Hit list, terminated)
    hit_list: Mutex<(HashSet<u64>, HashSet<u64>)>,

    /// Handle to storage layer.
    data: Arc<Workload>,
}

impl Scheduler for HitList {
    /// Register a transaction with the hit list.
    ///
    /// # Aborts
    ///
    /// Aborts if the assigned ID is already in use.
    fn register(&self) -> Result<TransactionInfo, Aborted> {
        let id = self.get_id();
        // Create info holder.
        let info = TransactionInfo::new(Some(id.to_string()), None);
        debug!("Register {}", id);
        // Create runtime tracker.
        let at = ActiveTransaction::new(id);
        // Add to active transactions map
        if let Some(_) = self.active_transactions.insert(id, at) {
            let err = HitListError::IdAlreadyInUse(id);
            return Err(Aborted {
                reason: format!("{}", err),
            });
        }
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
        self.active_transactions
            .get_mut(&id)
            .unwrap()
            .add_key_inserted((index.get_name(), key));

        // Attempt to insert row.
        match index.insert(key, row) {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!("Abort transaction {:?}: {:?}", meta.get_id().unwrap(), e);
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
                Ok(vals)
            }
            Err(e) => {
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
                wg.add_key_written((index.get_name(), key));

                Ok(())
            }
            Err(e) => {
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
                wg.add_key_written((index.get_name(), key));

                Ok(())
            }
            Err(e) => {
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
        debug!("Aborting transaction {:?}", id);

        // Get exculsive lock on the hit list
        let lock = &mut *self.hit_list.lock().unwrap();
        let (hit_list, terminated) = lock;

        // Remove from hit list
        hit_list.remove(&id);

        // Add to terminated.
        terminated.insert(id);

        // Remove from active transaction.
        let mut at = self.active_transactions.remove(&id).unwrap();
        // Revert updates/deletes.
        let written = at.get_keys_written();
        for (index, key) in &written {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.revert(*key, "hit", &meta.get_id().unwrap()).unwrap();
        }
        // Remove read accesses from rows read.
        let read = at.get_keys_read();
        for (index, key) in read {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.revert_read(key, &meta.get_id().unwrap()).unwrap();
        }
        // Remove inserted values.
        let inserted = at.get_keys_inserted();
        for (index, key) in inserted {
            let index = self.data.get_internals().get_index(&index).unwrap();
            index.remove(key).unwrap();
        }

        Ok(())
    }

    /// Commit a transaction.
    fn commit(&self, meta: TransactionInfo) -> Result<(), Aborted> {
        // Transaction id.
        let id = meta.get_id().unwrap().parse::<u64>().unwrap();
        debug!("Committing transaction {:?}", id);

        // Get exculsive lock on the hit list
        let lock = &mut *self.hit_list.lock().unwrap();
        let (hit_list, terminated) = lock;
        // Not in hit list.
        if !hit_list.contains(&id) {
            // Get predecessors
            let predecessors = self
                .active_transactions
                .get_mut(&id)
                .unwrap()
                .get_predecessors();
            for predecessor in predecessors {
                // If predecessor is active add to hit list.
                if !terminated.contains(&predecessor) {
                    hit_list.insert(predecessor);
                }
            }
            // Add to terminated.
            terminated.insert(id);
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
        let hit_list = Mutex::new((HashSet::new(), HashSet::new()));

        HitList {
            id,
            active_transactions,
            hit_list,
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
