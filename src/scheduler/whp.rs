use crate::common::error::{NonFatalError, WaitHitError};
use crate::common::transaction_information::{Operation, OperationType, TransactionInformation};

use crate::scheduler::{StatsBucket, ValueId};
use crate::storage::access::{Access, TransactionId};
use crate::storage::{datatype::Data, table::Table, Database};

use crossbeam_epoch as epoch;
use parking_lot::{Mutex, MutexGuard};
use rustc_hash::FxHashSet;
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thread_local::ThreadLocal;
use tracing::info;

#[derive(Debug)]
pub struct WaitHit {
    shared: Arc<Shared>,
    txn_info: ThreadLocal<RefCell<Option<TransactionInformation>>>,
    pur: ThreadLocal<RefCell<FxHashSet<u64>>>,
    puw: ThreadLocal<RefCell<FxHashSet<u64>>>,
}

impl WaitHit {
    pub fn new(cores: usize) -> Self {
        info!(
            "Initialise basic wait hit protocol with {} thread(s)",
            cores
        );

        Self {
            shared: Arc::new(Shared::new()),
            txn_info: ThreadLocal::new(),
            pur: ThreadLocal::new(),
            puw: ThreadLocal::new(),
        }
    }

    fn get_operations(&self) -> Vec<Operation> {
        let mut txn_info = self.txn_info.get().unwrap().borrow_mut();
        txn_info.as_mut().unwrap().get_clone()
    }

    fn record(&self, op_type: OperationType, vid: ValueId, prv: u64) {
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();

        self.txn_info
            .get()
            .unwrap()
            .borrow_mut()
            .as_mut()
            .unwrap()
            .add(op_type, table_id, column_id, offset, prv);
    }

    fn clear_txn_info(&self) {
        *self
            .txn_info
            .get_or(|| RefCell::new(Some(TransactionInformation::new())))
            .borrow_mut() = Some(TransactionInformation::new()); // clear txn info
    }

    fn clear_predecessors(&self) {
        self.pur
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut()
            .clear();

        self.puw
            .get_or(|| RefCell::new(FxHashSet::default()))
            .borrow_mut()
            .clear();
    }

    pub fn begin(&self) -> TransactionId {
        self.clear_txn_info();
        self.clear_predecessors();
        let id = self.shared.get_id();

        TransactionId::WaitHit(id)
    }

    pub fn read_value(
        &self,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<Data, NonFatalError> {
        let this_id = meta.get_transaction_id().extract();
        let table_id = vid.get_table_id();
        let offset = vid.get_offset();
        let table: &Table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset); 
        let prv = rw_table.push_front(Access::Read(meta.get_transaction_id())); 
        let lsn = table.get_lsn(offset);

        spin(prv, lsn);

        let guard = &epoch::pin(); // pin thread
        let snapshot = rw_table.iter(guard); // iterator over access history

        // for each write in the rwtable, add a predecessor upon read to wait list
        for (id, access) in snapshot {
            if id < &prv {
                match access {
                    Access::Write(txn_info) => match txn_info {
                        TransactionId::WaitHit(pred_id) => {
                            if this_id == *pred_id {
                                continue; // skip self predecessors
                            } else {
                                self.pur.get().unwrap().borrow_mut().insert(*pred_id);
                            }
                        }
                        _ => panic!("unexpected transaction information"),
                    },
                    Access::Read(_) => {}
                }
            }
        }

        let column_id = vid.get_column_id();
        let tuple = table.get_tuple(column_id, offset).get();
        let value = tuple.get_value().unwrap().get_value();

        lsn.store(prv + 1, Ordering::Release);
        self.record(OperationType::Read, vid, prv);

        Ok(value)
    }

    pub fn write_value(
        &self,
        value: &mut Data,
        vid: ValueId,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let this_id = meta.get_transaction_id().extract();
        let table_id = vid.get_table_id();
        let column_id = vid.get_column_id();
        let offset = vid.get_offset();
        let table = database.get_table(table_id);
        let rw_table = table.get_rwtable(offset);
        let lsn = table.get_lsn(offset);
        let prv = rw_table.push_front(Access::Write(meta.get_transaction_id()));

        spin(prv, lsn);

        let tuple = table.get_tuple(column_id, offset); // handle to tuple
        let dirty = tuple.get().is_dirty();

        // if tuple is dirty then abort
        // else for each read in the rwtable, add a predecessor upon write to hit list
        if dirty {
            // ok to have state change under feet here
            rw_table.erase(prv); // remove from rwtable
            lsn.store(prv + 1, Ordering::Release); // update lsn
            return Err(WaitHitError::RowDirty.into());
        } else {
            let guard = &epoch::pin(); // pin thread
            let snapshot = rw_table.iter(guard); // iterator over rwtable

            for (id, access) in snapshot {
                if id < &prv {
                    match access {
                        Access::Read(txn_info) => match txn_info {
                            TransactionId::WaitHit(pred_id) => {
                                if this_id == *pred_id {
                                    continue; 
                                } else {
                                    self.puw.get().unwrap().borrow_mut().insert(*pred_id);
                                }
                            }
                            _ => panic!("unexpected transaction information"),
                        },
                        Access::Write(txn_info) => match txn_info {
                            TransactionId::WaitHit(pred_id) => {
                                if this_id == *pred_id {
                                    continue; 
                                } else {
                                    self.puw.get().unwrap().borrow_mut().insert(*pred_id);
                                }
                            }

                            _ => panic!("unexpected transaction information"),
                        },
                    }
                }
            }
        }

        table
            .get_tuple(column_id, offset)
            .get()
            .set_value(value)
            .unwrap();

        lsn.store(prv + 1, Ordering::Release); // update lsn

        self.record(OperationType::Write, vid, prv);

        Ok(())
    }

    pub fn commit<'g>(
        &self,
        meta: &mut StatsBucket,
        database: &Database,
    ) -> Result<(), NonFatalError> {
        let this_id = meta.get_transaction_id().extract();

        let mut g = self.shared.get_lock();

        let pur = self.pur.get().unwrap().borrow().clone();

        for pred in pur {
            if g.has_terminated(pred) {
                if g.get_terminated_outcome(pred) == TransactionOutcome::Aborted {
                    drop(g);
                    return Err(WaitHitError::PredecessorAborted.into());
                }
            } else {
                drop(g);
                return Err(WaitHitError::PredecessorActive.into());
            }
        }

        if !g.is_in_hit_list(this_id) {
            self.tidyup(database, true);

            let puw = self.puw.get().unwrap().borrow().clone(); // TODO: avoid clone

            for pred in puw {
                if !g.has_terminated(pred) {
                    g.add_to_hit_list(pred);
                }
            }
            drop(g);
            Ok(())
        } else {
            drop(g);
            Err(WaitHitError::Hit.into())
        }
    }

    /// Abort operation.
    /// Remove transaction from the hit list and add to the terminated list.
    pub fn abort<'g>(&self, meta: &mut StatsBucket, database: &Database) {
        let this_id = meta.get_transaction_id().extract();
        let mut g = self.shared.get_lock();
        g.remove_from_hit_list(this_id);
        g.add_to_terminated_list(this_id, TransactionOutcome::Aborted);
        self.tidyup(database, false);
    }

    pub fn tidyup<'g>(&self, database: &Database, commit: bool) {
        let ops = self.get_operations();

        for op in ops {
            let Operation {
                op_type,
                table_id,
                column_id,
                offset,
                prv,
            } = op;

            let table = database.get_table(table_id);
            let rwtable = table.get_rwtable(offset);
            let tuple = table.get_tuple(column_id, offset);

            match op_type {
                OperationType::Read => {
                    rwtable.erase(prv);
                }
                OperationType::Write => {
                    if commit {
                        tuple.get().commit();
                    } else {
                        tuple.get().revert();
                    }
                    rwtable.erase(prv);
                }
            }
        }
    }
}

fn spin(prv: u64, lsn: &AtomicU64) {
    let mut i = 0;
    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum TransactionOutcome {
    Aborted,
    Committed,
}

#[derive(Eq, Debug)]
pub struct TerminatedEntry {
    id: u64,
    outcome: TransactionOutcome,
}

#[derive(Debug)]
pub struct Lists {
    hit_list: FxHashSet<u64>,
    terminated_list: FxHashSet<TerminatedEntry>,
}
#[derive(Debug)]
pub struct Shared {
    txn_ctr: Mutex<u64>,
    lists: Mutex<Lists>,
}

impl TerminatedEntry {
    fn get_outcome(&self) -> TransactionOutcome {
        self.outcome.clone()
    }
}

impl Lists {
    fn new() -> Self {
        Self {
            hit_list: FxHashSet::default(),
            terminated_list: FxHashSet::default(),
        }
    }

    pub fn add_to_hit_list(&mut self, id: u64) {
        self.hit_list.insert(id);
    }

    pub fn remove_from_hit_list(&mut self, id: u64) {
        self.hit_list.remove(&id);
    }

    pub fn is_in_hit_list(&self, id: u64) -> bool {
        self.hit_list.contains(&id)
    }

    pub fn add_to_terminated_list(&mut self, id: u64, outcome: TransactionOutcome) {
        self.terminated_list.insert(TerminatedEntry { id, outcome });
    }

    pub fn has_terminated(&self, id: u64) -> bool {
        let pred = TerminatedEntry {
            id,
            outcome: TransactionOutcome::Aborted, // safe to use any outcome as hash by id
        };
        self.terminated_list.contains(&pred)
    }

    pub fn get_terminated_outcome(&self, id: u64) -> TransactionOutcome {
        let pred = TerminatedEntry {
            id,
            outcome: TransactionOutcome::Aborted, // safe to use any outcome as hash by id
        };
        self.terminated_list.get(&pred).unwrap().get_outcome()
    }

    pub fn remove_from_terminated_list(&mut self, id: u64) {
        let entry = TerminatedEntry {
            id,
            outcome: TransactionOutcome::Aborted,
        };
        self.terminated_list.remove(&entry);
    }
}

impl Shared {
    pub fn new() -> Self {
        Self {
            txn_ctr: Mutex::new(0),
            lists: Mutex::new(Lists::new()),
        }
    }

    pub fn get_id(&self) -> u64 {
        let mut lock = self.txn_ctr.lock(); // get mutex lock
        let id = *lock; // get copy of id
        *lock += 1; // increment
        id
    }

    pub fn get_lock(&self) -> MutexGuard<Lists> {
        self.lists.lock()
    }
}

impl PartialEq for TerminatedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for TerminatedEntry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.id.hash(hasher);
    }
}
