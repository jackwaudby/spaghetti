use crate::common::error::NonFatalError;
use crate::scheduler::owh::error::OptimisedWaitHitError;
use crate::scheduler::owh::garbage_collection::GarbageCollector;
use crate::scheduler::owh::thread_state::ThreadState;
use crate::scheduler::owh::transaction::{
    Operation, OperationType, PredecessorUpon, TransactionState,
};
use crate::scheduler::{CurrentValues, NewValues, Scheduler, TransactionInfo};
use crate::storage;
use crate::storage::datatype::Data;
use crate::storage::Access;
use crate::workloads::smallbank::SB_SF_MAP;
use crate::workloads::PrimaryKey;
use crate::workloads::Workload;

use std::collections::VecDeque;
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

#[derive(Debug)]
pub struct OptimisedWaitHit {
    thread_states: Arc<Vec<Arc<ThreadState>>>,
    data: Arc<Workload>,
    garbage_collector: Option<GarbageCollector>,
    sender: Option<Mutex<mpsc::Sender<()>>>,
    accounts: usize,
}

impl OptimisedWaitHit {
    pub fn new(size: usize, data: Workload) -> Self {
        let config = data.get_config();

        let sf = config.get_int("scale_factor").unwrap() as u64;
        let accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;

        let do_gc = config.get_bool("garbage_collection").unwrap();
        info!("Initialise optimised wait-hit with {} core(s)", size);
        let mut terminated_lists = vec![];

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

        OptimisedWaitHit {
            thread_states: atomic_tl,
            data: Arc::new(data),
            garbage_collector,
            sender,
            accounts,
        }
    }
}

impl Scheduler for OptimisedWaitHit {
    fn begin(&self) -> TransactionInfo {
        let thread_id = thread::current()
            .name()
            .unwrap()
            .to_string()
            .parse::<usize>()
            .unwrap();
        debug!("tid: {}; begin", thread_id);

        let seq_num = self.thread_states[thread_id].new_transaction();

        TransactionInfo::OptimisticWaitHit(thread_id, seq_num)
    }

    fn read(
        &self,
        index_id: usize,
        key: &PrimaryKey,
        columns: &[&str],
        meta: &TransactionInfo,
    ) -> Result<Vec<Data>, NonFatalError> {
        if let TransactionInfo::OptimisticWaitHit(thread_id, seq_num) = meta {
            debug!("tid: {}; read", thread_id);
            let offset = storage::calculate_offset(key.into(), index_id, self.accounts);
            let record = self.data.get_db().get_record(offset); // get record

            let mut guard = record.get_rw_table().get_lock();
            let prv = guard.push_front(Access::Read(meta.clone())); // get prv
            drop(guard);

            let lsn = record.get_lsn();
            loop {
                let i = lsn.get();

                if i == prv {
                    break; // prv == lsn
                }
            }

            let guard = record.get_rw_table().get_lock();
            let snapshot: VecDeque<(u64, Access)> = guard.snapshot(); // get accesses
            drop(guard);

            for (id, access) in snapshot {
                if id < prv {
                    match access {
                        Access::Write(txn_info) => match txn_info {
                            TransactionInfo::OptimisticWaitHit(ref pthread_id, ref pseq_num) => {
                                if pthread_id == thread_id && pseq_num == seq_num {
                                    continue; // self dependency
                                } else {
                                    self.thread_states[*thread_id].add_predecessor(
                                        *seq_num,
                                        (*pthread_id, *pseq_num),
                                        PredecessorUpon::Read,
                                    ); // add pur
                                }
                            }
                            _ => panic!("unexpected transaction information"),
                        },
                        Access::Read(_) => {}
                    }
                }
            }

            let row = record.get_row();
            let mut guard = row.get_lock();
            let mut res = guard.get_values(columns).unwrap();
            drop(guard);
            let vals = res.get_values();

            self.thread_states[*thread_id].add_key(
                *seq_num,
                OperationType::Read,
                key.clone(),
                index_id,
            ); // register operation; used to clean up.

            lsn.replace(prv + 1); // increment to next operation

            Ok(vals)
        } else {
            panic!("unexpected transaction info");
        }
    }

    fn write(
        &self,
        index: usize,
        key: &PrimaryKey,
        columns: &[&str],
        read: Option<&[&str]>,
        params: Option<&[Data]>,
        f: &dyn Fn(CurrentValues, Option<&[Data]>) -> Result<NewValues, NonFatalError>,
        meta: &TransactionInfo,
    ) -> Result<Option<Vec<Data>>, NonFatalError> {
        if let TransactionInfo::OptimisticWaitHit(thread_id, seq_num) = meta {
            debug!("tid: {}; write", thread_id);
            let offset = storage::calculate_offset(key.into(), index, self.accounts);
            let record = self.data.get_db().get_record(offset); // get record

            let mut guard = record.get_rw_table().get_lock();
            let prv = guard.push_front(Access::Write(meta.clone())); // get prv
            drop(guard);

            let lsn = record.get_lsn();
            loop {
                let i = lsn.get();

                if i == prv {
                    break; // prv == lsn
                }
            }

            let mut row = record.get_row().get_lock();
            let dirty = row.is_dirty();

            if dirty {
                drop(row);
                let mut guard = record.get_rw_table().get_lock();
                guard.erase((prv, Access::Write(meta.clone())));
                drop(guard);
                lsn.replace(prv + 1);
                self.abort(meta);
                return Err(NonFatalError::RowDirty(
                    "todo".to_string(),
                    "todo".to_string(),
                ));
            } else {
                let guard = record.get_rw_table().get_lock();
                let snapshot: VecDeque<(u64, Access)> = guard.snapshot(); // get accesses
                drop(guard);

                for (id, access) in snapshot.clone() {
                    if id < prv {
                        match access {
                            Access::Read(txn_info) => match txn_info {
                                TransactionInfo::OptimisticWaitHit(
                                    ref pthread_id,
                                    ref pseq_num,
                                ) => {
                                    if pthread_id == thread_id && pseq_num == seq_num {
                                        continue; // self dependency
                                    } else {
                                        self.thread_states[*thread_id].add_predecessor(
                                            *seq_num,
                                            (*pthread_id, *pseq_num),
                                            PredecessorUpon::Write,
                                        );
                                    }
                                }
                                _ => panic!("unexpected transaction information"),
                            },
                            Access::Write(txn_info) => match txn_info {
                                TransactionInfo::OptimisticWaitHit(
                                    ref pthread_id,
                                    ref pseq_num,
                                ) => {
                                    if pthread_id == thread_id && pseq_num == seq_num {
                                        continue; // self dependency
                                    } else {
                                        self.thread_states[*thread_id].add_predecessor(
                                            *seq_num,
                                            (*pthread_id, *pseq_num),
                                            PredecessorUpon::Write,
                                        );
                                    }
                                }
                                _ => panic!("unexpected transaction information"),
                            },
                        }
                    }
                }

                let current_values;
                if let Some(columns) = read {
                    let mut res = row.get_values(columns).unwrap();
                    current_values = Some(res.get_values());
                } else {
                    current_values = None;
                }

                let new_values = match f(current_values.clone(), params) {
                    Ok(res) => res,
                    Err(e) => {
                        drop(row);

                        let mut guard = record.get_rw_table().get_lock();
                        guard.erase((prv, Access::Write(meta.clone())));
                        drop(guard);

                        lsn.replace(prv + 1);

                        self.abort(meta);
                        return Err(e);
                    }
                };

                row.set_values(columns, &new_values).unwrap();
                drop(row);

                self.thread_states[*thread_id].add_key(
                    *seq_num,
                    OperationType::Write,
                    key.clone(),
                    index,
                ); // register operation; used to clean up.

                lsn.replace(prv + 1);

                Ok(current_values)
            }
        } else {
            panic!("unexpected transaction info");
        }
    }

    fn abort(&self, meta: &TransactionInfo) -> NonFatalError {
        if let TransactionInfo::OptimisticWaitHit(thread_id, seq_num) = meta {
            debug!("tid: {}; abort", thread_id);
            self.thread_states[*thread_id].set_state(*seq_num, TransactionState::Aborted); // set state.

            let rg = &self.thread_states[*thread_id];
            let x = rg.get_transaction_id(*seq_num);

            assert_eq!(x, *seq_num, "{} != {}", x, seq_num);

            let ops = rg.get_info(*seq_num);

            for op in ops {
                let Operation {
                    op_type,
                    ref key,
                    index,
                } = op;
                let offset = storage::calculate_offset(key.into(), index, self.accounts);

                let record = self.data.get_db().get_record(offset);
                let rw_table = record.get_rw_table();
                let mut guard = rw_table.get_lock();

                match op_type {
                    OperationType::Read => {
                        guard.erase_all(Access::Read(meta.clone())); // remove access
                        drop(guard);
                    }
                    OperationType::Write => {
                        let row = record.get_row();
                        let mut rguard = row.get_lock();
                        rguard.revert();
                        drop(rguard);

                        guard.erase_all(Access::Write(meta.clone())); // remove access

                        drop(guard);
                    }
                }
            }

            let se = rg.get_start_epoch(*seq_num);
            rg.get_epoch_tracker().add_terminated(*seq_num, se);

            NonFatalError::NonSerializable
        } else {
            panic!("unexpected transaction info");
        }
    }

    /// Commit a transaction.
    fn commit(&self, meta: &TransactionInfo) -> Result<(), NonFatalError> {
        if let TransactionInfo::OptimisticWaitHit(thread_id, seq_num) = meta {
            debug!("tid: {}; commit", thread_id);
            // CHECK //
            if let TransactionState::Aborted = self.thread_states[*thread_id].get_state(*seq_num) {
                self.abort(&meta);
                return Err(OptimisedWaitHitError::Hit(meta.to_string()).into());
            }

            // HIT PHASE //

            let mut hit_list = self.thread_states[*thread_id].get_hit_list(*seq_num); // get hit list

            while !hit_list.is_empty() {
                let (p_thread_id, p_seq_num) = hit_list.pop().unwrap(); // take a predecessor

                // if active then hit
                if let TransactionState::Active =
                    self.thread_states[p_thread_id].get_state(p_seq_num)
                {
                    self.thread_states[p_thread_id].set_state(p_seq_num, TransactionState::Aborted);
                }
            }

            // CHECK //
            if let TransactionState::Aborted = self.thread_states[*thread_id].get_state(*seq_num) {
                self.abort(&meta);

                let e = OptimisedWaitHitError::Hit(meta.to_string());
                debug!("tid: {}; abort: {}", thread_id, e);
                return Err(e.into());
            }

            // WAIT PHASE //

            let mut wait_list = self.thread_states[*thread_id].get_wait_list(*seq_num); // get wait list

            while !wait_list.is_empty() {
                let (p_thread_id, p_seq_num) = wait_list.pop().unwrap(); // take a predecessor

                match self.thread_states[p_thread_id].get_state(p_seq_num) {
                    TransactionState::Active => {
                        self.abort(&meta); // abort txn
                        let e = OptimisedWaitHitError::PredecessorActive(meta.to_string());
                        debug!("tid: {}; abort: {}", thread_id, e);
                        return Err(e.into());
                    }
                    TransactionState::Aborted => {
                        self.abort(&meta); // abort txn
                        let e = OptimisedWaitHitError::PredecessorAborted(
                            meta.to_string(),
                            format!("({}-{})", p_thread_id, p_seq_num),
                        );
                        debug!("tid: {}; abort: {}", thread_id, e);
                        return Err(e.into());
                    }
                    TransactionState::Committed => {}
                }
            }

            // CHECK //
            if let TransactionState::Aborted = self.thread_states[*thread_id].get_state(*seq_num) {
                self.abort(&meta); // abort txn
                let e = OptimisedWaitHitError::Hit(meta.to_string());
                debug!("tid: {}; abort: {}", thread_id, e);
                return Err(e.into());
            }

            // TRY COMMIT //

            let outcome = self.thread_states[*thread_id].try_commit(*seq_num);
            match outcome {
                Ok(_) => {
                    // commit changes

                    let rg = &self.thread_states[*thread_id];

                    let x = rg.get_transaction_id(*seq_num);
                    assert_eq!(x, *seq_num, "{} != {}", x, seq_num);

                    let ops = rg.get_info(*seq_num);

                    for op in ops {
                        let Operation {
                            op_type,
                            ref key,
                            index,
                        } = op;
                        let offset = storage::calculate_offset(key.into(), index, self.accounts);

                        let record = self.data.get_db().get_record(offset);
                        let rw_table = record.get_rw_table();
                        let mut guard = rw_table.get_lock();

                        match op_type {
                            OperationType::Read => {
                                guard.erase_all(Access::Read(meta.clone())); // remove access
                                drop(guard);
                            }
                            OperationType::Write => {
                                let row = record.get_row();
                                let mut rguard = row.get_lock();
                                rguard.commit();
                                drop(rguard);

                                guard.erase_all(Access::Write(meta.clone())); // remove access

                                drop(guard);
                            }
                        }
                    }

                    let se = rg.get_start_epoch(*seq_num);
                    rg.get_epoch_tracker().add_terminated(*seq_num, se);

                    Ok(())
                }
                Err(_) => {
                    self.abort(&meta);
                    Err(NonFatalError::NonSerializable)
                }
            }
        } else {
            panic!("unexpected transaction info");
        }
    }
}

impl Drop for OptimisedWaitHit {
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

impl fmt::Display for OptimisedWaitHit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO")
    }
}
