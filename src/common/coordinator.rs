use crate::common::{
    error::NonFatalError, global_state::GlobalState, message::Request,
    parameter_generation::ParameterGenerator, statistics::local::LocalStatistics,
};
use crate::scheduler::Scheduler;
use crate::storage::Database;
use crate::workloads::smallbank::{
    self,
    paramgen::{SmallBankGenerator, SmallBankTransactionProfile},
};

use config::Config;
use std::sync::mpsc;
use tracing::debug;

use super::error::SerializationGraphError;
use super::stats_bucket::StatsBucket;

// struct WaitGuards<'a> {
//     guards: Option<Vec<spin::MutexGuard<'a, u8>>>,
// }

// impl<'a> WaitGuards<'a> {
//     fn new() -> Self {
//         Self { guards: None }
//     }
// }
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct AbortReason {
    request_no: u32,
    location: bool,
    latency: u64,
    reason: u8,
    attempt: u64,
    id: u64,
    abort_through: usize,
}

pub fn run(core_id: usize, stats_tx: mpsc::Sender<LocalStatistics>, global_state: &GlobalState) {
    // global state
    let config = global_state.get_config();
    let scheduler = global_state.get_scheduler();
    let database = global_state.get_database();
    // let wait_manager = global_state.get_wait_manager();

    // local state
    let mut stats = LocalStatistics::new();
    let mut transaction_generator = get_transaction_generator(config, core_id);

    stats.start_worker();

    let max_transactions = config.get_int("transactions").unwrap() as u32;
    let mut completed_transactions = 0;

    let mut wtr = csv::Writer::from_path(format!("{}-aborts.csv", core_id)).unwrap();

    loop {
        if completed_transactions == max_transactions {
            debug!("Max transactions completed: {} ", completed_transactions);
            break;
        } else {
            let request = transaction_generator.get_next();
            let isolation_level = request.get_isolation_level();
            // stats.start_latency();

            let mut response;
            // let mut guard s = WaitGuards::new();

            let mut retries = 0;

            loop {
                stats.start_tx();
                let mut meta = scheduler.begin(isolation_level);

                //                stats.start_tx();
                // let transaction_id = meta.get_transaction_id();

                response =
                    execute_logic(&mut meta, request.clone(), scheduler, database, &mut stats);

                // if transaction was restarted and had some locks
                // if guards.guards.is_some() {
                //     let g = guards.guards.take().unwrap();
                //     wait_manager.release(g);
                // }

                match response {
                    Ok(_) => {
                        stats.start_commit();
                        let commit_res = scheduler.commit(&mut meta, database);
                        stats.stop_commit();

                        match commit_res {
                            Ok(_) => {
                                stats.inc_commits();
                                let tx_time = stats.stop_tx();
                                stats.stop_txn_commit(tx_time);

                                break;
                            }
                            Err(_) => {
                                scheduler.abort(&mut meta, database);
                                stats.inc_aborts();
                                stats.inc_commit_aborts();
                                let tx_time = stats.stop_tx();
                                stats.stop_txn_commit_abort(tx_time);
                                stats.start_wait_manager();
                                // let problem_transactions = meta.get_problem_transactions();
                                // let g = wait_manager
                                //     .wait(transaction_id.extract(), problem_transactions);
                                // guards.guards.replace(g);
                                stats.stop_wait_manager();
                                retries += 1;
                            }
                        }
                    }
                    Err(e) => match e {
                        NonFatalError::NoccError => {}
                        NonFatalError::SerializationGraphError(e) => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_aborts();
                            stats.inc_logic_aborts();
                            let tx_time = stats.stop_tx();
                            stats.stop_txn_logic_abort(tx_time);

                            let mut reason = 0;
                            match e {
                                SerializationGraphError::ReadOpCycleFound => {
                                    stats.inc_read_cf();
                                    reason = 1;
                                }
                                SerializationGraphError::WriteOpCascasde => {
                                    stats.inc_write_ca();
                                    reason = 2;
                                }
                                SerializationGraphError::ReadOpCascasde => {
                                    stats.inc_read_ca();
                                    reason = 3;
                                }
                                SerializationGraphError::WriteOpCycleFound => {
                                    stats.inc_write_cf();
                                    reason = 4;
                                }
                                SerializationGraphError::CycleFound => {
                                    stats.inc_rwrite_cf();
                                }
                                _ => {}
                            }

                            stats.start_wait_manager();
                            // let problem_transactions = meta.get_problem_transactions();
                            // wait_manager.wait(transaction_id.extract(), problem_transactions);
                            stats.stop_wait_manager();

                            let reason = AbortReason {
                                request_no: completed_transactions,
                                location: false,
                                latency: tx_time as u64,
                                reason,
                                attempt: retries,
                                id: meta.get_transaction_id().extract(),
                                abort_through: meta.get_abort_through(),
                            };

                            wtr.serialize(&reason).unwrap();

                            retries += 1;
                        }
                        NonFatalError::SmallBankError(_) => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_not_found();

                            let tx_time = stats.stop_tx();
                            stats.stop_txn_not_found(tx_time);

                            // TODO: abort then commit
                            break;
                        }
                    },
                }
            }

            if retries != 0 {
                stats.inc_retries();
                stats.add_cum_retries(retries);
            }

            // let tx = stats.stop_tx();
            // stats.stop_latency(tx);

            completed_transactions += 1;
        }
    }
    wtr.flush().unwrap();

    stats.stop_worker();

    stats_tx.send(stats).unwrap();
}

pub fn execute_logic<'a>(
    meta: &mut StatsBucket,
    request: Request,
    scheduler: &'a Scheduler,
    database: &'a Database,
    stats: &mut LocalStatistics,
) -> Result<(), NonFatalError> {
    use SmallBankTransactionProfile::*;
    let parameters = request.get_parameters();

    match parameters.get() {
        Amalgamate(params) => {
            smallbank::procedures::amalgmate(meta, params.clone(), scheduler, database, stats)
        }
        Balance(params) => {
            smallbank::procedures::balance(meta, params.clone(), scheduler, database, stats)
        }
        SmallBankTransactionProfile::DepositChecking(params) => {
            smallbank::procedures::deposit_checking(
                meta,
                params.clone(),
                scheduler,
                database,
                stats,
            )
        }
        SmallBankTransactionProfile::SendPayment(params) => {
            smallbank::procedures::send_payment(meta, params.clone(), scheduler, database, stats)
        }
        SmallBankTransactionProfile::TransactSaving(params) => {
            smallbank::procedures::transact_savings(
                meta,
                params.clone(),
                scheduler,
                database,
                stats,
            )
        }
        SmallBankTransactionProfile::WriteCheck(params) => {
            smallbank::procedures::write_check(meta, params.clone(), scheduler, database, stats)
        }
    }
}

pub fn get_transaction_generator(config: &Config, core_id: usize) -> ParameterGenerator {
    let sf = config.get_int("scale_factor").unwrap() as u64;
    let set_seed = config.get_bool("set_seed").unwrap();

    let seed;
    if set_seed {
        let s = config.get_int("seed").unwrap() as u64;
        seed = Some(s);
    } else {
        seed = None;
    }

    match config.get_str("workload").unwrap().as_str() {
        "smallbank" => {
            let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
            let isolation_mix = config.get_str("isolation_mix").unwrap();

            let gen = SmallBankGenerator::new(
                core_id,
                sf,
                set_seed,
                seed,
                use_balance_mix,
                isolation_mix,
            );
            ParameterGenerator::SmallBank(gen)
        }

        _ => unimplemented!(),
    }
}
