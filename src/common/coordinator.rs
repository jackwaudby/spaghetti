use super::stats_bucket::StatsBucket;
use crate::common::message::Parameters;
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
use crate::workloads::tatp::{
    self,
    paramgen::{TatpGenerator, TatpTransactionProfile},
};
use crate::workloads::ycsb::{
    self,
    paramgen::{YcsbGenerator, YcsbTransactionProfile},
};

use config::Config;
use serde::{Deserialize, Serialize};
use std::sync::mpsc;
use tracing::debug;

struct WaitGuards<'a> {
    guards: Option<Vec<std::sync::MutexGuard<'a, u8>>>,
}

impl<'a> WaitGuards<'a> {
    fn new() -> Self {
        Self { guards: None }
    }
}

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
    let wait_manager = global_state.get_wait_manager();

    // local state
    let mut stats = LocalStatistics::new();
    let mut transaction_generator = get_transaction_generator(config, core_id);

    stats.start_worker();

    let max_transactions = config.get_int("transactions").unwrap() as u32;
    let mut completed_transactions = 0;

    loop {
        if completed_transactions == max_transactions {
            debug!("Max transactions completed: {} ", completed_transactions);
            break;
        } else {
            let request = transaction_generator.get_next();
            let isolation_level = request.get_isolation_level();
            stats.start_latency();

            let mut response;
            let mut guards = WaitGuards::new();

            loop {
                let mut meta = scheduler.begin(isolation_level);
                let transaction_id = meta.get_transaction_id();
                stats.start_tx();

                response =
                    execute_logic(&mut meta, request.clone(), scheduler, database, &mut stats);

                // if transaction was restarted and had some locks
                if guards.guards.is_some() {
                    let g = guards.guards.take().unwrap();
                    wait_manager.release(g);
                }

                match response {
                    Ok(_) => {
                        stats.start_commit();
                        let commit_res = scheduler.commit(&mut meta, database);
                        stats.stop_commit();

                        match commit_res {
                            Ok(_) => {
                                stats.inc_commits();

                                break;
                            }
                            Err(e) => match e {
                                NonFatalError::NoccError => {}
                                NonFatalError::SmallBankError(_) => {}
                                NonFatalError::RowNotFound => {}
                                NonFatalError::SerializationGraphError(_) => {
                                    scheduler.abort(&mut meta, database);
                                    stats.inc_aborts();

                                    stats.start_wait_manager();
                                    let mut problem_transactions = meta.get_problem_transactions();
                                    let abort_through = meta.get_abort_through();
                                    problem_transactions.insert(abort_through);
                                    let g = wait_manager
                                        .wait(transaction_id.extract(), problem_transactions);
                                    guards.guards.replace(g);
                                    stats.stop_wait_manager();
                                }
                            },
                        }
                    }
                    Err(e) => match e {
                        NonFatalError::NoccError => {}
                        NonFatalError::SerializationGraphError(_) => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_aborts();

                            stats.start_wait_manager();
                            let mut problem_transactions = meta.get_problem_transactions();
                            let abort_through = meta.get_abort_through();
                            problem_transactions.insert(abort_through);
                            wait_manager.wait(transaction_id.extract(), problem_transactions);
                            stats.stop_wait_manager();
                        }
                        NonFatalError::SmallBankError(_) => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_not_found();

                            break;
                        }
                        NonFatalError::RowNotFound => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_not_found();

                            break;
                        }
                    },
                }
            }

            let tx = stats.stop_tx();
            stats.stop_latency(tx);

            completed_transactions += 1;
        }
    }

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

    match parameters {
        Parameters::SmallBank(p) => match p {
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
                smallbank::procedures::send_payment(
                    meta,
                    params.clone(),
                    scheduler,
                    database,
                    stats,
                )
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
        },
        Parameters::Ycsb(p) => match p {
            YcsbTransactionProfile::General(params) => {
                ycsb::procedures::transaction(meta, params.clone(), scheduler, database, stats)
            }
        },
        Parameters::Tatp(p) => match p {
            TatpTransactionProfile::GetSubscriberData(params) => {
                tatp::procedures::get_subscriber_data(
                    meta,
                    params.clone(),
                    scheduler,
                    database,
                    stats,
                )
            }
            TatpTransactionProfile::GetAccessData(params) => {
                tatp::procedures::get_access_data(meta, params.clone(), scheduler, database, stats)
            }
            TatpTransactionProfile::GetNewDestination(params) => {
                tatp::procedures::get_new_destination(
                    meta,
                    params.clone(),
                    scheduler,
                    database,
                    stats,
                )
            }
            TatpTransactionProfile::UpdateLocationData(params) => {
                tatp::procedures::update_location(meta, params.clone(), scheduler, database, stats)
            }
            TatpTransactionProfile::UpdateSubscriberData(params) => {
                tatp::procedures::update_subscriber_data(
                    meta,
                    params.clone(),
                    scheduler,
                    database,
                    stats,
                )
            }
        },
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
        "ycsb" => {
            let theta = config.get_float("theta").unwrap();
            let update_rate = config.get_float("update_rate").unwrap();
            let serializable_rate = config.get_float("serializable_rate").unwrap();

            let gen = YcsbGenerator::new(
                core_id,
                sf,
                set_seed,
                seed,
                theta,
                update_rate,
                serializable_rate,
            );
            ParameterGenerator::Ycsb(gen)
        }
        "tatp" => {
            let use_nurand = config.get_bool("nurand").unwrap();
            let gen = TatpGenerator::new(core_id, sf, set_seed, seed, use_nurand);
            ParameterGenerator::Tatp(gen)
        }
        _ => unimplemented!(),
    }
}
