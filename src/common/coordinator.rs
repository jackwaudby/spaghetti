use super::stats_bucket::StatsBucket;
use crate::common::message::Parameters;
use crate::common::{
    error::NonFatalError, global_state::GlobalState, message::Request,
    parameter_generation::ParameterGenerator, statistics::local::LocalStatistics,
};
use crate::scheduler::msgt::Cycle;
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
use std::time::{Duration, Instant};
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

    let warmup = config.get_int("warmup").unwrap() as u64;
    let warmup_st = Instant::now();
    let warmup_dur = Duration::new(warmup, 0);
    let warmup_end = warmup_st + warmup_dur;

    // local state
    let mut stats = LocalStatistics::new();
    let mut transaction_generator = get_transaction_generator(config, core_id);

    debug!("Begin warmup phase");
    while warmup_end > Instant::now() {
        let request = transaction_generator.get_next();
        let isolation_level = request.get_isolation_level();
        let mut meta = scheduler.begin(isolation_level);
        let response = execute_logic(&mut meta, request.clone(), scheduler, database);

        match response {
            Ok(_) => {
                let commit_res = scheduler.commit(&mut meta, database);

                if let Err(e) = commit_res {
                    if let NonFatalError::SerializationGraphError(_) = e {
                        scheduler.abort(&mut meta, database);
                    }
                }
            }
            Err(e) => {
                if let NonFatalError::SerializationGraphError(_)
                | NonFatalError::SmallBankError(_)
                | NonFatalError::RowNotFound = e
                {
                    scheduler.abort(&mut meta, database);
                }
            }
        }
    }

    debug!("Warmup phase complete");

    let execution = config.get_int("execution").unwrap() as u64;
    let execution_st = Instant::now();
    let execution_dur = Duration::new(execution, 0);
    let execution_end = execution_st + execution_dur;

    stats.start_worker();

    let max_transactions = config.get_int("transactions").unwrap() as u32;
    let mut completed_transactions = 0;

    loop {
        if completed_transactions == max_transactions {
            debug!("Max transactions completed: {} ", completed_transactions);
            break;
        } else if execution_end < Instant::now() {
            debug!("Timed out");
            break;
        } else {
            let request = transaction_generator.get_next();
            let isolation_level = request.get_isolation_level();
            stats.start_latency();

            let mut response;
            let mut guards = WaitGuards::new();

            let mut attempts = 0;

            loop {
                if attempts > 100000000 {
                    panic!("{} aborted too many times", core_id);
                }

                let mut meta = scheduler.begin(isolation_level);
                let transaction_id = meta.get_transaction_id();
                stats.start_tx();

                response = execute_logic(&mut meta, request.clone(), scheduler, database);

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
                                stats.inc_conflicts(&meta);

                                break;
                            }
                            Err(e) => match e {
                                NonFatalError::NoccError => {}
                                NonFatalError::SmallBankError(_) => {}
                                NonFatalError::RowNotFound => {}
                                NonFatalError::SerializationGraphError(_)
                                | NonFatalError::WaitHitError(_)
                                | NonFatalError::TwoPhaseLockingError(_) => {
                                    scheduler.abort(&mut meta, database);

                                    stats.inc_aborts();
                                    record_cycle_type(&mut meta, &mut stats);
                                    record_path_len(&mut meta, &mut stats);
                                    stats.inc_conflicts(&meta);

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
                        NonFatalError::SerializationGraphError(_)
                        | NonFatalError::WaitHitError(_)
                        | NonFatalError::TwoPhaseLockingError(_) => {
                            scheduler.abort(&mut meta, database);

                            stats.inc_aborts();
                            record_cycle_type(&mut meta, &mut stats);
                            record_path_len(&mut meta, &mut stats);
                            stats.inc_conflicts(&meta);

                            stats.start_wait_manager();
                            let mut problem_transactions = meta.get_problem_transactions();
                            let abort_through = meta.get_abort_through();
                            problem_transactions.insert(abort_through);
                            let g =
                                wait_manager.wait(transaction_id.extract(), problem_transactions);
                            guards.guards.replace(g);
                            stats.stop_wait_manager();
                        }
                        NonFatalError::SmallBankError(_) => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_not_found();
                            stats.inc_conflicts(&meta);

                            break;
                        }
                        NonFatalError::RowNotFound => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_not_found();
                            stats.inc_conflicts(&meta);

                            break;
                        }
                    },
                }
                attempts += 1;
            }

            let tx = stats.stop_tx();
            stats.stop_latency(tx);

            completed_transactions += 1;
        }
    }

    stats.stop_worker();

    stats_tx.send(stats).unwrap();
}

fn record_path_len(meta: &mut StatsBucket, stats: &mut LocalStatistics) {
    if let Some(path_len) = meta.get_path_len() {
        stats.inc_path_len(*path_len);
    }
}

fn record_cycle_type(meta: &mut StatsBucket, stats: &mut LocalStatistics) {
    if let Some(cycle_type) = meta.get_cycle_type() {
        match cycle_type {
            Cycle::G0 => stats.inc_g0(),
            Cycle::G1c => stats.inc_g1(),
            Cycle::G2 => stats.inc_g2(),
        }
    }
}

pub fn execute_logic<'a>(
    meta: &mut StatsBucket,
    request: Request,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    use SmallBankTransactionProfile::*;
    let parameters = request.get_parameters();

    match parameters {
        Parameters::SmallBank(p) => match p {
            Amalgamate(params) => {
                smallbank::procedures::amalgmate(meta, params.clone(), scheduler, database)
            }
            Balance(params) => {
                smallbank::procedures::balance(meta, params.clone(), scheduler, database)
            }
            SmallBankTransactionProfile::DepositChecking(params) => {
                smallbank::procedures::deposit_checking(meta, params.clone(), scheduler, database)
            }
            SmallBankTransactionProfile::SendPayment(params) => {
                smallbank::procedures::send_payment(meta, params.clone(), scheduler, database)
            }
            SmallBankTransactionProfile::TransactSaving(params) => {
                smallbank::procedures::transact_savings(meta, params.clone(), scheduler, database)
            }
            SmallBankTransactionProfile::WriteCheck(params) => {
                smallbank::procedures::write_check(meta, params.clone(), scheduler, database)
            }
        },
        Parameters::Ycsb(p) => match p {
            YcsbTransactionProfile::General(params) => {
                ycsb::procedures::transaction(meta, params.clone(), scheduler, database)
            }
        },
        Parameters::Tatp(p) => match p {
            TatpTransactionProfile::GetSubscriberData(params) => {
                tatp::procedures::get_subscriber_data(meta, params.clone(), scheduler, database)
            }
            TatpTransactionProfile::GetAccessData(params) => {
                tatp::procedures::get_access_data(meta, params.clone(), scheduler, database)
            }
            TatpTransactionProfile::GetNewDestination(params) => {
                tatp::procedures::get_new_destination(meta, params.clone(), scheduler, database)
            }
            TatpTransactionProfile::UpdateLocationData(params) => {
                tatp::procedures::update_location(meta, params.clone(), scheduler, database)
            }
            TatpTransactionProfile::UpdateSubscriberData(params) => {
                tatp::procedures::update_subscriber_data(meta, params.clone(), scheduler, database)
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
            let queries = config.get_int("queries").unwrap() as u64;

            let gen = YcsbGenerator::new(
                core_id,
                sf,
                set_seed,
                seed,
                theta,
                update_rate,
                serializable_rate,
                queries,
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
