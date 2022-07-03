use crate::common::error::NonFatalError;
use crate::common::global_state::GlobalState;
use crate::common::message::Request;
use crate::common::parameter_generation::ParameterGenerator;
use crate::common::statistics::local::LocalStatistics;
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

    loop {
        if completed_transactions == max_transactions {
            debug!("Max transactions completed: {} ", completed_transactions);
            break;
        } else {
            let request = transaction_generator.get_next();
            let isolation_level = request.get_isolation_level();
            stats.start_latency();

            let mut response;
            // let mut guard s = WaitGuards::new();

            loop {
                let (mut meta, _d) = scheduler.begin(isolation_level);

                stats.start_tx();
                // let transaction_id = meta.get_transaction_id();

                response = execute_logic(&mut meta, request.clone(), scheduler, database);

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
                                break;
                            }
                            Err(_) => {
                                // case 1 when commit fails
                                stats.inc_aborts();
                                stats.inc_commit_aborts();
                                stats.start_wait_manager();
                                // let problem_transactions = meta.get_problem_transactions();
                                // let g = wait_manager
                                //     .wait(transaction_id.extract(), problem_transactions);
                                // guards.guards.replace(g);

                                stats.stop_wait_manager();
                            }
                        }
                    }
                    Err(e) => match e {
                        NonFatalError::NoccError => {}
                        NonFatalError::SerializationGraphError(e) => {
                            // case 2 when logic fails

                            match e {
                                SerializationGraphError::ReadOpCycleFound => {
                                    stats.inc_logic_aborts();
                                    stats.inc_read_cf();
                                }
                                SerializationGraphError::ReadOpCascasde => {
                                    stats.inc_logic_aborts();
                                    stats.inc_read_cf();
                                }
                                _ => {}
                            }

                            stats.inc_aborts();

                            if let SerializationGraphError::CycleFound = e {
                                stats.inc_logic_aborts();
                            }

                            stats.start_wait_manager();
                            // let problem_transactions = meta.get_problem_transactions();
                            // wait_manager.wait(transaction_id.extract(), problem_transactions);
                            stats.stop_wait_manager();
                        }
                        NonFatalError::SmallBankError(_) => {
                            scheduler.abort(&mut meta, database);
                            stats.inc_not_found();

                            // TODO: abort then commit
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
) -> Result<(), NonFatalError> {
    use SmallBankTransactionProfile::*;
    let parameters = request.get_parameters();

    match parameters.get() {
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
