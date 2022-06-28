use crate::common::error::NonFatalError;
use crate::common::message::{Outcome, Parameters, Request, Response};
use crate::common::parameter_generation::ParameterGenerator;
use crate::common::statistics::LocalStatistics;
use crate::scheduler::Scheduler;
use crate::storage::Database;
// use crate::workloads::acid::paramgen::{AcidGenerator, AcidTransactionProfile};
// use crate::workloads::dummy::paramgen::{DummyGenerator, DummyTransactionProfile};
use crate::workloads::smallbank::paramgen::{SmallBankGenerator, SmallBankTransactionProfile};
// use crate::workloads::tatp::paramgen::{TatpGenerator, TatpTransactionProfile};
// use crate::workloads::ycsb::paramgen::{YcsbGenerator, YcsbTransactionProfil
// e};
// use crate::workloads::{acid, dummy, smallbank, tatp, ycsb};
use crate::common::global_state::GlobalState;
use crate::common::utils;
use crate::workloads::smallbank;

use config::Config;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tracing::debug;

pub fn run(core_id: usize, stats_tx: mpsc::Sender<LocalStatistics>, global_state: &GlobalState) {
    // global state
    let config = global_state.get_config();
    let scheduler = global_state.get_scheduler();
    let database = global_state.get_database();
    let wait_manager = global_state.get_wait_manager();

    // local state
    let mut stats = LocalStatistics::new(config, core_id);
    let mut transaction_generator = get_transaction_generator(config, core_id);
    let mut txn_log_handle = utils::create_transaction_log(config, core_id);

    let max_runtime = config.get_int("timeout").unwrap() as u64;
    let timeout = compute_timeout(max_runtime);

    let worker_start = Instant::now();

    let max_transactions = config.get_int("transactions").unwrap() as u32;
    let mut completed_transactions = 0;

    loop {
        if completed_transactions == max_transactions {
            debug!("Max transactions completed: {} ", completed_transactions);
            break;
        } else if Instant::now() > timeout {
            debug!("Timeout reached: {} minute(s)", max_runtime);
            break;
        } else {
            let request = transaction_generator.get_next();
            let txn_start = Instant::now();
            let mut response;
            let mut old_transaction_id = 0;
            let mut guards = None;
            loop {
                response = execute_transaction(request.clone(), scheduler, database);
                let transaction_id = response.get_transaction_id();
                let transaction_outcome = response.get_outcome();

                // if transaction was restarted and had some locks
                if guards.is_some() {
                    wait_manager.release(old_transaction_id, guards.unwrap());
                }

                stats.record(&response);

                match transaction_outcome {
                    Outcome::Committed(_) => break,
                    Outcome::Aborted(err) => match err {
                        NonFatalError::RowNotFound(_, _) => break,
                        NonFatalError::SmallBankError(_) => break,
                        NonFatalError::UnableToConvertFromDataType(_, _) => break,
                        NonFatalError::RowDirty(_) => panic!("dont get here for sgt"),
                        NonFatalError::NonSerializable => panic!("dont get here for sgt"),
                        NonFatalError::SerializationGraphError(_) => {}
                        NonFatalError::WaitHitError(_) => {}
                        NonFatalError::AttendezError(_) => {}
                    },
                }

                let problem_transactions = response.get_problem_transactions();

                // let wait_start = Instant::now();
                // guards = Some(wait_manager.wait(transaction_id as u64, problem_transactions));
                guards = None;
                // stats.stop_wait_manager(wait_start);

                old_transaction_id = transaction_id;
            }

            response.set_total_latency(txn_start.elapsed().as_nanos());
            utils::append_to_log(&mut txn_log_handle, &response);
            completed_transactions += 1;
        }
    }

    stats.stop_worker(worker_start);

    stats_tx.send(stats).unwrap();
}

fn compute_timeout(max_runtime: u64) -> Instant {
    let start = Instant::now();
    let duration = Duration::new(max_runtime * 60, 0);
    start + duration
}

pub fn execute_transaction<'a>(
    request: Request,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Response {
    let request_no = request.get_request_no();
    let transaction = request.get_transaction();
    let isolation = request.get_isolation_level();

    let result = match request.get_parameters() {
        // Parameters::Tatp(params) => match params {
        //     TatpTransactionProfile::GetSubscriberData(params) => {
        //         tatp::procedures::get_subscriber_data(
        //             params.clone(),
        //             scheduler,
        //             database,
        //             isolation,
        //         )
        //     }
        //     TatpTransactionProfile::GetAccessData(params) => {
        //         tatp::procedures::get_access_data(params.clone(), scheduler, database, isolation)
        //     }
        //     TatspTransactionProfile::GetNewDestination(params) => {
        //         tatp::procedures::get_new_destination(
        //             params.clone(),
        //             scheduler,
        //             database,
        //             isolation,
        //         )
        //     }
        //     TatpTransactionProfile::UpdateLocationData(params) => {
        //         tatp::procedures::update_location(params.clone(), scheduler, database, isolation)
        //     }
        //     TatpTransactionProfile::UpdateSubscriberData(params) => {
        //         tatp::procedures::update_subscriber_data(
        //             params.clone(),
        //             scheduler,
        //             database,
        //             isolation,
        //         )
        //     }
        // },
        Parameters::SmallBank(params) => match params {
            SmallBankTransactionProfile::Amalgamate(params) => {
                smallbank::procedures::amalgmate(params.clone(), scheduler, database, isolation)
            }
            SmallBankTransactionProfile::Balance(params) => {
                smallbank::procedures::balance(params.clone(), scheduler, database, isolation)
            }
            SmallBankTransactionProfile::DepositChecking(params) => {
                smallbank::procedures::deposit_checking(
                    params.clone(),
                    scheduler,
                    database,
                    isolation,
                )
            }
            SmallBankTransactionProfile::SendPayment(params) => {
                smallbank::procedures::send_payment(params.clone(), scheduler, database, isolation)
            }
            SmallBankTransactionProfile::TransactSaving(params) => {
                smallbank::procedures::transact_savings(
                    params.clone(),
                    scheduler,
                    database,
                    isolation,
                )
            }
            SmallBankTransactionProfile::WriteCheck(params) => {
                smallbank::procedures::write_check(params.clone(), scheduler, database, isolation)
            }
        },

        _ => unimplemented!(),
    };

    Response::new(request_no, transaction.clone(), result)
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
        // "acid" => {
        //     let anomaly = config.get_str("anomaly").unwrap();
        //     let delay = config.get_int("delay").unwrap() as u64;
        //     let gen = AcidGenerator::new(thread_id, sf, set_seed, seed, &anomaly, delay);

        //     ParameterGenerator::Acid(gen)
        // }
        // "dummy" => {
        //     let gen = DummyGenerator::new(thread_id, sf, set_seed, seed);

        //     ParameterGenerator::Dummy(gen)
        // }
        // "tatp" => {
        //     let use_nurand = config.get_bool("nurand").unwrap();
        //     let gen = TatpGenerator::new(thread_id, sf, set_seed, seed, use_nurand);
        //     ParameterGenerator::Tatp(gen)
        // }
        // "ycsb" => {
        //     let theta = config.get_float("theta").unwrap();
        //     let update_rate = config.get_float("update_rate").unwrap();
        //     let serializable_rate = config.get_float("serializable_rate").unwrap();

        //     let gen = YcsbGenerator::new(
        //         thread_id,
        //         sf,
        //         set_seed,
        //         seed,
        //         theta,
        //         update_rate,
        //         serializable_rate,
        //     );
        //     ParameterGenerator::Ycsb(gen)
        // }
        _ => unimplemented!(),
    }
}
