use crate::common::error::NonFatalError;
use crate::common::global_state::GlobalState;
use crate::common::message::{Outcome, Request, Response};
use crate::common::parameter_generation::ParameterGenerator;
use crate::common::statistics::local::LocalStatistics;
use crate::scheduler::Scheduler;
use crate::storage::Database;
use crate::workloads::smallbank;
use crate::workloads::smallbank::paramgen::{SmallBankGenerator, SmallBankTransactionProfile};

use config::Config;
use std::sync::mpsc;
use tracing::debug;

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

            stats.start_latency();

            let mut response;
            let mut guards = None;

            loop {
                response = execute_transaction(request.clone(), scheduler, database);

                let transaction_id = response.get_transaction_id();
                let transaction_outcome = response.get_outcome();

                // if transaction was restarted and had some locks
                if guards.is_some() {
                    wait_manager.release(guards.unwrap());
                }

                match transaction_outcome {
                    Outcome::Committed(_) => {
                        stats.inc_commits();
                        break;
                    }
                    Outcome::Aborted(err) => match err {
                        NonFatalError::RowNotFound(_, _) => break,
                        NonFatalError::SmallBankError(_) => {
                            stats.inc_not_found();
                            break;
                        }
                        NonFatalError::UnableToConvertFromDataType(_, _) => panic!("oh no"),
                        NonFatalError::RowDirty(_) => {}
                        NonFatalError::NonSerializable => {}
                        NonFatalError::SerializationGraphError(_) => {}
                        NonFatalError::WaitHitError(_) => {}
                        NonFatalError::AttendezError(_) => {}
                    },
                }
                stats.inc_aborts();

                let problem_transactions = response.get_problem_transactions();

                stats.start_wait_manager();
                guards = Some(wait_manager.wait(transaction_id as u64, problem_transactions));
                stats.stop_wait_manager();
            }

            stats.stop_latency();

            completed_transactions += 1;
        }
    }

    stats.stop_worker();

    stats_tx.send(stats).unwrap();
}

pub fn execute_transaction<'a>(
    request: Request,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Response {
    let request_no = request.get_request_no();
    let transaction = request.get_transaction();
    let isolation = request.get_isolation_level();

    let parameters = request.get_parameters();

    let result = match parameters.get() {
        SmallBankTransactionProfile::Amalgamate(params) => {
            smallbank::procedures::amalgmate(params.clone(), scheduler, database, isolation)
        }
        SmallBankTransactionProfile::Balance(params) => {
            smallbank::procedures::balance(params.clone(), scheduler, database, isolation)
        }
        SmallBankTransactionProfile::DepositChecking(params) => {
            smallbank::procedures::deposit_checking(params.clone(), scheduler, database, isolation)
        }
        SmallBankTransactionProfile::SendPayment(params) => {
            smallbank::procedures::send_payment(params.clone(), scheduler, database, isolation)
        }
        SmallBankTransactionProfile::TransactSaving(params) => {
            smallbank::procedures::transact_savings(params.clone(), scheduler, database, isolation)
        }
        SmallBankTransactionProfile::WriteCheck(params) => {
            smallbank::procedures::write_check(params.clone(), scheduler, database, isolation)
        }
    };

    Response::new(request_no, transaction.clone(), result, 0)
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
