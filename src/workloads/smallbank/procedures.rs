use crate::common::message::Success;
use crate::common::stored_procedure_result::StoredProcedureResult;
use crate::common::value_id::ValueId;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::IsolationLevel;

use std::convert::TryFrom;
use tracing::instrument;

/// Balance transaction.
///
/// Sum the balances of a customer's checking and savings accounts.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn balance<'a>(
    params: Balance,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> StoredProcedureResult {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.get_name();

            let mut meta = scheduler.begin(isolation);

            // get customer id
            let cust = ValueId::new(0, 0, offset);
            if let Err(error) = scheduler.read_value(cust, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get checking balance
            let checking = ValueId::new(1, 1, offset);
            if let Err(error) = scheduler.read_value(checking, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get checking balance
            let savings = ValueId::new(2, 1, offset);
            if let Err(error) = scheduler.read_value(savings, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            if let Err(error) = scheduler.commit(&mut meta, database, TransactionType::ReadOnly) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            let success = Success::default(meta.get_transaction_id());
            StoredProcedureResult::from_success(success, isolation, &mut meta)
        }
        _ => panic!("unexpected database"),
    }
}

/// Deposit checking transaction
///
/// Increase checking balance by X amount.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn deposit_checking<'a>(
    params: DepositChecking,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> StoredProcedureResult {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.get_name();

            let mut meta = scheduler.begin(isolation);

            // get customer id
            let cust = ValueId::new(0, 0, offset);
            if let Err(error) = scheduler.read_value(cust, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get current balance
            let bal = ValueId::new(1, 1, offset);
            let current_balance = match scheduler.read_value(bal.clone(), &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };

            // update checking balance
            let mut new_val =
                Data::from(f64::try_from(current_balance).unwrap() + params.get_value());
            if let Err(error) = scheduler.write_value(&mut new_val, bal, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            if let Err(error) = scheduler.commit(&mut meta, database, TransactionType::ReadWrite) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            let success = Success::default(meta.get_transaction_id());
            StoredProcedureResult::from_success(success, isolation, &mut meta)
        }
        _ => panic!("unexpected database"),
    }
}

/// TransactSavings transaction.
///
/// TODO: logic as per Durner, but does not make sense.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn transact_savings<'a>(
    params: TransactSaving,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> StoredProcedureResult {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name;

            let mut meta = scheduler.begin(isolation);

            // get customer id
            let cust = ValueId::new(0, 0, offset);
            if let Err(error) = scheduler.read_value(cust, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get current balance
            let savings = ValueId::new(2, 1, offset);
            let res = match scheduler.read_value(savings.clone(), &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };

            // abort if balance would be negative
            let balance = f64::try_from(res).unwrap() + params.value;
            if balance < 0.0 {
                scheduler.abort(&mut meta, database);
                let error = SmallBankError::InsufficientFunds.into();

                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            //  update saving balance
            let val = &mut Data::from(balance);
            if let Err(error) = scheduler.write_value(val, savings, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            if let Err(error) = scheduler.commit(&mut meta, database, TransactionType::ReadWrite) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            let success = Success::default(meta.get_transaction_id());
            StoredProcedureResult::from_success(success, isolation, &mut meta)
        }
        _ => panic!("unexpected database"),
    }
}

/// Amalgamate transaction.
///
/// Move all the funds from one customer to another.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn amalgmate<'a>(
    params: Amalgamate,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> StoredProcedureResult {
    match &*database {
        Database::SmallBank(_) => {
            let offset1 = params.name1;
            let offset2 = params.name2;

            let mut meta = scheduler.begin(isolation);

            // cust1
            let cust1 = ValueId::new(0, 0, offset1);
            let savings1 = ValueId::new(2, 1, offset1);
            let checking1 = ValueId::new(1, 1, offset1);

            if let Err(error) = scheduler.read_value(cust1, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // read 2 -- current savings balance (customer1)
            let res1 = match scheduler.read_value(savings1, &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };

            // read 3 -- current checking balance (customer1)
            let res2 = match scheduler.read_value(checking1, &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };

            let val = &mut Data::Double(0.0);

            // write 1 -- update saving balance (cust1)
            if let Err(error) = scheduler.write_value(val, savings1, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // write 2 -- update checking balance (cust1)
            if let Err(error) = scheduler.write_value(val, checking1, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // amount to send
            let sum = f64::try_from(res1).unwrap() + f64::try_from(res2).unwrap();

            // cust2
            let cust2 = ValueId::new(0, 0, offset2);
            let checking2 = ValueId::new(1, 1, offset2);

            // read 4 -- get customer2 id
            if let Err(error) = scheduler.read_value(cust2, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // read 5 -- current checking balance (customer2)
            let res3 = match scheduler.read_value(checking2, &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };

            let bal = &mut Data::Double(sum + f64::try_from(res3).unwrap());

            // write 3 -- update checking balance (cust2)
            if let Err(error) = scheduler.write_value(bal, checking2, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // commit
            if let Err(error) = scheduler.commit(&mut meta, database, TransactionType::ReadWrite) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            let success = Success::default(meta.get_transaction_id());
            StoredProcedureResult::from_success(success, isolation, &mut meta)
        }
        _ => panic!("unexpected database"),
    }
}

/// Write check transaction.
///
/// Write a check against an account taking funds from checking; applying overdraft charge if needed.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn write_check<'a>(
    params: WriteCheck,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> StoredProcedureResult {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;

            let mut meta = scheduler.begin(isolation);

            // get customer id
            let cust = ValueId::new(0, 0, offset);
            if let Err(error) = scheduler.read_value(cust, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get savings balance
            let savings = ValueId::new(2, 1, offset);
            let bal1 = match scheduler.read_value(savings, &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };
            let bal1 = f64::try_from(bal1).unwrap();

            // get checking balance
            let checking = ValueId::new(1, 1, offset);
            let bal2 = match scheduler.read_value(checking, &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };
            let bal2 = f64::try_from(bal2).unwrap();

            // apply overdraft charge
            let total = bal1 + bal2; // total balance
            let mut amount = params.value;
            if total < amount {
                amount += 1.0;
            }

            // update checking balance
            let new_check = &mut Data::Double(total - amount);
            if let Err(error) = scheduler.write_value(new_check, checking, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // commit
            if let Err(error) = scheduler.commit(&mut meta, database, TransactionType::ReadWrite) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            let success = Success::default(meta.get_transaction_id());
            StoredProcedureResult::from_success(success, isolation, &mut meta)
        }
        _ => panic!("unexpected database"),
    }
}

/// Send payment transaction.
///
/// Transfer money between accounts; if there is sufficient funds in the checking account.
#[instrument(level = "debug", skip(params, scheduler, database, isolation))]
pub fn send_payment<'a>(
    params: SendPayment,
    scheduler: &'a Scheduler,
    database: &'a Database,
    isolation: IsolationLevel,
) -> StoredProcedureResult {
    match &*database {
        Database::SmallBank(_) => {
            let offset1 = params.name1;
            let offset2 = params.name2;

            let mut meta = scheduler.begin(isolation);

            // get cust1 id
            let cust1 = ValueId::new(0, 0, offset1);
            if let Err(error) = scheduler.read_value(cust1, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get cust1 checking
            let checking1 = ValueId::new(1, 1, offset1);
            let bal1 = match scheduler.read_value(checking1.clone(), &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };

            // if balance would be negative then abort
            let mut bal1 = f64::try_from(bal1).unwrap();
            bal1 -= params.value;
            if bal1 < 0.0 {
                scheduler.abort(&mut meta, database);
                let error = SmallBankError::InsufficientFunds.into();
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // update value cust1 checking balance to new balance
            let val1 = &mut Data::Double(bal1);
            if let Err(error) = scheduler.write_value(val1, checking1, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get cust2 id
            let cust2 = ValueId::new(0, 0, offset2);
            if let Err(error) = scheduler.read_value(cust2, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // get cust2 checking
            let checking2 = ValueId::new(1, 1, offset2);
            let bal2 = match scheduler.read_value(checking2.clone(), &mut meta, database) {
                Ok(bal) => bal,
                Err(error) => {
                    return StoredProcedureResult::from_error(error, isolation, &mut meta);
                }
            };

            let mut bal2 = f64::try_from(bal2).unwrap();
            bal2 += params.value;

            // update cust2 checking balance
            let val2 = &mut Data::Double(bal2);
            if let Err(error) = scheduler.write_value(val2, checking2, &mut meta, database) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            // commit
            if let Err(error) = scheduler.commit(&mut meta, database, TransactionType::ReadWrite) {
                return StoredProcedureResult::from_error(error, isolation, &mut meta);
            }

            let success = Success::default(meta.get_transaction_id());
            StoredProcedureResult::from_success(success, isolation, &mut meta)
        }
        _ => panic!("unexpected database"),
    }
}
