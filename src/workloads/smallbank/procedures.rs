use crate::common::error::NonFatalError;
use crate::scheduler::Scheduler;
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};

use crossbeam_epoch as epoch;
use std::convert::TryFrom;

/// Balance transaction.
///
/// Sum the balances of a customer's checking and savings accounts.
pub fn balance<'a>(
    params: Balance,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(); // register
            scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get customer id
            scheduler.read_value(1, 1, offset, &meta, database, guard)?; // get checking balance
            scheduler.read_value(2, 1, offset, &meta, database, guard)?; // get savings balance
            scheduler.commit(&meta, database, guard)?; // commit

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Deposit checking transaction
///
/// Increase checking balance by X amount.
pub fn deposit_checking<'a>(
    params: DepositChecking,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin();
            scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get customer id
            let res = scheduler.read_value(1, 1, offset, &meta, database, guard)?; // get current balance
            let balance = Data::from(f64::try_from(res)? + params.value); // new balance
            scheduler.write_value(&balance, 1, 1, offset, &meta, database, guard)?; // write 1 -- update balance
            scheduler.commit(&meta, database, guard)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// TransactSavings transaction.
///
/// TODO: logic as per Durner, but does not make sense.
pub fn transact_savings<'a>(
    params: TransactSaving,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(); // register
            scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get customer id
            let res = scheduler.read_value(2, 1, offset, &meta, database, guard)?; // get savings balance
            let balance = f64::try_from(res)? + params.value; // new balance
            if balance < 0.0 {
                scheduler.abort(&meta, database, guard);
                return Err(SmallBankError::InsufficientFunds.into());
            }
            scheduler.write_value(&Data::from(balance), 2, 1, offset, &meta, database, guard)?; // write 1 -- update saving balance
            scheduler.commit(&meta, database, guard)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Amalgamate transaction.
///
/// Move all the funds from one customer to another.
pub fn amalgmate<'a>(
    params: Amalgamate,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset1 = params.name1 as usize;
            let offset2 = params.name2 as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(); // register
            scheduler.read_value(0, 0, offset1, &meta, database, guard)?; // read 1 -- get customer1 id
            let res1 = scheduler.read_value(2, 1, offset1, &meta, database, guard)?; // read 2 -- current savings balance (customer1)
            let res2 = scheduler.read_value(1, 1, offset1, &meta, database, guard)?; // read 3 -- current checking balance (customer1)
            scheduler.write_value(&Data::Double(0.0), 2, 1, offset1, &meta, database, guard)?; // write 1 -- update saving balance (cust1)
            scheduler.write_value(&Data::Double(0.0), 1, 1, offset1, &meta, database, guard)?; // write 2 -- update checking balance (cust1)
            let sum = f64::try_from(res1)? + f64::try_from(res2)?; // amount to send
            scheduler.read_value(0, 0, offset2, &meta, database, guard)?; // read 4 -- get customer2 id
            let res3 = scheduler.read_value(1, 1, offset2, &meta, database, guard)?; // read 5 -- current checking balance (customer2)
            let bal = sum + f64::try_from(res3)?;
            scheduler.write_value(&Data::Double(bal), 1, 1, offset2, &meta, database, guard)?;
            scheduler.commit(&meta, database, guard)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Write check transaction.
///
/// Write a check against an account taking funds from checking; applying overdraft charge if needed.
pub fn write_check<'a>(
    params: WriteCheck,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin();
            scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get customer id
            let savings =
                f64::try_from(scheduler.read_value(2, 1, offset, &meta, database, guard)?)?; // get savings balance
            let checking =
                f64::try_from(scheduler.read_value(1, 1, offset, &meta, database, guard)?)?; // get checking balance
            let total = savings + checking; // total balance
            let mut amount = params.value;
            if total < amount {
                amount += 1.0; // apply overdraft charge
            }
            let new_check = total - amount;
            scheduler.write_value(
                &Data::Double(new_check),
                1,
                1,
                offset,
                &meta,
                database,
                guard,
            )?; // update checking balance
            scheduler.commit(&meta, database, guard)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Send payment transaction.
///
/// Transfer money between accounts; if there is sufficient funds in the checking account.
pub fn send_payment<'a>(
    params: SendPayment,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset1 = params.name1 as usize;
            let offset2 = params.name2 as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(); // register
            scheduler.read_value(0, 0, offset1, &meta, database, guard)?; // get cust1 id
            let mut checking =
                f64::try_from(scheduler.read_value(1, 1, offset1, &meta, database, guard)?)?; // get cust1 checking
            checking -= params.value;
            if checking < 0.0 {
                scheduler.abort(&meta, database, guard);
                return Err(SmallBankError::InsufficientFunds.into());
            }
            scheduler.write_value(
                &Data::Double(checking),
                1,
                1,
                offset1,
                &meta,
                database,
                guard,
            )?; // update cust1 checking balance
            scheduler.read_value(0, 0, offset2, &meta, database, guard)?; // get cust2 id
            let mut checking =
                f64::try_from(scheduler.read_value(1, 1, offset2, &meta, database, guard)?)?; // get cust2 checking
            checking += params.value;
            scheduler.write_value(
                &Data::Double(checking),
                1,
                1,
                offset2,
                &meta,
                database,
                guard,
            )?; // update cust2 checking
            scheduler.commit(&meta, database, guard)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}
