use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::IsolationLevel;

use crossbeam_epoch as epoch;
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
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(isolation); // register
            scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get customer id
            scheduler.read_value(1, 1, offset, &meta, database, guard)?; // get checking balance
            scheduler.read_value(2, 1, offset, &meta, database, guard)?; // get savings balance
            scheduler.commit(&meta, database, guard, TransactionType::ReadOnly)?; // commit

            Ok(Success::new(meta, None, None, None, None, None))
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
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(isolation);
            scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get customer id
            let res = scheduler.read_value(1, 1, offset, &meta, database, guard)?; // get current balance
            let mut balance = Data::from(f64::try_from(res)? + params.value); // new balance
            scheduler.write_value(&mut balance, 1, 1, offset, &meta, database, guard)?; // write 1 -- update balance
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?;

            Ok(Success::new(meta, None, None, None, None, None))
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
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(isolation); // register
            scheduler.read_value(0, 0, offset, &meta, database, guard)?; // get customer id
            let res = scheduler.read_value(2, 1, offset, &meta, database, guard)?; // get savings balance
            let balance = f64::try_from(res)? + params.value; // new balance
            if balance < 0.0 {
                scheduler.abort(&meta, database, guard);
                return Err(SmallBankError::InsufficientFunds.into());
            }
            scheduler.write_value(
                &mut Data::from(balance),
                2,
                1,
                offset,
                &meta,
                database,
                guard,
            )?; // write 1 -- update saving balance
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?;

            Ok(Success::new(meta, None, None, None, None, None))
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
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset1 = params.name1 as usize;
            let offset2 = params.name2 as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(isolation); // register
            scheduler.read_value(0, 0, offset1, &meta, database, guard)?; // read 1 -- get customer1 id
            let res1 = scheduler.read_value(2, 1, offset1, &meta, database, guard)?; // read 2 -- current savings balance (customer1)
            let res2 = scheduler.read_value(1, 1, offset1, &meta, database, guard)?; // read 3 -- current checking balance (customer1)
            let val = &mut Data::Double(0.0);
            scheduler.write_value(val, 2, 1, offset1, &meta, database, guard)?; // write 1 -- update saving balance (cust1)
            scheduler.write_value(val, 1, 1, offset1, &meta, database, guard)?; // write 2 -- update checking balance (cust1)
            let sum = f64::try_from(res1)? + f64::try_from(res2)?; // amount to send
            scheduler.read_value(0, 0, offset2, &meta, database, guard)?; // read 4 -- get customer2 id
            let res3 = scheduler.read_value(1, 1, offset2, &meta, database, guard)?; // read 5 -- current checking balance (customer2)
            let mut bal = Data::Double(sum + f64::try_from(res3)?);
            scheduler.write_value(&mut bal, 1, 1, offset2, &meta, database, guard)?;
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?;

            Ok(Success::new(meta, None, None, None, None, None))
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
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset = params.name as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(isolation);
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
            let mut new_check = Data::Double(total - amount);
            scheduler.write_value(&mut new_check, 1, 1, offset, &meta, database, guard)?; // update checking balance
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?;

            Ok(Success::new(meta, None, None, None, None, None))
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
) -> Result<Success, NonFatalError> {
    match &*database {
        Database::SmallBank(_) => {
            let offset1 = params.name1 as usize;
            let offset2 = params.name2 as usize;
            let guard = &epoch::pin(); // pin thread
            let meta = scheduler.begin(isolation); // register
            scheduler.read_value(0, 0, offset1, &meta, database, guard)?; // get cust1 id
            let mut checking =
                f64::try_from(scheduler.read_value(1, 1, offset1, &meta, database, guard)?)?; // get cust1 checking
            checking -= params.value;
            if checking < 0.0 {
                scheduler.abort(&meta, database, guard);
                return Err(SmallBankError::InsufficientFunds.into());
            }
            let val1 = &mut Data::Double(checking);
            scheduler.write_value(val1, 1, 1, offset1, &meta, database, guard)?; // update cust1 checking balance
            scheduler.read_value(0, 0, offset2, &meta, database, guard)?; // get cust2 id
            let mut checking =
                f64::try_from(scheduler.read_value(1, 1, offset2, &meta, database, guard)?)?; // get cust2 checking
            checking += params.value;
            let val2 = &mut Data::Double(checking);
            scheduler.write_value(val2, 1, 1, offset2, &meta, database, guard)?; // update cust2 checking
            scheduler.commit(&meta, database, guard, TransactionType::ReadWrite)?;

            Ok(Success::new(meta, None, None, None, None, None))
        }
        _ => panic!("unexpected database"),
    }
}
