use crate::common::error::NonFatalError;
use crate::common::message::Success;
use crate::common::statistics::protocol_diagnostics::ProtocolDiagnostics;
use crate::scheduler::{Scheduler, TransactionType};
use crate::storage::access::TransactionId;
use crate::storage::datatype::Data;
use crate::storage::Database;
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::IsolationLevel;

use std::convert::TryFrom;
use std::time::Instant;
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
            let offset = params.get_name();

            let meta = scheduler.begin(isolation);
            scheduler.read_value(0, 0, offset, &meta, database)?; // get customer id
            scheduler.read_value(1, 1, offset, &meta, database)?; // get checking balance
            scheduler.read_value(2, 1, offset, &meta, database)?; // get savings balance

            let diagnostics = timed_commit(&meta, database, TransactionType::ReadOnly, scheduler)?;

            // let mut diag = scheduler.commit(&meta, database, TransactionType::ReadOnly)?;

            Ok(Success::diagnostics(meta, diagnostics))
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
            let offset = params.get_name();

            let meta = scheduler.begin(isolation);
            scheduler.read_value(0, 0, offset, &meta, database)?; // get customer id
            let current_balance = scheduler.read_value(1, 1, offset, &meta, database)?;
            let mut new_balance =
                Data::from(f64::try_from(current_balance).unwrap() + params.get_value());
            // scheduler.write_value(&mut new_balance, 1, 1, offset, &meta, database)?; // update checking balance
            let write_dur =
                timed_write(&mut new_balance, 1, 1, offset, &meta, scheduler, database)?; // update checking balance

            let start = Instant::now();
            let mut diag = scheduler.commit(&meta, database, TransactionType::ReadWrite)?;
            let dur = start.elapsed().as_nanos();

            diag.set_write_time(write_dur);
            diag.set_commit_time(dur);

            Ok(Success::diagnostics(meta, diag))
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

            let meta = scheduler.begin(isolation); // register
            scheduler.read_value(0, 0, offset, &meta, database)?; // get customer id
            let res = scheduler.read_value(2, 1, offset, &meta, database)?; // get savings balance
            let balance = f64::try_from(res)? + params.value; // new balance
            if balance < 0.0 {
                scheduler.abort(&meta, database);
                return Err(SmallBankError::InsufficientFunds.into());
            }

            let write_dur = timed_write(
                &mut Data::from(balance),
                2,
                1,
                offset,
                &meta,
                scheduler,
                database,
            )?;

            // scheduler.write_value(&mut Data::from(balance), 2, 1, offset, &meta, database)?; // write 1 -- update saving balance

            let start = Instant::now();
            let mut diag = scheduler.commit(&meta, database, TransactionType::ReadWrite)?;
            let dur = start.elapsed().as_nanos();

            diag.set_write_time(write_dur);
            diag.set_commit_time(dur);

            Ok(Success::diagnostics(meta, diag))
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

            let meta = scheduler.begin(isolation); // register
            scheduler.read_value(0, 0, offset1, &meta, database)?; // read 1 -- get customer1 id
            let res1 = scheduler.read_value(2, 1, offset1, &meta, database)?; // read 2 -- current savings balance (customer1)
            let res2 = scheduler.read_value(1, 1, offset1, &meta, database)?; // read 3 -- current checking balance (customer1)
            let val = &mut Data::Double(0.0);
            let w1 = timed_write(val, 2, 1, offset1, &meta, scheduler, database)?; // write 1 -- update saving balance (cust1)
            let w2 = timed_write(val, 1, 1, offset1, &meta, scheduler, database)?; // write 2 -- update checking balance (cust1)
            let sum = f64::try_from(res1)? + f64::try_from(res2)?; // amount to send
            scheduler.read_value(0, 0, offset2, &meta, database)?; // read 4 -- get customer2 id
            let res3 = scheduler.read_value(1, 1, offset2, &meta, database)?; // read 5 -- current checking balance (customer2)

            let mut bal = Data::Double(sum + f64::try_from(res3)?);
            let w3 = timed_write(&mut bal, 1, 1, offset2, &meta, scheduler, database)?;

            let mut diagnostics =
                timed_commit(&meta, database, TransactionType::ReadWrite, scheduler)?;
            diagnostics.set_write_time((w1 + w2 + w3) / 3);

            Ok(Success::diagnostics(meta, diagnostics))
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

            let meta = scheduler.begin(isolation);
            scheduler.read_value(0, 0, offset, &meta, database)?; // get customer id
            let savings = f64::try_from(scheduler.read_value(2, 1, offset, &meta, database)?)?; // get savings balance
            let checking = f64::try_from(scheduler.read_value(1, 1, offset, &meta, database)?)?; // get checking balance
            let total = savings + checking; // total balance
            let mut amount = params.value;
            if total < amount {
                amount += 1.0; // apply overdraft charge
            }
            let mut new_check = Data::Double(total - amount);

            let write_dur = timed_write(&mut new_check, 1, 1, offset, &meta, scheduler, database)?; // update checking balance

            let mut diagnostics =
                timed_commit(&meta, database, TransactionType::ReadWrite, scheduler)?;
            diagnostics.set_write_time(write_dur);

            Ok(Success::diagnostics(meta, diagnostics))
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
            let offset1 = params.name1;
            let offset2 = params.name2;

            let meta = scheduler.begin(isolation);

            // get cust1 id
            scheduler.read_value(0, 0, offset1, &meta, database)?;

            // get cust1 checking
            let mut checking =
                f64::try_from(scheduler.read_value(1, 1, offset1, &meta, database)?)?;
            checking -= params.value;
            if checking < 0.0 {
                scheduler.abort(&meta, database);
                return Err(SmallBankError::InsufficientFunds.into());
            }

            // update cust1 checking balance
            let val1 = &mut Data::Double(checking);
            let write_dur_1 = timed_write(val1, 1, 1, offset1, &meta, scheduler, database)?;
            // scheduler.write_value(val1, 1, 1, offset1, &meta, database)?;

            // get cust2 id
            scheduler.read_value(0, 0, offset2, &meta, database)?;

            // get cust2 checking
            let mut checking =
                f64::try_from(scheduler.read_value(1, 1, offset2, &meta, database)?)?;
            checking += params.value;

            // update cust2 checking
            let val2 = &mut Data::Double(checking);
            let write_dur_2 = timed_write(val2, 1, 1, offset2, &meta, scheduler, database)?;
            // scheduler.write_value(val2, 1, 1, offset2, &meta, database)?;

            let mut diagnostics =
                timed_commit(&meta, database, TransactionType::ReadWrite, scheduler)?;
            // let mut diag = scheduler.commit(&meta, database, TransactionType::ReadWrite);

            diagnostics.set_write_time((write_dur_1 + write_dur_2) / 2);

            Ok(Success::diagnostics(meta, diagnostics))
        }
        _ => panic!("unexpected database"),
    }
}

fn timed_write<'a>(
    value: &mut Data,
    table_id: usize,
    column_id: usize,
    offset: usize,
    meta: &TransactionId,
    scheduler: &'a Scheduler,
    database: &Database,
) -> Result<u128, NonFatalError> {
    let start = Instant::now();
    scheduler.write_value(value, table_id, column_id, offset, meta, database)?;
    Ok(start.elapsed().as_nanos())
}

fn timed_commit<'a>(
    meta: &TransactionId,
    database: &Database,
    transaction_type: TransactionType,
    scheduler: &'a Scheduler,
) -> Result<ProtocolDiagnostics, NonFatalError> {
    let start = Instant::now();
    let mut diagnostics = scheduler.commit(meta, database, transaction_type)?;
    let duration = start.elapsed().as_nanos();
    diagnostics.set_commit_time(duration);

    Ok(diagnostics)
}
