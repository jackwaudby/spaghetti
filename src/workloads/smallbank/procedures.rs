use crate::common::error::NonFatalError;
use crate::scheduler::Scheduler;
use crate::storage::datatype::Data;
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey::*;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::Database;
use crate::workloads::PrimaryKey::*;

use std::convert::TryFrom;
use std::sync::Arc;
use tracing::debug;

/// Balance transaction.
///
/// Sum the balances of a customer's checking and savings accounts.
pub fn balance(
    params: Balance,
    scheduler: Arc<Scheduler>,
    database: Arc<Database>,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(db) => {
            debug!("balance txn");

            let accounts_pk = SmallBank(Account(params.name));
            let savings_pk = SmallBank(Savings(params.name));
            let checking_pk = SmallBank(Checking(params.name));

            let meta = scheduler.begin(); // register

            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                accounts_pk.into(),
                &meta,
            )?; // read 1 -- get customer id

            scheduler.read_value(
                Arc::clone(&db.saving.balance),
                Arc::clone(&db.saving.lsns),
                Arc::clone(&db.saving.rw_tables),
                savings_pk.into(),
                &meta,
            )?; // read 2 -- get savings

            scheduler.read_value(
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                checking_pk.into(),
                &meta,
            )?; // read 3 -- get checking

            scheduler.commit(&meta)?; // commit

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Deposit checking transaction
///
/// Increase checking balance by X amount.
pub fn deposit_checking(
    params: DepositChecking,
    scheduler: Arc<Scheduler>,
    database: Arc<Database>,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(db) => {
            debug!("deposit checking txn");
            let offset = params.name as usize;

            let meta = scheduler.begin();

            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                offset,
                &meta,
            )?; // read 1 -- get customer id

            let res = scheduler.read_value(
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset,
                &meta,
            )?; // read 2 -- current balance

            let balance = Data::from(f64::try_from(res)? + params.value); // new balance

            scheduler.write_value(
                &balance,
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset,
                &meta,
            )?; // write 1 -- update balance

            scheduler.commit(&meta)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// TransactSavings transaction.
///
/// TODO: logic as per Durner, but does not make sense.
pub fn transact_savings(
    params: TransactSaving,
    scheduler: Arc<Scheduler>,
    database: Arc<Database>,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(db) => {
            debug!("transact savings txn");
            let offset = params.name as usize;

            let meta = scheduler.begin(); // register

            debug!("read cust id");
            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                offset,
                &meta,
            )?; // read 1 -- get customer id

            debug!("read saving");
            let res = scheduler.read_value(
                Arc::clone(&db.saving.balance),
                Arc::clone(&db.saving.lsns),
                Arc::clone(&db.saving.rw_tables),
                offset,
                &meta,
            )?; // read 2 -- current savings balance

            let balance = f64::try_from(res)? + params.value; // new balance

            if balance < 0.0 {
                scheduler.abort(&meta);
                debug!("no money: {}", balance);
                return Err(SmallBankError::InsufficientFunds.into());
            }

            scheduler.write_value(
                &Data::from(balance),
                Arc::clone(&db.saving.balance),
                Arc::clone(&db.saving.lsns),
                Arc::clone(&db.saving.rw_tables),
                offset,
                &meta,
            )?; // write 1 -- update saving balance

            scheduler.commit(&meta)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Amalgamate transaction.
///
/// Move all the funds from one customer to another.
pub fn amalgmate(
    params: Amalgamate,
    scheduler: Arc<Scheduler>,
    database: Arc<Database>,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(db) => {
            debug!("amalgmate");
            let offset1 = params.name1 as usize;
            let offset2 = params.name2 as usize;

            let meta = scheduler.begin(); // register

            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                offset1,
                &meta,
            )?; // read 1 -- get customer1 id

            let res1 = scheduler.read_value(
                Arc::clone(&db.saving.balance),
                Arc::clone(&db.saving.lsns),
                Arc::clone(&db.saving.rw_tables),
                offset1,
                &meta,
            )?; // read 2 -- current savings balance (customer1)

            let res2 = scheduler.read_value(
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset1,
                &meta,
            )?; // read 3 -- current checking balance (customer1)

            scheduler.write_value(
                &Data::Double(0.0),
                Arc::clone(&db.saving.balance),
                Arc::clone(&db.saving.lsns),
                Arc::clone(&db.saving.rw_tables),
                offset1,
                &meta,
            )?; // write 1 -- update saving balance (customer1)

            scheduler.write_value(
                &Data::Double(0.0),
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset1,
                &meta,
            )?; // write 2 -- update checking balance (customer1)

            let sum = f64::try_from(res1)? + f64::try_from(res2)?; // amount to send

            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                offset2,
                &meta,
            )?; // read 4 -- get customer2 id

            let res3 = scheduler.read_value(
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset2,
                &meta,
            )?; // read 5 -- current checking balance (customer2)

            let bal = sum + f64::try_from(res3)?;

            scheduler.write_value(
                &Data::Double(bal),
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset2,
                &meta,
            )?;

            scheduler.commit(&meta)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Write check transaction.
///
/// Write a check against an account taking funds from checking; applying overdraft charge if needed.
pub fn write_check(
    params: WriteCheck,
    scheduler: Arc<Scheduler>,
    database: Arc<Database>,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(db) => {
            debug!("write check txn");
            let offset = params.name as usize;

            let meta = scheduler.begin();

            debug!("read cust id");
            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                offset,
                &meta,
            )?; // get customer id

            debug!("read savings");

            let savings = f64::try_from(scheduler.read_value(
                Arc::clone(&db.saving.balance),
                Arc::clone(&db.saving.lsns),
                Arc::clone(&db.saving.rw_tables),
                offset,
                &meta,
            )?)?; // get savings balance

            debug!("read checking");
            let checking = f64::try_from(scheduler.read_value(
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset,
                &meta,
            )?)?; // get checking balance

            let total = savings + checking; // total balance
            let mut amount = params.value;

            if total < amount {
                amount += 1.0; // apply overdraft charge
            }

            let new_check = total - amount;

            debug!("write checking");
            scheduler.write_value(
                &Data::Double(new_check),
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset,
                &meta,
            )?; // update checking balance

            scheduler.commit(&meta)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}

/// Send payment transaction.
///
/// Transfer money between accounts; if there is sufficient funds in the checking account.
pub fn send_payment(
    params: SendPayment,
    scheduler: Arc<Scheduler>,
    database: Arc<Database>,
) -> Result<String, NonFatalError> {
    match &*database {
        Database::SmallBank(db) => {
            debug!("send payment");
            let offset1 = params.name1 as usize;
            let offset2 = params.name2 as usize;

            let meta = scheduler.begin(); // register

            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                offset1,
                &meta,
            )?; // get cust1 id

            let mut checking = f64::try_from(scheduler.read_value(
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset1,
                &meta,
            )?)?; // get cust1 checking

            checking -= params.value;

            if checking < 0.0 {
                scheduler.abort(&meta);
                debug!("no money: {}", checking);
                return Err(SmallBankError::InsufficientFunds.into());
            }

            scheduler.write_value(
                &Data::Double(checking),
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset1,
                &meta,
            )?; // update cust1 checking balance

            scheduler.read_value(
                Arc::clone(&db.accounts.customer_id),
                Arc::clone(&db.accounts.lsns),
                Arc::clone(&db.accounts.rw_tables),
                offset2,
                &meta,
            )?; // get cust2 id

            let mut checking = f64::try_from(scheduler.read_value(
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset2,
                &meta,
            )?)?; // get cust2 checking

            checking += params.value;

            scheduler.write_value(
                &Data::Double(checking),
                Arc::clone(&db.checking.balance),
                Arc::clone(&db.checking.lsns),
                Arc::clone(&db.checking.rw_tables),
                offset2,
                &meta,
            )?; // update cust2 checking

            scheduler.commit(&meta)?;

            Ok("ok".to_string())
        }
        _ => panic!("unexpected database"),
    }
}
