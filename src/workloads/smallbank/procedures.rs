use crate::common::error::NonFatalError;
use crate::common::stats_bucket::StatsBucket;
use crate::common::value_id::ValueId;
use crate::scheduler::Scheduler;
use crate::storage::{datatype::Data, Database};
use crate::workloads::smallbank::{
    error::SmallBankError,
    paramgen::{Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck},
};

use std::convert::TryFrom;

/// Balance transaction.
///
/// Sum the balances of a customer's checking and savings accounts.
pub fn balance<'a>(
    meta: &mut StatsBucket,
    params: Balance,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset = params.get_name();

    // get customer id
    let cust = ValueId::new(0, 0, offset);
    scheduler.read_value(cust, meta, database)?;

    // get checking balance
    let checking = ValueId::new(1, 1, offset);
    scheduler.read_value(checking, meta, database)?;

    // get savings balance
    let savings = ValueId::new(2, 1, offset);
    scheduler.read_value(savings, meta, database)?;

    Ok(())
}

/// Deposit checking transaction
///
/// Increase checking balance by X amount.
pub fn deposit_checking<'a>(
    meta: &mut StatsBucket,
    params: DepositChecking,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset = params.get_name();

    // get customer id
    let cust = ValueId::new(0, 0, offset);
    scheduler.read_value(cust, meta, database)?;

    // get current balance
    let bal = ValueId::new(1, 1, offset);
    let current_balance = scheduler.read_value(bal.clone(), meta, database)?;

    // update checking balance
    let mut new_val = Data::from(f64::try_from(current_balance).unwrap() + params.get_value());
    scheduler.write_value(&mut new_val, bal, meta, database)?;

    Ok(())
}

/// TransactSavings transaction.
///
/// TODO: logic as per Durner, but does not make sense.
pub fn transact_savings<'a>(
    meta: &mut StatsBucket,
    params: TransactSaving,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset = params.name;

    // get customer id
    let c_vid = ValueId::new(0, 0, offset);
    scheduler.read_value(c_vid, meta, database)?;

    // get savings balance
    let s_vid = ValueId::new(2, 1, offset);
    let saving = scheduler.read_value(s_vid.clone(), meta, database)?;

    // abort if balance would be negative
    let new_saving = f64::try_from(saving).unwrap() + params.value;
    if new_saving < 0.0 {
        // scheduler.abort(meta, database);
        return Err(SmallBankError::InsufficientFunds.into());
    }

    //  update saving balance
    let val = &mut Data::from(new_saving);
    scheduler.write_value(val, s_vid, meta, database)?;

    Ok(())
}

/// Amalgamate transaction.
///
/// Move all the funds from one customer to another.
pub fn amalgmate<'a>(
    meta: &mut StatsBucket,
    params: Amalgamate,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset1 = params.name1;
    let offset2 = params.name2;

    // cust1
    let c_vid1 = ValueId::new(0, 0, offset1);
    let ch_vid1 = ValueId::new(1, 1, offset1);
    let s_vid1 = ValueId::new(2, 1, offset1);

    // read 1: get customer 1 id
    scheduler.read_value(c_vid1, meta, database)?;

    // read 2: get customer 1 savings balance
    let res1 = scheduler.read_value(s_vid1, meta, database)?;

    // write 1: set customer 1 saving balance to 0
    let val = &mut Data::Double(0.0);
    scheduler.write_value(val, s_vid1, meta, database)?;

    // read 3: get customer 2 checking balance
    let res2 = scheduler.read_value(ch_vid1, meta, database)?;

    // write 2: set customer 1 checking balance to 0
    scheduler.write_value(val, ch_vid1, meta, database)?;

    // amount to send
    let sum = f64::try_from(res1).unwrap() + f64::try_from(res2).unwrap();

    let c_vid2 = ValueId::new(0, 0, offset2);
    let ch_vid2 = ValueId::new(1, 1, offset2);

    // read 4: get customer 2 id
    scheduler.read_value(c_vid2, meta, database)?;

    // read 5 -- current checking balance (customer2)
    let res3 = scheduler.read_value(ch_vid2, meta, database)?;

    // write 3 -- update checking balance (cust2)
    let bal = &mut Data::Double(sum + f64::try_from(res3).unwrap());
    scheduler.write_value(bal, ch_vid2, meta, database)?;

    Ok(())
}

/// Write check transaction.
///
/// Write a check against an account taking funds from checking; applying overdraft charge if needed.
pub fn write_check<'a>(
    meta: &mut StatsBucket,
    params: WriteCheck,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset = params.name as usize;

    // get customer id
    let c_vid = ValueId::new(0, 0, offset);
    scheduler.read_value(c_vid, meta, database)?;

    // get savings balance
    let s_vid = ValueId::new(2, 1, offset);
    let bal1 = scheduler.read_value(s_vid, meta, database)?;

    // get checking balance
    let ch_vid = ValueId::new(1, 1, offset);
    let bal2 = scheduler.read_value(ch_vid, meta, database)?;

    // apply overdraft charge
    let bal1 = f64::try_from(bal1).unwrap();
    let bal2 = f64::try_from(bal2).unwrap();

    let total = bal1 + bal2; // total balance
    let mut amount = params.value;
    if total < amount {
        amount += 1.0;
    }

    // update checking balance
    let new_check = &mut Data::Double(total - amount);
    scheduler.write_value(new_check, ch_vid, meta, database)?;

    Ok(())
}

/// Send payment transaction.
///
/// Transfer money between accounts; if there is sufficient funds in the checking account.
pub fn send_payment<'a>(
    meta: &mut StatsBucket,
    params: SendPayment,
    scheduler: &'a Scheduler,
    database: &'a Database,
) -> Result<(), NonFatalError> {
    let offset1 = params.name1;
    let offset2 = params.name2;

    // get cust1 id
    let c_vid = ValueId::new(0, 0, offset1);
    scheduler.read_value(c_vid, meta, database)?;

    // get cust1 checking
    let checking1 = ValueId::new(1, 1, offset1);
    let bal1 = scheduler.read_value(checking1.clone(), meta, database)?;

    // if balance would be negative then abort
    let mut bal1 = f64::try_from(bal1).unwrap();
    bal1 -= params.value;
    if bal1 < 0.0 {
        // scheduler.abort(meta, database);
        return Err(SmallBankError::InsufficientFunds.into());
    }

    // update value cust1 checking balance to new balance
    let val1 = &mut Data::Double(bal1);
    scheduler.write_value(val1, checking1, meta, database)?;

    // get cust2 id
    let cust2 = ValueId::new(0, 0, offset2);
    scheduler.read_value(cust2, meta, database)?;

    // get cust2 checking
    let checking2 = ValueId::new(1, 1, offset2);
    let bal2 = scheduler.read_value(checking2.clone(), meta, database)?;

    let mut bal2 = f64::try_from(bal2).unwrap();
    bal2 += params.value;

    // update cust2 checking balance
    let val2 = &mut Data::Double(bal2);
    scheduler.write_value(val2, checking2, meta, database)?;

    Ok(())
}
