use crate::common::error::NonFatalError;
use crate::scheduler::Protocol;
use crate::storage::datatype::Data;
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey::*;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::PrimaryKey::*;

use std::convert::TryFrom;
use std::sync::Arc;
use tracing::debug;

/// Balance transaction.
///
/// Sum the balances of a customer's checking and savings accounts.
pub fn balance(params: Balance, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    debug!("balance txn");
    let accounts_cols = ["customer_id"]; // columns
    let other_cols = ["balance"];

    let accounts_pk = SmallBank(Account(params.name));
    let savings_pk = SmallBank(Savings(params.name));
    let checking_pk = SmallBank(Checking(params.name));

    let meta = protocol.begin(); // register

    protocol.read("account_name", &accounts_pk, &accounts_cols, &meta)?; // read 1 -- get customer id

    protocol.read("savings_idx", &savings_pk, &other_cols, &meta)?; // read 2 -- get savings

    protocol.read("checking_idx", &checking_pk, &other_cols, &meta)?; // read 3 -- get checking

    protocol.commit(&meta)?; // commit

    Ok("ok".to_string())
}

/// Deposit checking transaction.
pub fn deposit_checking(
    params: DepositChecking,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    debug!("deposit checking txn");
    let accounts_cols = ["customer_id"];
    let checking_cols = ["balance"];

    let accounts_pk = SmallBank(Account(params.name));
    let checking_pk = SmallBank(Checking(params.name));

    let update_checking =
        |current: Option<Vec<Data>>, params: Option<&[Data]>| -> Result<Vec<Data>, NonFatalError> {
            let balance = f64::try_from(current.unwrap()[0].clone())?; // get current balance
            let value = f64::try_from(params.unwrap()[0].clone())?; // get deposit amount
            let new_balance = vec![Data::from(balance + value)]; // create new balance
            Ok(new_balance)
        };

    let params = vec![Data::Double(params.value)];

    let meta = protocol.begin();

    protocol.read("account_name", &accounts_pk, &accounts_cols, &meta)?;

    protocol.write(
        "checking_idx",
        &checking_pk,
        &checking_cols,
        Some(&checking_cols),
        Some(&params),
        &update_checking,
        &meta,
    )?; // update -- set balance

    protocol.commit(&meta)?;

    Ok("ok".to_string())
}

/// TransactSavings transaction.
///
/// Makes a withdrawal on the savings account.
pub fn transact_savings(
    params: TransactSaving,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    debug!("transact savings txn");
    let accounts_cols = ["customer_id"];
    let savings_cols = ["balance"];

    let accounts_pk = SmallBank(Account(params.name));
    let savings_pk = SmallBank(Savings(params.name));

    let params = vec![Data::Double(params.value)];

    let savings_withdrawal =
        |current: Option<Vec<Data>>, params: Option<&[Data]>| -> Result<Vec<Data>, NonFatalError> {
            let balance = f64::try_from(current.unwrap()[0].clone()).unwrap(); // get current balance
            let value = f64::try_from(params.unwrap()[0].clone()).unwrap(); // get value
            if balance - value > 0.0 {
                Ok(vec![Data::Double(balance - value)])
            } else {
                Err(SmallBankError::InsufficientFunds.into())
            }
        };

    let meta = protocol.begin(); // register

    protocol.read("account_name", &accounts_pk, &accounts_cols, &meta)?; // read -- get customer ID

    protocol.write(
        "savings_idx",
        &savings_pk,
        &savings_cols,
        Some(&savings_cols),
        Some(&params),
        &savings_withdrawal,
        &meta,
    )?; // update -- set savings balance

    protocol.commit(&meta)?;

    Ok("ok".to_string())
}

/// Amalgamate transaction.
///
/// Move all the funds from one customer to another.
pub fn amalgmate(params: Amalgamate, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    debug!("amalgmate");
    let accounts_cols = ["customer_id"]; // columns
    let other_cols = ["balance"];

    let accounts_pk1 = SmallBank(Account(params.name1));
    let savings_pk1 = SmallBank(Savings(params.name1));
    let checking_pk1 = SmallBank(Checking(params.name1));

    let accounts_pk2 = SmallBank(Account(params.name2));
    let checking_pk2 = SmallBank(Checking(params.name2));

    let values = vec![Data::Double(0.0)];

    let update_cust2 =
        |current: Option<Vec<Data>>, params: Option<&[Data]>| -> Result<Vec<Data>, NonFatalError> {
            let balance = f64::try_from(current.unwrap()[0].clone())?; // current balance
            let value = f64::try_from(params.unwrap()[0].clone())?; // increment
            Ok(vec![Data::from(balance + value)])
        };

    let set = |_current: Option<Vec<Data>>,
               params: Option<&[Data]>|
     -> Result<Vec<Data>, NonFatalError> {
        let value = f64::try_from(params.unwrap()[0].clone())?; // set from params
        Ok(vec![Data::from(value)])
    };

    let meta = protocol.begin(); // register

    protocol.read("account_name", &accounts_pk1, &accounts_cols, &meta)?; // read -- cust1

    let res2 = protocol.write(
        "savings_idx",
        &savings_pk1,
        &other_cols,
        Some(&other_cols),
        Some(&values),
        &set,
        &meta,
    )?; // get and set savings -- cust1

    let res3 = protocol.write(
        "checking_idx",
        &checking_pk1,
        &other_cols,
        Some(&other_cols),
        Some(&values),
        &set,
        &meta,
    )?; // get and set checking -- cust1

    let a = f64::try_from(res2.unwrap()[0].clone()).unwrap();
    let b = f64::try_from(res3.unwrap()[0].clone()).unwrap();
    let params: Vec<Data> = vec![Data::Double(a + b)]; // amount to send to cust2

    protocol.read("account_name", &accounts_pk2, &accounts_cols, &meta)?; // read -- cust2

    protocol.write(
        "checking_idx",
        &checking_pk2,
        &other_cols,
        Some(&other_cols),
        Some(&params),
        &update_cust2,
        &meta,
    )?; // update -- cust2

    protocol.commit(&meta)?;

    Ok("ok".to_string())
}

/// Write check transaction.
///
/// Write a check against an account taking funds from checking; applying overdraft charge if needed.
pub fn write_check(params: WriteCheck, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    debug!("write check txn");
    let accounts_cols = ["customer_id"];
    let other_cols = ["balance"];

    let accounts_pk = SmallBank(Account(params.name));
    let savings_pk = SmallBank(Savings(params.name));
    let checking_pk = SmallBank(Checking(params.name));

    let update_checking =
        |current: Option<Vec<Data>>, params: Option<&[Data]>| -> Result<Vec<Data>, NonFatalError> {
            let savings = f64::try_from(params.unwrap()[1].clone())?; // savings balance
            let checking = f64::try_from(current.unwrap()[0].clone())?; // checking balance
            let value = f64::try_from(params.unwrap()[0].clone())?; // amount

            let new_balance;
            if savings + checking < value {
                new_balance = vec![Data::Double(checking - (value + 1.0))]; // overdraft charge
            } else {
                new_balance = vec![Data::Double(checking - value)]; // have funds
            }

            Ok(new_balance)
        };

    let meta = protocol.begin();

    protocol.read("account_name", &accounts_pk, &accounts_cols, &meta)?;

    let res2 = protocol.read("savings_idx", &savings_pk, &other_cols, &meta)?; // get savings balance

    let params = vec![Data::Double(params.value), res2[0].clone()];

    protocol.write(
        "checking_idx",
        &checking_pk,
        &other_cols,
        Some(&other_cols),
        Some(&params),
        &update_checking,
        &meta,
    )?; // update checking balance

    protocol.commit(&meta)?;

    Ok("ok".to_string())
}

/// Send payment transaction.
///
/// Transfer money between accounts; if there is sufficient funds in the checking account.
pub fn send_payment(params: SendPayment, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    debug!("send payment");
    let accounts_cols = ["customer_id"];
    let checking_cols = ["balance"];

    let accounts_pk1 = SmallBank(Account(params.name1));
    let accounts_pk2 = SmallBank(Account(params.name2));
    let checking_pk1 = SmallBank(Checking(params.name1));
    let checking_pk2 = SmallBank(Checking(params.name2));

    let params = vec![Data::Double(params.value)];

    let check_funds =
        |current: Option<Vec<Data>>, params: Option<&[Data]>| -> Result<Vec<Data>, NonFatalError> {
            let current_balance = f64::try_from(current.unwrap()[0].clone())?; // checking balance of cust1
            let value = f64::try_from(params.unwrap()[0].clone())?; // proposed payment amount

            if value < current_balance {
                Ok(vec![Data::Double(current_balance - value)])
            } else {
                Err(SmallBankError::InsufficientFunds.into())
            }
        };

    let increase_balance =
        |current: Option<Vec<Data>>, params: Option<&[Data]>| -> Result<Vec<Data>, NonFatalError> {
            let current_balance = f64::try_from(current.unwrap()[0].clone())?;
            let value = f64::try_from(params.unwrap()[0].clone())?;

            Ok(vec![Data::Double(current_balance + value)])
        };

    let meta = protocol.begin(); // register

    protocol.read("account_name", &accounts_pk1, &accounts_cols, &meta)?; // read -- get customer ID 1

    protocol.read("account_name", &accounts_pk2, &accounts_cols, &meta)?; // read -- get customer ID 2

    protocol.write(
        "checking_idx",
        &checking_pk1,
        &checking_cols,
        Some(&checking_cols),
        Some(&params),
        &check_funds,
        &meta,
    )?;

    protocol.write(
        "checking_idx",
        &checking_pk2,
        &checking_cols,
        Some(&checking_cols),
        Some(&params),
        &increase_balance,
        &meta,
    )?;

    protocol.commit(&meta)?;

    Ok("ok".to_string())
}
