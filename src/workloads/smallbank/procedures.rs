use crate::common::error::NonFatalError;
use crate::scheduler::Protocol;
use crate::storage::datatype::Data;
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::PrimaryKey;

use std::convert::TryFrom;
use std::sync::Arc;

/// Balance transaction.
///
/// Sum the balances of a customer's checking and savings accounts.
pub fn balance(params: Balance, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let accounts_cols = ["customer_id"]; // columns
    let other_cols = ["balance"];

    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));

    let meta = protocol.scheduler.register()?; // register

    protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk,
        &accounts_cols,
        &meta,
    )?; // read 1 -- get customer id

    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(params.name));
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(params.name));

    protocol.scheduler.read(
        "savings",
        Some("savings_idx"),
        &savings_pk,
        &other_cols,
        &meta,
    )?; // read 2 -- get savings

    protocol.scheduler.read(
        "checking",
        Some("checking_idx"),
        &checking_pk,
        &other_cols,
        &meta,
    )?; // read 3 -- get checking

    protocol.scheduler.commit(&meta)?; // commit

    // let savings_balance = f64::try_from(read2[0].clone()).unwrap();
    // let checking_balance = f64::try_from(read3[0].clone()).unwrap();
    // let total_balance = vec![Data::Double(savings_balance + checking_balance)]; // calculate total balance
    // let res_cols = vec!["total_balance"];
    //    let res = datatype::to_result(None, None, None, Some(&res_cols), Some(&total_balance)).unwrap();
    //    let res = datatype::to_result(None, Some(1), None, None, None).unwrap(); // convert

    Ok("ok".to_string())
}

/// Deposit checking transaction.
pub fn deposit_checking(
    params: DepositChecking,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let accounts_cols = ["customer_id"];
    let checking_cols = ["balance"];

    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));

    let update_checking = |columns: &[&str],
                           current: Option<Vec<Data>>,
                           params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let balance = f64::try_from(current.unwrap()[0].clone())?; // get current balance
        let value = f64::try_from(params.unwrap()[0].clone())?; // get deposit amount
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let new_balance = vec![Data::from(balance + value)]; // create new balance
        Ok((new_columns, new_balance))
    };

    let meta = protocol.scheduler.register()?; // register

    protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk,
        &accounts_cols,
        &meta,
    )?; // read -- get customer ID

    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(params.name));
    let params = vec![Data::Double(params.value)];

    protocol.scheduler.update(
        "checking",
        Some("checking_idx"),
        &checking_pk,
        &checking_cols,
        true,
        Some(&params),
        &update_checking,
        &meta,
    )?; // update -- set balance

    protocol.scheduler.commit(&meta)?; // commit

    //    let res = datatype::to_result(None, Some(1), None, None, None).unwrap(); // convert

    Ok("ok".to_string())
}

/// TransactSavings transaction.
///
/// Makes a withdrawal on the savings account.
pub fn transact_savings(
    params: TransactSaving,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let accounts_cols = ["customer_id"];
    let savings_cols = ["balance"];
    let id = params.name;
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    let params = vec![Data::Double(params.value)];
    let savings_withdrawal = |columns: &[&str],
                              current: Option<Vec<Data>>,
                              params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let balance = f64::try_from(current.unwrap()[0].clone()).unwrap(); // get current balance
        let value = f64::try_from(params.unwrap()[0].clone()).unwrap(); // get value
        if balance - value > 0.0 {
            let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
            let new_balance = vec![Data::Double(balance - value)]; // create new balance
            Ok((new_columns, new_balance))
        } else {
            Err(SmallBankError::InsufficientFunds.into())
        }
    };

    let meta = protocol.scheduler.register()?; // register

    protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk,
        &accounts_cols,
        &meta,
    )?; // read -- get customer ID

    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(id));

    protocol.scheduler.update(
        "savings",
        Some("savings_idx"),
        &savings_pk,
        &savings_cols,
        true,
        Some(&params),
        &savings_withdrawal,
        &meta,
    )?; // update -- set savings balance

    protocol.scheduler.commit(&meta)?; // commit

    //    let res = datatype::to_result(None, Some(1), None, None, None).unwrap(); // convert
    Ok("ok".to_string())
}

/// Amalgamate transaction.
///
/// Move all the funds from one customer to another.
pub fn amalgmate(params: Amalgamate, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let accounts_cols = ["customer_id"]; // columns
    let other_cols = ["balance"];

    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name1));

    let meta = protocol.scheduler.register()?; // register

    let res1 = protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk,
        &accounts_cols,
        &meta,
    )?; // read -- get customer1 ID

    let cust_id = u64::try_from(res1[0].clone()).unwrap();
    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id));
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));

    let values = vec![Data::Double(0.0)];

    let res2 = protocol.scheduler.read_and_update(
        "savings",
        Some("savings_idx"),
        &savings_pk,
        &other_cols,
        &values,
        &meta,
    )?; // clone and set savings

    let res3 = protocol.scheduler.read_and_update(
        "checking",
        Some("checking_idx"),
        &checking_pk,
        &other_cols,
        &values,
        &meta,
    )?; // get and set checking

    let a = f64::try_from(res2[0].clone()).unwrap();
    let b = f64::try_from(res3[0].clone()).unwrap();
    let total = a + b; // create balance

    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name2));
    let res1 = protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk,
        &accounts_cols,
        &meta,
    )?; // read -- get customer ID

    let cust_id = u64::try_from(res1[0].clone()).unwrap();

    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let params: Vec<Data> = vec![Data::Double(total)];

    let update = |columns: &[&str],
                  current: Option<Vec<Data>>,
                  params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let balance = f64::try_from(current.unwrap()[0].clone())?; // current balance
        let value = f64::try_from(params.unwrap()[0].clone())?;
        let new_balance = vec![Data::from(balance + value)];
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();

        Ok((new_columns, new_balance))
    };

    protocol.scheduler.update(
        "checking",
        Some("checking_idx"),
        &checking_pk,
        &other_cols,
        true,
        Some(&params),
        &update,
        &meta,
    )?; // update

    protocol.scheduler.commit(&meta)?; // commit

    //    let res = datatype::to_result(None, Some(2), None, None, None).unwrap();

    Ok("res".to_string())
}

/// Write check transaction.
///
/// Write a check against an account taking funds from checking; applying overdraft charge if needed.
pub fn write_check(params: WriteCheck, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let accounts_cols = ["customer_id"];
    let other_cols = ["balance"];

    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));

    let update_checking = |columns: &[&str],
                           current: Option<Vec<Data>>,
                           params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let a = f64::try_from(params.unwrap()[1].clone())?; // savings balance
        let b = f64::try_from(current.unwrap()[0].clone())?; // checking balance

        let value = f64::try_from(params.unwrap()[0].clone())?; // amount

        let new_balance;
        if a + b < value {
            new_balance = vec![Data::Double(b - (value + 1.0))]; // overdraft charge
        } else {
            new_balance = vec![Data::Double(b - value)]; // have funds
        }

        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();

        Ok((new_columns, new_balance))
    };

    let meta = protocol.scheduler.register()?; // register

    let res1 = protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk,
        &accounts_cols,
        &meta,
    )?;

    let cust_id = u64::try_from(res1[0].clone()).unwrap();
    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id));
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));

    let res2 = protocol.scheduler.read(
        "savings",
        Some("savings_idx"),
        &savings_pk,
        &other_cols,
        &meta,
    )?; // get savings balance

    let params = vec![Data::Double(params.value), res2[0].clone()];

    protocol.scheduler.update(
        "checking",
        Some("checking_idx"),
        &checking_pk,
        &other_cols,
        true,
        Some(&params),
        &update_checking,
        &meta,
    )?; // update checking balance

    protocol.scheduler.commit(&meta)?;

    //   let res = datatype::to_result(None, Some(2), None, None, None).unwrap();

    Ok("ok".to_string())
}

/// Send payment transaction.
///
/// Transfer money between accounts; if there is sufficient funds in the checking account.
pub fn send_payment(params: SendPayment, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let accounts_cols = ["customer_id"];
    let checking_cols = ["balance"];

    let accounts_pk1 = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name1));
    let accounts_pk2 = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name2));

    let params = vec![Data::Double(params.value)];

    let check_funds = |columns: &[&str],
                       current: Option<Vec<Data>>,
                       params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let current_balance = f64::try_from(current.unwrap()[0].clone())?; // checking balance of cust1
        let value = f64::try_from(params.unwrap()[0].clone())?; // proposed payment amount

        if value < current_balance {
            let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
            let new_balance: Vec<Data> = vec![Data::Double(current_balance - value)];
            Ok((new_columns, new_balance))
        } else {
            Err(SmallBankError::InsufficientFunds.into())
        }
    };

    let increase_balance = |columns: &[&str],
                            current: Option<Vec<Data>>,
                            params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let current_balance = f64::try_from(current.unwrap()[0].clone())?;
        let value = f64::try_from(params.unwrap()[0].clone())?;

        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let new_balance: Vec<Data> = vec![Data::Double(current_balance + value)];

        Ok((new_columns, new_balance))
    };

    let meta = protocol.scheduler.register()?; // register

    let res1 = protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk1,
        &accounts_cols,
        &meta,
    )?; // read -- get customer ID 1
    let res2 = protocol.scheduler.read(
        "accounts",
        Some("account_name"),
        &accounts_pk2,
        &accounts_cols,
        &meta,
    )?; // read -- get customer ID 2

    let cust_id1 = u64::try_from(res1[0].clone()).unwrap();
    let cust_id2 = u64::try_from(res2[0].clone()).unwrap();
    let checking_pk1 = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id1));
    let checking_pk2 = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id2));

    protocol.scheduler.update(
        "checking",
        Some("checking_idx"),
        &checking_pk1,
        &checking_cols,
        true,
        Some(&params),
        &check_funds,
        &meta,
    )?;

    protocol.scheduler.update(
        "checking",
        Some("checking_idx"),
        &checking_pk2,
        &checking_cols,
        true,
        Some(&params),
        &increase_balance,
        &meta,
    )?;

    protocol.scheduler.commit(&meta)?;

    //  let res = datatype::to_result(None, Some(2), None, None, None).unwrap();

    Ok("ok".to_string())
    //    Ok(res)
}
