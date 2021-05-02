use crate::common::error::NonFatalError;
use crate::common::statistics::{add_commit_time, add_read_time, add_reg_time, add_update_time};
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::smallbank::error::SmallBankError;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::PrimaryKey;

use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Instant;

/// Balance transaction.
///
/// Sum the balances of a customer's checking and savings accounts.
pub fn balance(params: Balance, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let accounts_cols: Vec<&str> = vec!["customer_id"]; // columns
    let other_cols: Vec<&str> = vec!["balance"];

    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));

    let start = Instant::now();
    let meta = protocol.scheduler.register()?; // register
    let end = start.elapsed();
    add_reg_time(end.as_nanos());

    let start = Instant::now();
    let read1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?; // read 1 -- get customer ID
    let end = start.elapsed();
    add_read_time(end.as_nanos());

    let cust_id = match i64::try_from(read1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    }; // convert to u64

    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id)); // keys
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let start = Instant::now();

    let read2 = protocol
        .scheduler
        .read("savings", savings_pk, &other_cols, meta.clone())?; // read 2 -- get savings
    let end = start.elapsed();
    add_read_time(end.as_nanos());

    let start = Instant::now();
    let read3 = protocol
        .scheduler
        .read("checking", checking_pk, &other_cols, meta.clone())?; // read 3 -- get checking
    let end = start.elapsed();
    add_read_time(end.as_nanos());

    let savings_balance = match f64::try_from(read2[0].clone()) {
        Ok(bal) => bal,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    }; // convert to u64

    let checking_balance = match f64::try_from(read3[0].clone()) {
        Ok(bal) => bal,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    }; // convert to u64
    let start = std::time::Instant::now(); // start timer
    protocol.scheduler.commit(meta.clone())?; // commit
    let end = start.elapsed();
    add_commit_time(end.as_nanos());

    let res_cols = vec!["total_balance"];
    let total_balance = vec![Data::Double(savings_balance + checking_balance)]; // calculate total balance
    let res = datatype::to_result(None, None, None, Some(&res_cols), Some(&total_balance)).unwrap(); // convert

    Ok(res)
}

/// Deposit checking transaction.
pub fn deposit_checking(
    params: DepositChecking,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let start = Instant::now();
    let meta = protocol.scheduler.register()?; // register
    let end = start.elapsed();
    add_reg_time(end.as_nanos());

    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?; // read -- get customer ID
    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    let checking_cols: Vec<String> = vec!["balance".to_string()];
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let params: Vec<Data> = vec![Data::Double(params.value)];
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let balance = f64::try_from(current.unwrap()[0].clone())?; // get current balance
        let value = f64::try_from(params[0].clone())?; // get deposit amount
        let new_balance = vec![(balance + value).to_string()]; // create new balance
        Ok((columns, new_balance))
    };
    let start = Instant::now(); // start timer
    protocol.scheduler.update(
        "checking",
        checking_pk,
        checking_cols,
        true,
        params,
        &update,
        meta.clone(),
    )?; // update -- set balance
    let end = start.elapsed();
    add_update_time(end.as_nanos());

    let start = std::time::Instant::now(); // start timer
    protocol.scheduler.commit(meta.clone())?; // commit
    let end = start.elapsed();
    add_commit_time(end.as_nanos());

    let res = datatype::to_result(None, Some(1), None, None, None).unwrap(); // convert

    Ok(res)
}

/// TransactSavings transaction.
///
/// Makes a withdrawal on the savings account.
pub fn transact_savings(
    params: TransactSaving,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));

    let start = Instant::now();
    let meta = protocol.scheduler.register()?; // register
    let end = start.elapsed();
    add_reg_time(end.as_nanos());

    let start = Instant::now();
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?; // read -- get customer ID
    let end = start.elapsed();
    add_read_time(end.as_nanos());

    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    }; // convert to u64

    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id));
    let savings_cols: Vec<String> = vec!["balance".to_string()];
    let params = vec![Data::Double(params.value)];
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let balance = f64::try_from(current.unwrap()[0].clone())?; // get current balance
        let value = f64::try_from(params[0].clone())?; // get value
        if balance - value > 0.0 {
            let new_balance = vec![(balance - value).to_string()]; // create new balance
            Ok((columns, new_balance))
        } else {
            Err(SmallBankError::InsufficientFunds.into())
        }
    };

    let start = Instant::now();
    protocol.scheduler.update(
        "savings",
        savings_pk,
        savings_cols,
        true,
        params,
        &update,
        meta.clone(),
    )?; // update -- set savings balance
    let end = start.elapsed();
    add_update_time(end.as_nanos());

    let start = std::time::Instant::now(); // start timer
    protocol.scheduler.commit(meta.clone())?; // commit
    let end = start.elapsed();
    add_commit_time(end.as_nanos());

    let res = datatype::to_result(None, Some(1), None, None, None).unwrap(); // convert

    Ok(res)
}

/// Amalgamate transaction.
///
/// Move all the funds from one customer to another.
pub fn amalgmate(params: Amalgamate, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let start = Instant::now();
    let meta = protocol.scheduler.register()?; // register
    let end = start.elapsed();
    add_reg_time(end.as_nanos());

    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name1));

    let start = Instant::now();
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?; // read -- get customer1 ID
    let end = start.elapsed();
    add_read_time(end.as_nanos());

    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    let other_cols: Vec<&str> = vec!["balance"];
    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id));
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let values = vec!["0".to_string()];
    let values: Vec<&str> = values.iter().map(|s| s as &str).collect();
    let res2 = protocol.scheduler.read_and_update(
        "savings",
        savings_pk,
        &other_cols,
        &values,
        meta.clone(),
    )?; // get and set savings
    let res3 = protocol.scheduler.read_and_update(
        "checking",
        checking_pk,
        &other_cols,
        &values,
        meta.clone(),
    )?; // get and set checking

    let a = match f64::try_from(res2[0].clone()) {
        Ok(balance) => balance,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    let b = match f64::try_from(res3[0].clone()) {
        Ok(balance) => balance,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    let total = a + b; // create balance

    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name2));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?; // read -- get customer ID
    let end = start.elapsed();
    add_read_time(end.as_nanos());

    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    let checking_cols: Vec<String> = vec!["balance".to_string()];
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let params: Vec<Data> = vec![Data::Double(total)];
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let balance = f64::try_from(current.unwrap()[0].clone())?; // current balance
        let value = f64::try_from(params[0].clone())?;
        let new_balance = vec![(balance + value).to_string()];
        Ok((columns, new_balance))
    };

    let start = Instant::now(); // start timer
    protocol.scheduler.update(
        "checking",
        checking_pk,
        checking_cols,
        true,
        params,
        &update,
        meta.clone(),
    )?; // update
    let end = start.elapsed();
    add_update_time(end.as_nanos());

    let start = Instant::now(); // start timer
    protocol.scheduler.commit(meta.clone())?; // commit
    let end = start.elapsed();
    add_commit_time(end.as_nanos());

    let res = datatype::to_result(None, Some(2), None, None, None).unwrap();

    Ok(res)
}

/// Write check transaction.
///
/// Write a check against an account taking funds from checking; applying overdraft charge if needed.
pub fn write_check(params: WriteCheck, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let start = Instant::now();
    let meta = protocol.scheduler.register()?; // register
    let end = start.elapsed();
    add_reg_time(end.as_nanos());

    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;
    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta).unwrap();
            return Err(e);
        }
    };

    let other_cols: Vec<&str> = vec!["balance"];
    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id));
    let res2 = protocol
        .scheduler
        .read("savings", savings_pk, &other_cols, meta.clone())?; // read

    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let checking_cols: Vec<String> = vec!["balance".to_string()];
    let params = vec![Data::Double(params.value), res2[0].clone()];
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let a = f64::try_from(params[1].clone())?;
        let b = f64::try_from(current.unwrap()[0].clone())?;
        let value = f64::try_from(params[0].clone())?;
        let new_balance;
        if a + b < value {
            new_balance = vec![(b - (value + 1.0)).to_string()]; // overdraft charge
        } else {
            new_balance = vec![(b - value).to_string()];
        }

        Ok((columns, new_balance))
    };

    protocol.scheduler.update(
        "checking",
        checking_pk,
        checking_cols,
        true,
        params,
        &update,
        meta.clone(),
    )?; // update

    let start = std::time::Instant::now(); // start timer
    protocol.scheduler.commit(meta)?; // commit
    let end = start.elapsed();
    add_commit_time(end.as_nanos());

    let res = datatype::to_result(None, Some(2), None, None, None).unwrap();

    Ok(res)
}

/// Send payment transaction.
///
/// Transfer money between accounts; if there is sufficient funds in the checking account.
pub fn send_payment(params: SendPayment, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    let start = Instant::now();
    let meta = protocol.scheduler.register()?; // register
    let end = start.elapsed();
    add_reg_time(end.as_nanos());

    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name1));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?; // read -- get customer ID 1
    let cust_id1 = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta).unwrap();
            return Err(e);
        }
    };

    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name2));
    let res2 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?; // read -- get customer ID 2
    let cust_id2 = match i64::try_from(res2[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta).unwrap();
            return Err(e);
        }
    };

    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id1));
    let checking_cols: Vec<String> = vec!["balance".to_string()];
    let params = vec![Data::Double(params.value)];

    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let current_balance = f64::try_from(current.unwrap()[0].clone())?;
        let value = f64::try_from(params[0].clone())?;

        if value < current_balance {
            let new_balance = vec![(current_balance - value).to_string()];
            Ok((columns, new_balance))
        } else {
            Err(SmallBankError::InsufficientFunds.into())
        }
    };

    protocol.scheduler.update(
        "checking",
        checking_pk,
        checking_cols,
        true,
        params.clone(),
        &update,
        meta.clone(),
    )?; // update

    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id2));
    let checking_cols: Vec<String> = vec!["balance".to_string()];
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let current_balance = f64::try_from(current.unwrap()[0].clone())?;
        let value = f64::try_from(params[0].clone())?;
        let new_balance = vec![(current_balance + value).to_string()];
        Ok((columns, new_balance))
    };

    protocol.scheduler.update(
        "checking",
        checking_pk,
        checking_cols,
        true,
        params,
        &update,
        meta.clone(),
    )?; // update

    let start = std::time::Instant::now(); // start timer
    protocol.scheduler.commit(meta.clone())?; // commit
    let end = start.elapsed();
    add_commit_time(end.as_nanos());

    let res = datatype::to_result(None, Some(2), None, None, None).unwrap();

    Ok(res)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::workloads::smallbank::loader;
    use crate::workloads::{Internal, Workload};
    use config::Config;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::convert::TryInto;
    use test_env_log::test;

    #[test]
    fn transactions_test() {
        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-smallbank.toml"))
            .unwrap();
        let config = Arc::new(c);

        // Workload with fixed seed.
        let schema = "./schema/smallbank_schema.txt".to_string();
        let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
        let seed = config.get_int("seed").unwrap();
        let mut rng = StdRng::seed_from_u64(seed.try_into().unwrap());
        loader::populate_tables(&internals, &mut rng).unwrap();
        let workload = Arc::new(Workload::Tatp(internals));

        // Scheduler.
        let workers = config.get_int("workers").unwrap();
        let protocol = Arc::new(Protocol::new(Arc::clone(&workload), workers as usize).unwrap());

        //////////////////////
        //// Balance ////
        //////////////////////
        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"49368358\"}}"
        );

        ////////////////////////////////////
        //// Deposit Checking ////
        ////////////////////////////////////

        assert_eq!(
            deposit_checking(
                DepositChecking {
                    name: "cust1".to_string(),
                    value: 10.0
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":1,\"deleted\":null,\"val\":null}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),

        "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"49368368\"}}"
        );

        ////////////////////////////////////
        //// Transact Saving ////
        ////////////////////////////////////

        assert_eq!(
            transact_savings(
                TransactSaving {
                    name: "cust1".to_string(),
                    value: 43.3
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":1,\"deleted\":null,\"val\":null}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"49368324.7\"}}"

        );

        /////////////////////////////
        //// Amalgamate ////
        /////////////////////////////

        assert_eq!(
                balance(
                    Balance {
                        name: "cust2".to_string()
                    },
                    Arc::clone(&protocol)
                )
                    .unwrap(),
        "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"51361298\"}}"

            );

        assert_eq!(
            amalgmate(
                Amalgamate {
                    name1: "cust1".to_string(),
                    name2: "cust2".to_string(),
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":2,\"deleted\":null,\"val\":null}"
        );

        assert_eq!(
                balance(
                    Balance {
                        name: "cust1".to_string()
                    },
                    Arc::clone(&protocol)
                )
                    .unwrap(),
        "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"0\"}}"


            );

        assert_eq!(
            balance(
                Balance {
                    name: "cust2".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
                    "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"100729622.7\"}}"


        );

        /////////////////////////////
        //// Write Check /////
        /////////////////////////////
        assert_eq!(
            write_check(
                WriteCheck {
                    name: "cust1".to_string(),
                    value: 50.0
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":2,\"deleted\":null,\"val\":null}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
                                "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"-51\"}}"


        );

        assert_eq!(
            write_check(
                WriteCheck {
                    name: "cust2".to_string(),
                    value: 11.7
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":2,\"deleted\":null,\"val\":null}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust2".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"100729611\"}}"
        );

        /////////////////////////////////
        //// Send Payment /////
        /////////////////////////////////
        assert_eq!(
            send_payment(
                SendPayment {
                    name1: "cust2".to_string(),
                    name2: "cust1".to_string(),
                    value: 50.0
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":2,\"deleted\":null,\"val\":null}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
                                            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"-1\"}}"


        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust2".to_string()
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
                                            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"total_balance\":\"100729561\"}}"


        );
    }
}
//         ///////////////////////////////////////
//         //// GetNewDestination ////
//         ///////////////////////////////////////
//         assert_eq!(
//             get_new_destination(
//                 GetNewDestination {
//                     s_id: 1,
//                     sf_type: 4,
//                     start_time: 16,
//                     end_time: 12,
//                 },
//                 Arc::clone(&protocol)
//             )
//             .unwrap(),
//             "{number_x=\"655601632274699\"}"
//         );
//         assert_eq!(
//             format!(
//                 "{}",
//                 get_new_destination(
//                     GetNewDestination {
//                         s_id: 10,
//                         sf_type: 1,
//                         start_time: 0,
//                         end_time: 1,
//                     },
//                     Arc::clone(&protocol)
//                 )
//                 .unwrap_err()
//             ),
//             format!("not found: SpecialFacility(10, 1) in special_idx")
//         );

//         //////////////////////////////////
//         //// GetAccessData ////
//         /////////////////////////////////
//         assert_eq!(
//             get_access_data(
//                 GetAccessData {
//                     s_id: 1,
//                     ai_type: 1
//                 },
//                 Arc::clone(&protocol)
//             )
//             .unwrap(),
//             "{data_1=\"57\", data_2=\"200\", data_3=\"IEU\", data_4=\"WIDHY\"}"
//         );

//         assert_eq!(
//             format!(
//                 "{}",
//                 get_access_data(
//                     GetAccessData {
//                         s_id: 19,
//                         ai_type: 12
//                     },
//                     Arc::clone(&protocol)
//                 )
//                 .unwrap_err()
//             ),
//             format!("not found: AccessInfo(19, 12) in access_idx")
//         );

//         ////////////////////////////////////////////
//         //// UpdateSubscriberData ////
//         ///////////////////////////////////////////

//         let columns_sb = vec!["bit_1"];
//         let columns_sf = vec!["data_a"];

//         // Before
//         let values_sb = workload
//             .get_internals()
//             .get_index("sub_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
//                 &columns_sb,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();
//         let values_sf = workload
//             .get_internals()
//             .get_index("special_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 1)),
//                 &columns_sf,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();

//         let res_sb = datatype::to_result(&columns_sb, &values_sb.get_values().unwrap()).unwrap();
//         let res_sf = datatype::to_result(&columns_sf, &values_sf.get_values().unwrap()).unwrap();
//         assert_eq!(res_sb, "{bit_1=\"0\"}");
//         assert_eq!(res_sf, "{data_a=\"60\"}");

//         assert_eq!(
//             update_subscriber_data(
//                 UpdateSubscriberData {
//                     s_id: 1,
//                     sf_type: 1,
//                     bit_1: 1,
//                     data_a: 29,
//                 },
//                 Arc::clone(&protocol)
//             )
//             .unwrap(),
//             "{\"updated 2 rows.\"}"
//         );

//         // After
//         let values_sb = workload
//             .get_internals()
//             .get_index("sub_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
//                 &columns_sb,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();
//         let values_sf = workload
//             .get_internals()
//             .get_index("special_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 1)),
//                 &columns_sf,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();

//         let res_sb = datatype::to_result(&columns_sb, &values_sb.get_values().unwrap()).unwrap();
//         let res_sf = datatype::to_result(&columns_sf, &values_sf.get_values().unwrap()).unwrap();
//         assert_eq!(res_sb, "{bit_1=\"1\"}");
//         assert_eq!(res_sf, "{data_a=\"29\"}");

//         assert_eq!(
//             format!(
//                 "{}",
//                 update_subscriber_data(
//                     UpdateSubscriberData {
//                         s_id: 1345,
//                         sf_type: 132,
//                         bit_1: 0,
//                         data_a: 28,
//                     },
//                     Arc::clone(&protocol)
//                 )
//                 .unwrap_err()
//             ),
//             format!("not found: Subscriber(1345) in sub_idx")
//         );

//         ////////////////////////////////
//         //// UpdateLocation ////
//         /////////////////////////////////

//         let columns_sb = vec!["vlr_location"];

//         // Before
//         let values_sb = workload
//             .get_internals()
//             .get_index("sub_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
//                 &columns_sb,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();
//         let res_sb = datatype::to_result(&columns_sb, &values_sb.get_values().unwrap()).unwrap();
//         assert_eq!(res_sb, "{vlr_location=\"12\"}");

//         assert_eq!(
//             update_location(
//                 UpdateLocationData {
//                     s_id: 1,
//                     vlr_location: 4
//                 },
//                 Arc::clone(&protocol)
//             )
//             .unwrap(),
//             "{\"updated 1 row.\"}"
//         );

//         // After
//         let values_sb = workload
//             .get_internals()
//             .get_index("sub_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
//                 &columns_sb,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();

//         let res_sb = datatype::to_result(&columns_sb, &values_sb.get_values().unwrap()).unwrap();
//         assert_eq!(res_sb, "{vlr_location=\"4\"}");

//         assert_eq!(
//             format!(
//                 "{}",
//                 update_location(
//                     UpdateLocationData {
//                         s_id: 1345,
//                         vlr_location: 7,
//                     },
//                     Arc::clone(&protocol)
//                 )
//                 .unwrap_err()
//             ),
//             format!("not found: Subscriber(1345) in sub_idx")
//         );

//         /////////////////////////////////////////
//         //// InsertCallForwarding ////
//         ////////////////////////////////////////
//         let columns_cf = vec!["number_x"];
//         assert_eq!(
//             format!(
//                 "{}",
//                 workload
//                     .get_internals()
//                     .get_index("call_idx")
//                     .unwrap()
//                     .read(
//                         PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 0)),
//                         &columns_cf,
//                         "2pl",
//                         "t1",
//                     )
//                     .unwrap_err()
//             ),
//             format!("not found: CallForwarding(1, 3, 0) in call_idx")
//         );

//         assert_eq!(
//             insert_call_forwarding(
//                 InsertCallForwarding {
//                     s_id: 1,
//                     sf_type: 3,
//                     start_time: 0,
//                     end_time: 19,
//                     number_x: "551795089196026".to_string()
//                 },
//                 Arc::clone(&protocol)
//             )
//             .unwrap(),
//             "{\"inserted 1 row into call_forwarding.\"}"
//         );

//         let values_cf = workload
//             .get_internals()
//             .get_index("call_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 0)),
//                 &columns_cf,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();
//         let res_cf = datatype::to_result(&columns_cf, &values_cf.get_values().unwrap()).unwrap();

//         assert_eq!(res_cf, "{number_x=\"551795089196026\"}");

//         //////////////////////////////////////////
//         //// DeleteCallForwarding ////
//         /////////////////////////////////////////

//         let columns_cf = vec!["number_x"];

//         let values_cf = workload
//             .get_internals()
//             .get_index("call_idx")
//             .unwrap()
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 0)),
//                 &columns_cf,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap();
//         let res_cf = datatype::to_result(&columns_cf, &values_cf.get_values().unwrap()).unwrap();

//         assert_eq!(res_cf, "{number_x=\"551795089196026\"}");

//         assert_eq!(
//             delete_call_forwarding(
//                 DeleteCallForwarding {
//                     s_id: 2,
//                     sf_type: 2,
//                     start_time: 16,
//                 },
//                 Arc::clone(&protocol)
//             )
//             .unwrap(),
//             "{\"deleted 1 row from call_forwarding.\"}"
//         );

//         assert_eq!(
//             format!(
//                 "{}",
//                 workload
//                     .get_internals()
//                     .get_index("call_idx")
//                     .unwrap()
//                     .read(
//                         PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(2, 2, 16)),
//                         &columns_cf,
//                         "2pl",
//                         "t1",
//                     )
//                     .unwrap_err()
//             ),
//             format!("not found: CallForwarding(2, 2, 16) in call_idx")
//         );
//     }
// }
