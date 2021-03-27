use crate::common::error::NonFatalError;
use crate::server::scheduler::Protocol;
use crate::server::scheduler::Scheduler;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::paramgen::{
    Amalgamate, Balance, DepositChecking, SendPayment, TransactSaving, WriteCheck,
};
use crate::workloads::PrimaryKey;

use std::convert::TryFrom;
use std::sync::Arc;
// use std::thread;

/// Balance transaction.
pub fn balance(params: Balance, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    // Register with scheduler.
    let meta = protocol.scheduler.register()?;

    // Get customer ID from accounts table.
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;
    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    // Columns to get.
    let other_cols: Vec<&str> = vec!["balance"];
    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id));
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));

    let res2 = protocol
        .scheduler
        .read("savings", savings_pk, &other_cols, meta.clone())?;
    let res3 = protocol
        .scheduler
        .read("checking", checking_pk, &other_cols, meta.clone())?;
    let a = match f64::try_from(res2[0].clone()) {
        Ok(bal) => bal,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };
    let b = match f64::try_from(res3[0].clone()) {
        Ok(bal) => bal,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };
    let total = a + b;

    // Commit transaction.
    protocol.scheduler.commit(meta.clone())?;
    let res_cols = vec!["total_balance"];
    let res_vals = vec![Data::Double(total)];

    // Convert to result
    let res = datatype::to_result(&res_cols, &res_vals).unwrap();

    Ok(res)
}

/// Deposit checking transaction.
pub fn deposit_checking(
    params: DepositChecking,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    // Register transaction with scheduler.
    let meta = protocol.scheduler.register()?;

    // Get customer ID from accounts table.
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;
    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    // Update balance in checking table.
    let checking_cols: Vec<String> = vec!["balance".to_string()];
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let params: Vec<Data> = vec![Data::Double(params.value)];
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        // Get current balance.
        let balance = match f64::try_from(current.unwrap()[0].clone()) {
            Ok(balance) => balance,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };
        // Get value.
        let value = match f64::try_from(params[0].clone()) {
            Ok(balance) => balance,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };

        // Create new balance.
        let new_balance = vec![(balance + value).to_string()];
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
    )?;

    // Commit transaction.
    protocol.scheduler.commit(meta.clone())?;

    Ok("{\"updated 1 row.\"}".to_string())
}

/// TransactSavings transaction.
pub fn transact_savings(
    params: TransactSaving,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    // Register transaction with scheduler.
    let meta = protocol.scheduler.register()?;

    // Get customer ID from accounts table.
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;
    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    // 2. Update balance in checking table.
    let savings_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(cust_id));
    let savings_cols: Vec<String> = vec!["balance".to_string()];
    let params = vec![Data::Double(params.value)];
    //  Define update closure.
    // TODO: Add constraint.
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        // Get current balance.
        let balance = match f64::try_from(current.unwrap()[0].clone()) {
            Ok(balance) => balance,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };
        // Get value.
        let value = match f64::try_from(params[0].clone()) {
            Ok(balance) => balance,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };

        // Create new balance.
        let new_balance = vec![(balance - value).to_string()];
        Ok((columns, new_balance))
    };

    // 2v. Execute write operation.
    protocol.scheduler.update(
        "savings",
        savings_pk,
        savings_cols,
        true,
        params,
        &update,
        meta.clone(),
    )?;

    // Commit transaction.
    protocol.scheduler.commit(meta.clone())?;

    Ok("{\"updated 1 row.\"}".to_string())
}

/// Amalgamate transaction.
pub fn amalgmate(params: Amalgamate, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    // Register transaction with scheduler.
    let meta = protocol.scheduler.register()?;

    // Get customer ID from accounts table.
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name1));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;
    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    // Get balance of accounts and zero them.
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
    )?;
    let res3 = protocol.scheduler.read_and_update(
        "checking",
        checking_pk,
        &other_cols,
        &values,
        meta.clone(),
    )?;
    // Get current balance.
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

    let total = a + b;

    // Get customer ID from accounts table.
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name2));
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;
    let cust_id = match i64::try_from(res1[0].clone()) {
        Ok(cust_id) => cust_id as u64,
        Err(e) => {
            protocol.scheduler.abort(meta.clone()).unwrap();
            return Err(e);
        }
    };

    // Update balance in checking table.
    let checking_cols: Vec<String> = vec!["balance".to_string()];
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let params: Vec<Data> = vec![Data::Double(total)];
    let update = |columns: Vec<String>,
                  current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        // Get current balance.
        let balance = match f64::try_from(current.unwrap()[0].clone()) {
            Ok(balance) => balance,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };
        // Get value.
        let value = match f64::try_from(params[0].clone()) {
            Ok(balance) => balance,
            Err(e) => {
                protocol.scheduler.abort(meta.clone()).unwrap();
                return Err(e);
            }
        };

        // Create new balance.
        let new_balance = vec![(balance + value).to_string()];
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
    )?;

    // Commit transaction.
    protocol.scheduler.commit(meta.clone())?;

    Ok("{\"updated 2 rows.\"}".to_string())
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
    use std::sync::Once;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    static LOG: Once = Once::new();

    fn logging(on: bool) {
        if on {
            LOG.call_once(|| {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting default subscriber failed");
            });
        }
    }

    #[test]
    fn transactions_test() {
        logging(false);

        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("Test-smallbank.toml"))
            .unwrap();
        let config = Arc::new(c);

        // Workload with fixed seed.
        let schema = config.get_str("schema").unwrap();
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
            "{total_balance=\"53334\"}"
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
            "{\"updated 1 row.\"}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{total_balance=\"53344\"}"
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
            "{\"updated 1 row.\"}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{total_balance=\"53300.7\"}"
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
            "{total_balance=\"72811\"}"
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
            "{\"updated 2 rows.\"}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust1".to_string()
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{total_balance=\"0\"}"
        );

        assert_eq!(
            balance(
                Balance {
                    name: "cust2".to_string()
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{total_balance=\"126111.7\"}"
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
