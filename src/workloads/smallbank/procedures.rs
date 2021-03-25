use crate::common::error::NonFatalError;
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::paramgen::{Balance, DepositChecking};
use crate::workloads::PrimaryKey;

use std::sync::Arc;
// use std::thread;
use tracing::debug;

/// Balance transaction.
pub fn balance(params: Balance, protocol: Arc<Protocol>) -> Result<String, NonFatalError> {
    // Columns to get.
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    // Construct primary key.
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    // Register with scheduler.
    let meta = protocol.scheduler.register()?;
    // Get customer id
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;

    let cust_id = if let Data::Int(cust_id) = res1[0] {
        cust_id as u64
    } else {
        panic!("unexpected type");
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

    let a = if let Data::Double(balance) = res2[0] {
        balance
    } else {
        panic!("unexpected type");
    };

    let b = if let Data::Double(balance) = res3[0] {
        balance
    } else {
        panic!("unexpected type");
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

// /// GetNewDestination transaction.
// pub fn get_new_destination(
//     params: GetNewDestination,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     // debug!(
//     //     "\nSELECT cf.numberx
//     //        FROM Special_Facility AS sf, Call_Forwarding AS cf
//     //        WHERE
//     //          (sf.s_id = {} AND sf.sf_type = {} AND sf.is_active = 1)
//     //          AND
//     //          (cf.s_id = {} AND cf.sf_type = {})
//     //          AND
//     //         (cf.start_time <= {} AND  {} < cf.end_time);",
//     //     params.s_id,
//     //     params.sf_type,
//     //     params.s_id,
//     //     params.sf_type,
//     //     params.start_time,
//     //     params.end_time
//     // );

//     // Columns to read.
//     let sf_columns: Vec<&str> = vec!["s_id", "sf_type", "is_active"];
//     let cf_columns: Vec<&str> = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
//     // Construct PKs.
//     let sf_pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
//         params.s_id.into(),
//         params.sf_type.into(),
//     ));
//     let cf_pk = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
//         params.s_id.into(),
//         params.sf_type.into(),
//         params.start_time.into(),
//     ));
//     // Register with scheduler.
//     let meta = protocol.scheduler.register().unwrap();
//     // Execute read operations.
//     // 1) Attempt to get the special facility record.
//     let sf_res = protocol
//         .scheduler
//         .read("special_facility", sf_pk, &sf_columns, meta.clone())?;
//     // 2) Check sf.is_active = 1.
//     let val = if let Data::Int(val) = sf_res[2] {
//         val
//     } else {
//         panic!("Unexpected type")
//     };
//     if val != 1 {
//         protocol.scheduler.abort(meta.clone()).unwrap();
//         return Err(NonFatalError::RowNotFound(
//             format!("{}", sf_pk),
//             "special_facility".to_string(),
//         ));
//     }
//     // 3) Get call forwarding record.
//     let cf_res = protocol
//         .scheduler
//         .read("call_forwarding", cf_pk, &cf_columns, meta.clone())?;
//     // 4) Check end_time < cf.end_time
//     let val = if let Data::Int(val) = cf_res[3] {
//         val
//     } else {
//         panic!("Unexpected type")
//     };
//     if params.end_time as i64 >= val {
//         protocol.scheduler.abort(meta.clone()).unwrap();
//         return Err(NonFatalError::RowNotFound(
//             format!("{}", cf_pk),
//             "call_forwarding".to_string(),
//         ));
//     }
//     // Commit transaction.
//     protocol.scheduler.commit(meta.clone())?;
//     // Convert to result
//     let res = datatype::to_result(&vec![cf_columns[4].clone()], &vec![cf_res[4].clone()]).unwrap();
//     Ok(res)
// }

// /// GetAccessData transaction.
// pub fn get_access_data(
//     params: GetAccessData,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     // debug!(
//     //     "SELECT data1, data2, data3, data4
//     //        FROM Access_Info
//     //      WHERE s_id = {:?}
//     //        AND ai_type = {:?} ",
//     //     params.s_id, params.ai_type
//     // );

//     // Columns to read.
//     let columns: Vec<&str> = vec!["data_1", "data_2", "data_3", "data_4"];
//     // Construct primary key.
//     let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(
//         params.s_id,
//         params.ai_type.into(),
//     ));

//     let handle = thread::current();
//     debug!("Thread {}: register", handle.name().unwrap());
//     // Register with scheduler.
//     let meta = protocol.scheduler.register().unwrap();
//     // Execute read operation.
//     debug!("Thread {}: read", handle.name().unwrap());
//     let values = protocol
//         .scheduler
//         .read("access_info", pk, &columns, meta.clone())?;
//     debug!("Thread {}: commit", handle.name().unwrap());
//     // Commit transaction.
//     protocol.scheduler.commit(meta.clone())?;
//     // Convert to result
//     let res = datatype::to_result(&columns, &values).unwrap();

//     Ok(res)
// }

/// Deposit checking transaction.
pub fn deposit_checking(
    params: DepositChecking,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    //// Get customer id.
    let accounts_cols: Vec<&str> = vec!["customer_id"];
    let accounts_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(params.name));
    let meta = protocol.scheduler.register()?;
    let res1 = protocol
        .scheduler
        .read("accounts", accounts_pk, &accounts_cols, meta.clone())?;
    let cust_id = if let Data::Int(cust_id) = res1[0] {
        cust_id as u64
    } else {
        panic!("unexpected type");
    };

    //// Get checking balance.
    let checking_cols: Vec<&str> = vec!["balance"];
    let checking_pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(cust_id));
    let res2 = protocol.scheduler.read(
        "checking",
        checking_pk.clone(),
        &checking_cols,
        meta.clone(),
    )?;
    let balance = if let Data::Double(bal) = res2[0] {
        bal
    } else {
        panic!("unexpected type");
    };

    /// Update balance.
    let new_balance = vec![(balance + params.value).to_string()];
    let values: Vec<&str> = new_balance.iter().map(|s| s as &str).collect();

    // Execute write operation.
    protocol.scheduler.update(
        "checking",
        checking_pk.clone(),
        &checking_cols,
        &values,
        meta.clone(),
    )?;

    // Commit transaction.
    protocol.scheduler.commit(meta.clone())?;

    Ok("{\"updated 1 row.\"}".to_string())
}

// /// Update location transaction.
// pub fn update_location(
//     params: UpdateLocationData,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     // debug!(
//     //     "UPDATE Subscriber
//     //          SET vlr_location = {}
//     //          WHERE sub_nbr = {};",
//     //     helper::to_sub_nbr(params.s_id.into()),
//     //     params.vlr_location
//     // );

//     // Columns to write.
//     let columns_sb: Vec<&str> = vec!["vlr_location"];
//     // Values to write.
//     let values_sb = vec![params.vlr_location.to_string()];
//     let values_sb: Vec<&str> = values_sb.iter().map(|s| s as &str).collect();

//     // Construct primary key.
//     let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));

//     // Register with scheduler.
//     let meta = protocol.scheduler.register()?;

//     // Execute write operation.
//     protocol
//         .scheduler
//         .update("subscriber", pk_sb, &columns_sb, &values_sb, meta.clone())?;

//     // Commit transaction.
//     protocol.scheduler.commit(meta.clone())?;

//     Ok("{\"updated 1 row.\"}".to_string())
// }

// /// Insert call forwarding transaction.
// pub fn insert_call_forwarding(
//     params: InsertCallForwarding,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     // debug!(
//     //     "SELECT <s_id bind subid s_id>
//     //        FROM Subscriber
//     //        WHERE sub_nbr = {};
//     //      SELECT <sf_type bind sfid sf_type>
//     //        FROM Special_Facility
//     //        WHERE s_id = {}:
//     //      INSERT INTO Call_Forwarding
//     //        VALUES ({}, {}, {}, {}, {});",
//     //     helper::to_sub_nbr(params.s_id.into()),
//     //     params.s_id,
//     //     params.s_id,
//     //     params.sf_type,
//     //     params.start_time,
//     //     params.end_time,
//     //     params.number_x
//     // );

//     // Construct primary keys.
//     let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
//     let pk_sf = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
//         params.s_id,
//         params.sf_type.into(),
//     ));

//     // Register with scheduler.
//     let meta = protocol.scheduler.register().unwrap();
//     // Get record from subscriber table.
//     let columns_sb: Vec<&str> = vec!["s_id"];
//     protocol
//         .scheduler
//         .read("subscriber", pk_sb, &columns_sb, meta.clone())?;
//     // Get record from special facility.
//     let columns_sf: Vec<&str> = vec!["sf_type"];
//     protocol
//         .scheduler
//         .read("special_facility", pk_sf, &columns_sf, meta.clone())?;

//     // Insert into call forwarding.
//     // Calculate primary key
//     let pk_cf = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
//         params.s_id,
//         params.sf_type.into(),
//         params.start_time.into(),
//     ));
//     // Table name
//     let cf_name = "call_forwarding";
//     // Columns
//     let columns_cf: Vec<&str> = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
//     // Values
//     let s_id = params.s_id.to_string();
//     let sf_type = params.sf_type.to_string();
//     let start_time = params.start_time.to_string();
//     let end_time = params.end_time.to_string();
//     let number_x = params.number_x.to_string();
//     let values_cf: Vec<&str> = vec![&s_id, &sf_type, &start_time, &end_time, &number_x];

//     // Execute insert operation.
//     protocol
//         .scheduler
//         .create(cf_name, pk_cf, &columns_cf, &values_cf, meta.clone())?;

//     // Commit transaction.
//     protocol.scheduler.commit(meta.clone())?;

//     Ok("{\"inserted 1 row into call_forwarding.\"}".to_string())
// }

// /// Delete call forwarding transaction.
// pub fn delete_call_forwarding(
//     params: DeleteCallForwarding,
//     protocol: Arc<Protocol>,
// ) -> Result<String, NonFatalError> {
//     // debug!(
//     //     "SELECT <s_id bind subid s_id>
//     //      FROM Subscriber
//     //      WHERE sub_nbr = {};
//     //    DELETE FROM Call_Forwarding
//     //      WHERE s_id = <s_id value subid>
//     //      AND sf_type = {}
//     //      AND start_time = {};",
//     //     helper::to_sub_nbr(params.s_id.into()),
//     //     params.sf_type,
//     //     params.start_time,
//     // );

//     // Construct primary keys.
//     let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
//     let pk_cf = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
//         params.s_id,
//         params.sf_type.into(),
//         params.start_time.into(),
//     ));

//     // Register with scheduler.
//     let meta = protocol.scheduler.register().unwrap();
//     // Get record from subscriber table.
//     let columns_sb: Vec<&str> = vec!["s_id"];
//     protocol
//         .scheduler
//         .read("subscriber", pk_sb, &columns_sb, meta.clone())?;

//     // Delete from call forwarding.
//     protocol
//         .scheduler
//         .delete("call_forwarding", pk_cf, meta.clone())?;

//     // Commit transaction.
//     protocol.scheduler.commit(meta.clone())?;

//     Ok("{\"deleted 1 row from call_forwarding.\"}".to_string())
// }

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
        logging(true);

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

        assert_eq!(
            deposit_checking(
                DepositChecking {
                    name: "cust1".to_string(),
                    value: 10.0
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{updated 1 row.\"}"
        );

        // assert_eq!(
        //     balance(
        //         Balance {
        //             name: "cust1".to_string()
        //         },
        //         Arc::clone(&protocol)
        //     )
        //     .unwrap(),
        //     "{total_balance=\"53344\"}"
        // );
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
