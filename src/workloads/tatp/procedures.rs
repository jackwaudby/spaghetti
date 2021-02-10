use crate::common::error::SpaghettiError;
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::tatp::helper;
use crate::workloads::tatp::keys::TatpPrimaryKey;
use crate::workloads::tatp::profiles::{
    DeleteCallForwarding, GetAccessData, GetNewDestination, GetSubscriberData,
    InsertCallForwarding, UpdateLocationData, UpdateSubscriberData,
};
use crate::workloads::PrimaryKey;
use crate::Result;

use chrono::{DateTime, Utc};
use std::sync::Arc;
use tracing::debug;

/// GetSubscriberData transaction.
pub fn get_subscriber_data(
    params: GetSubscriberData,
    t_id: &str,
    t_ts: DateTime<Utc>,
    protocol: Arc<Protocol>,
) -> Result<String> {
    debug!(
        "\nSELECT s_id, sub_nbr,
            bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7,
            bit_8, bit_9, bit_10,
            hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7,
            hex_8, hex_9, hex_10,
            byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,
            byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,
            msc_location, vlr_location
   FROM Subscriber
   WHERE s_id = {:?};",
        params.s_id
    );

    // Columns to read.
    let columns: Vec<&str> = vec![
        "s_id",
        "sub_nbr",
        "bit_1",
        "bit_2",
        "bit_3",
        "bit_4",
        "bit_5",
        "bit_6",
        "bit_7",
        "bit_8",
        "bit_9",
        "bit_10",
        "hex_1",
        "hex_2",
        "hex_3",
        "hex_4",
        "hex_5",
        "hex_6",
        "hex_7",
        "hex_8",
        "hex_9",
        "hex_10",
        "byte_2_1",
        "byte_2_2",
        "byte_2_3",
        "byte_2_4",
        "byte_2_5",
        "byte_2_6",
        "byte_2_7",
        "byte_2_8",
        "byte_2_9",
        "byte_2_10",
        "msc_location",
    ];
    // Construct primary key.
    let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    // Register with scheduler.
    protocol.scheduler.register(t_id)?;
    // Execute read operation.
    let values = protocol
        .scheduler
        .read("sub_idx", pk, &columns, t_id, t_ts)?;
    // Commit transaction.
    protocol.scheduler.commit(t_id)?;
    // Convert to result
    let res = datatype::to_result(&columns, &values)?;

    Ok(res)
}

/// GetNewDestination transaction.
pub fn get_new_destination(
    params: GetNewDestination,
    t_id: &str,
    t_ts: DateTime<Utc>,
    protocol: Arc<Protocol>,
) -> Result<String> {
    debug!(
        "\nSELECT cf.numberx
           FROM Special_Facility AS sf, Call_Forwarding AS cf
           WHERE
             (sf.s_id = {} AND sf.sf_type = {} AND sf.is_active = 1)
             AND
             (cf.s_id = {} AND cf.sf_type = {})
             AND
            (cf.start_time <= {} AND  {} < cf.end_time);",
        params.s_id,
        params.sf_type,
        params.s_id,
        params.sf_type,
        params.start_time,
        params.end_time
    );

    // Columns to read.
    let sf_columns: Vec<&str> = vec!["s_id", "sf_type", "is_active"];
    let cf_columns: Vec<&str> = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
    // Construct PKs.
    let sf_pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id.into(),
        params.sf_type.into(),
    ));
    let cf_pk = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
        params.s_id.into(),
        params.sf_type.into(),
        params.start_time.into(),
    ));
    // Register with scheduler.
    protocol.scheduler.register(t_id).unwrap();
    // Execute read operations.
    // 1) Attempt to get the special facility record.
    let sf_res = protocol
        .scheduler
        .read("special_idx", sf_pk, &sf_columns, t_id, t_ts)?;
    // 2) Check sf.is_active = 1.
    let val = if let Data::Int(val) = sf_res[2] {
        val
    } else {
        panic!("Unexpected type")
    };
    if val != 1 {
        return Err(Box::new(SpaghettiError::RowDoesNotExist));
    }
    // 3) Get call forwarding record.
    let cf_res = protocol
        .scheduler
        .read("call_idx", cf_pk, &cf_columns, t_id, t_ts)?;
    // 4) Check end_time < cf.end_time
    let val = if let Data::Int(val) = cf_res[3] {
        val
    } else {
        panic!("Unexpected type")
    };
    if params.end_time as i64 >= val {
        return Err(Box::new(SpaghettiError::RowDoesNotExist));
    }
    // Commit transaction.
    protocol.scheduler.commit(t_id)?;
    // Convert to result
    let res = datatype::to_result(&vec![cf_columns[4].clone()], &vec![cf_res[4].clone()])?;
    Ok(res)
}

/// GetAccessData transaction.
pub fn get_access_data(
    params: GetAccessData,
    t_id: &str,
    t_ts: DateTime<Utc>,
    protocol: Arc<Protocol>,
) -> Result<String> {
    debug!(
        "SELECT data1, data2, data3, data4
           FROM Access_Info
         WHERE s_id = {:?}
           AND ai_type = {:?} ",
        params.s_id, params.ai_type
    );

    // Columns to read.
    let columns: Vec<&str> = vec!["data_1", "data_2", "data_3", "data_4"];
    // Construct primary key.
    let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(
        params.s_id,
        params.ai_type.into(),
    ));
    debug!("{}", pk);
    // Register with scheduler.
    protocol.scheduler.register(t_id).unwrap();
    // Execute read operation.
    let values = protocol
        .scheduler
        .read("access_idx", pk, &columns, t_id, t_ts)?;
    // Commit transaction.
    protocol.scheduler.commit(t_id)?;
    // Convert to result
    let res = datatype::to_result(&columns, &values)?;

    Ok(res)
}

/// Update subscriber transaction.
pub fn update_subscriber_data(
    params: UpdateSubscriberData,
    t_id: &str,
    t_ts: DateTime<Utc>,
    protocol: Arc<Protocol>,
) -> Result<String> {
    debug!(
        "UPDATE Subscriber
           SET bit_1 = {:?}
           WHERE s_id = {:?}
         UPDATE Special_Facility
           SET data_a = {:?}
           WHERE s_id = {:?}
             AND sf_type = {:?};",
        params.bit_1, params.s_id, params.data_a, params.s_id, params.sf_type
    );

    // Columns to write.
    let columns_sb: Vec<&str> = vec!["bit_1"];
    let columns_sp = vec!["data_a"];
    // Values to write.
    let values_sb = vec![params.bit_1.to_string()];
    let values_sb: Vec<&str> = values_sb.iter().map(|s| s as &str).collect();
    let values_sp = vec![params.data_a.to_string()];
    let values_sp: Vec<&str> = values_sp.iter().map(|s| s as &str).collect();

    // Construct primary key.
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let pk_sp = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id,
        params.sf_type.into(),
    ));

    // Register with scheduler.
    protocol.scheduler.register(t_id).unwrap();

    // Execute write operation.
    protocol
        .scheduler
        .write("sub_idx", pk_sb, &columns_sb, &values_sb, t_id, t_ts)?;
    protocol
        .scheduler
        .write("special_idx", pk_sp, &columns_sp, &values_sp, t_id, t_ts)?;

    // Commit transaction.
    protocol.scheduler.commit(t_id)?;

    Ok("{\"updated 2 rows.\"}".to_string())
}

/// Update location transaction.
pub fn update_location(
    params: UpdateLocationData,
    t_id: &str,
    t_ts: DateTime<Utc>,
    protocol: Arc<Protocol>,
) -> Result<String> {
    debug!(
        "UPDATE Subscriber
             SET vlr_location = {}
             WHERE sub_nbr = {};",
        helper::to_sub_nbr(params.s_id.into()),
        params.vlr_location
    );

    // Columns to write.
    let columns_sb: Vec<&str> = vec!["vlr_location"];
    // Values to write.
    let values_sb = vec![params.vlr_location.to_string()];
    let values_sb: Vec<&str> = values_sb.iter().map(|s| s as &str).collect();

    // Construct primary key.
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));

    // Register with scheduler.
    protocol.scheduler.register(t_id)?;

    // Execute write operation.
    protocol
        .scheduler
        .write("sub_idx", pk_sb, &columns_sb, &values_sb, t_id, t_ts)?;

    // Commit transaction.
    protocol.scheduler.commit(t_id)?;

    Ok("{\"updated 1 row.\"}".to_string())
}

/// Insert call forwarding transaction.
pub fn insert_call_forwarding(
    params: InsertCallForwarding,
    t_id: &str,
    t_ts: DateTime<Utc>,
    protocol: Arc<Protocol>,
) -> Result<String> {
    debug!(
        "SELECT <s_id bind subid s_id>
           FROM Subscriber
           WHERE sub_nbr = {};
         SELECT <sf_type bind sfid sf_type>
           FROM Special_Facility
           WHERE s_id = {}:
         INSERT INTO Call_Forwarding
           VALUES ({}, {}, {}, {}, {});",
        helper::to_sub_nbr(params.s_id.into()),
        params.s_id,
        params.s_id,
        params.sf_type,
        params.start_time,
        params.end_time,
        params.number_x
    );

    // Construct primary keys.
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let pk_sf = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id,
        params.sf_type.into(),
    ));

    // Register with scheduler.
    protocol.scheduler.register(t_id).unwrap();
    // Get record from subscriber table.
    let columns_sb: Vec<&str> = vec!["s_id"];
    protocol
        .scheduler
        .read("sub_idx", pk_sb, &columns_sb, t_id, t_ts)?;
    // Get record from special facility.
    let columns_sf: Vec<&str> = vec!["sf_type"];
    protocol
        .scheduler
        .read("special_idx", pk_sf, &columns_sf, t_id, t_ts)?;

    // Insert into call forwarding.
    // Calculate primary key
    let pk_cf = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
        params.s_id,
        params.sf_type.into(),
        params.start_time.into(),
    ));
    // Table name
    let cf_name = "call_forwarding";
    // Columns
    let columns_cf: Vec<&str> = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
    // Values
    let s_id = params.s_id.to_string();
    let sf_type = params.sf_type.to_string();
    let start_time = params.start_time.to_string();
    let end_time = params.end_time.to_string();
    let number_x = params.number_x.to_string();
    let values_cf: Vec<&str> = vec![&s_id, &sf_type, &start_time, &end_time, &number_x];

    // Execute insert operation.
    protocol
        .scheduler
        .insert(cf_name, pk_cf, &columns_cf, &values_cf, t_id)?;

    // Commit transaction.
    protocol.scheduler.commit(t_id)?;

    Ok("{\"inserted 1 row into call_forwarding.\"}".to_string())
}

/// Delete call forwarding transaction.
pub fn delete_call_forwarding(
    params: DeleteCallForwarding,
    t_id: &str,
    t_ts: DateTime<Utc>,
    protocol: Arc<Protocol>,
) -> Result<String> {
    debug!(
        "SELECT <s_id bind subid s_id>
         FROM Subscriber
         WHERE sub_nbr = {};
       DELETE FROM Call_Forwarding
         WHERE s_id = <s_id value subid>
         AND sf_type = {}
         AND start_time = {};",
        helper::to_sub_nbr(params.s_id.into()),
        params.sf_type,
        params.start_time,
    );

    // Construct primary keys.
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let pk_cf = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
        params.s_id,
        params.sf_type.into(),
        params.start_time.into(),
    ));

    // Register with scheduler.
    protocol.scheduler.register(t_id).unwrap();
    // Get record from subscriber table.
    let columns_sb: Vec<&str> = vec!["s_id"];
    protocol
        .scheduler
        .read("sub_idx", pk_sb, &columns_sb, t_id, t_ts)?;

    // Delete from call forwarding.
    protocol.scheduler.delete("call_idx", pk_cf, t_id)?;

    // Commit transaction.
    protocol.scheduler.commit(t_id)?;

    Ok("{\"deleted 1 row from call_forwarding.\"}".to_string())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::workloads::tatp::loader;
    use crate::workloads::{Internal, Workload};
    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::Once;
    use std::time::SystemTime;
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

    lazy_static! {
        static ref CONFIG: Arc<Config> = {
            // Initialise configuration.
            let mut c = Config::default();
            c.merge(config::File::with_name("Test.toml")).unwrap();
            let config = Arc::new(c);
            config
        };
    }

    #[test]
    fn transactions_test() {
        logging(false);
        // Workload with fixed seed
        let mut rng = StdRng::seed_from_u64(42);
        let config = Arc::clone(&CONFIG);
        let internals = Internal::new("tatp_schema.txt", config).unwrap();
        loader::populate_tables(&internals, &mut rng).unwrap();
        let workload = Arc::new(Workload::Tatp(internals));
        // Protocol
        let protocol = Arc::new(Protocol::new(Arc::clone(&workload)).unwrap());
        let sys_time = SystemTime::now();
        let datetime: DateTime<Utc> = sys_time.into();
        let t_id = datetime.to_string();
        let t_ts = datetime;

        ///////////////////////////////////////
        //// GetSubscriberData ////
        ///////////////////////////////////////
        assert_eq!(
            get_subscriber_data(GetSubscriberData { s_id: 1 }, &t_id, t_ts, Arc::clone(&protocol)).unwrap(),
            "{s_id=\"1\", sub_nbr=\"000000000000001\", bit_1=\"0\", bit_2=\"1\", bit_3=\"0\", bit_4=\"1\", bit_5=\"1\", bit_6=\"1\", bit_7=\"0\", bit_8=\"0\", bit_9=\"1\", bit_10=\"0\", hex_1=\"8\", hex_2=\"6\", hex_3=\"10\", hex_4=\"8\", hex_5=\"2\", hex_6=\"13\", hex_7=\"8\", hex_8=\"10\", hex_9=\"1\", hex_10=\"9\", byte_2_1=\"222\", byte_2_2=\"248\", byte_2_3=\"210\", byte_2_4=\"100\", byte_2_5=\"205\", byte_2_6=\"163\", byte_2_7=\"118\", byte_2_8=\"127\", byte_2_9=\"77\", byte_2_10=\"52\", msc_location=\"16\"}"
        );

        assert_eq!(
            format!(
                "{}",
                get_subscriber_data(
                    GetSubscriberData { s_id: 100 },
                    &t_id,
                    t_ts,
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("row does not exist in index.")
        );

        ///////////////////////////////////////
        //// GetNewDestination ////
        ///////////////////////////////////////
        assert_eq!(
            get_new_destination(
                GetNewDestination {
                    s_id: 1,
                    sf_type: 1,
                    start_time: 8,
                    end_time: 12,
                },
                &t_id,
                t_ts,
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{number_x=\"993245295996111\"}"
        );
        assert_eq!(
            format!(
                "{}",
                get_new_destination(
                    GetNewDestination {
                        s_id: 10,
                        sf_type: 1,
                        start_time: 0,
                        end_time: 1,
                    },
                    &t_id,
                    t_ts,
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("row does not exist in index.")
        );

        //////////////////////////////////
        //// GetAccessData ////
        /////////////////////////////////
        assert_eq!(
            get_access_data(
                GetAccessData {
                    s_id: 1,
                    ai_type: 1
                },
                &t_id,
                t_ts,
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{data_1=\"165\", data_2=\"166\", data_3=\"FPK\", data_4=\"BLZPL\"}"
        );

        assert_eq!(
            format!(
                "{}",
                get_access_data(
                    GetAccessData {
                        s_id: 19,
                        ai_type: 12
                    },
                    &t_id,
                    t_ts,
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("row does not exist in index.")
        );

        ////////////////////////////////////////////
        //// UpdateSubscriberData ////
        ///////////////////////////////////////////

        let columns_sb = vec!["bit_1"];
        let columns_sf = vec!["data_a"];

        // Before
        let values_sb = workload
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .index_read(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), &columns_sb)
            .unwrap();
        let values_sf = workload
            .get_internals()
            .get_index("special_idx")
            .unwrap()
            .index_read(
                PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 1)),
                &columns_sf,
            )
            .unwrap();

        let res_sb = datatype::to_result(&columns_sb, &values_sb).unwrap();
        let res_sf = datatype::to_result(&columns_sf, &values_sf).unwrap();
        assert_eq!(res_sb, "{bit_1=\"0\"}");
        assert_eq!(res_sf, "{data_a=\"73\"}");

        assert_eq!(
            update_subscriber_data(
                UpdateSubscriberData {
                    s_id: 1,
                    sf_type: 1,
                    bit_1: 1,
                    data_a: 29,
                },
                &t_id,
                t_ts,
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"updated 2 rows.\"}"
        );

        // After
        let values_sb = workload
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .index_read(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), &columns_sb)
            .unwrap();
        let values_sf = workload
            .get_internals()
            .get_index("special_idx")
            .unwrap()
            .index_read(
                PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 1)),
                &columns_sf,
            )
            .unwrap();

        let res_sb = datatype::to_result(&columns_sb, &values_sb).unwrap();
        let res_sf = datatype::to_result(&columns_sf, &values_sf).unwrap();
        assert_eq!(res_sb, "{bit_1=\"1\"}");
        assert_eq!(res_sf, "{data_a=\"29\"}");

        assert_eq!(
            format!(
                "{}",
                update_subscriber_data(
                    UpdateSubscriberData {
                        s_id: 1345,
                        sf_type: 132,
                        bit_1: 0,
                        data_a: 28,
                    },
                    &t_id,
                    t_ts,
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("row does not exist in index.")
        );

        ////////////////////////////////
        //// UpdateLocation ////
        /////////////////////////////////

        let columns_sb = vec!["vlr_location"];

        // Before
        let values_sb = workload
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .index_read(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), &columns_sb)
            .unwrap();
        let res_sb = datatype::to_result(&columns_sb, &values_sb).unwrap();
        assert_eq!(res_sb, "{vlr_location=\"12\"}");

        assert_eq!(
            update_location(
                UpdateLocationData {
                    s_id: 1,
                    vlr_location: 4
                },
                &t_id,
                t_ts,
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"updated 1 row.\"}"
        );

        // After
        let values_sb = workload
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .index_read(PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)), &columns_sb)
            .unwrap();

        let res_sb = datatype::to_result(&columns_sb, &values_sb).unwrap();
        assert_eq!(res_sb, "{vlr_location=\"4\"}");

        assert_eq!(
            format!(
                "{}",
                update_location(
                    UpdateLocationData {
                        s_id: 1345,
                        vlr_location: 7,
                    },
                    &t_id,
                    t_ts,
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("row does not exist in index.")
        );

        /////////////////////////////////////////
        //// InsertCallForwarding ////
        ////////////////////////////////////////
        let columns_cf = vec!["number_x"];
        assert_eq!(
            format!(
                "{}",
                workload
                    .get_internals()
                    .get_index("call_idx")
                    .unwrap()
                    .index_read(
                        PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 16)),
                        &columns_cf,
                    )
                    .unwrap_err()
            ),
            format!("row does not exist in index.")
        );

        assert_eq!(
            insert_call_forwarding(
                InsertCallForwarding {
                    s_id: 1,
                    sf_type: 3,
                    start_time: 16,
                    end_time: 19,
                    number_x: "551795089196026".to_string()
                },
                &t_id,
                t_ts,
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"inserted 1 row into call_forwarding.\"}"
        );

        let values_cf = workload
            .get_internals()
            .get_index("call_idx")
            .unwrap()
            .index_read(
                PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 16)),
                &columns_cf,
            )
            .unwrap();
        let res_cf = datatype::to_result(&columns_cf, &values_cf).unwrap();

        assert_eq!(res_cf, "{number_x=\"551795089196026\"}");

        //////////////////////////////////////////
        //// DeleteCallForwarding ////
        /////////////////////////////////////////

        let columns_cf = vec!["number_x"];

        let values_cf = workload
            .get_internals()
            .get_index("call_idx")
            .unwrap()
            .index_read(
                PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 0)),
                &columns_cf,
            )
            .unwrap();
        let res_cf = datatype::to_result(&columns_cf, &values_cf).unwrap();

        assert_eq!(res_cf, "{number_x=\"551795089196025\"}");

        assert_eq!(
            delete_call_forwarding(
                DeleteCallForwarding {
                    s_id: 1,
                    sf_type: 3,
                    start_time: 0,
                },
                &t_id,
                t_ts,
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"deleted 1 row from call_forwarding.\"}"
        );

        assert_eq!(
            format!(
                "{}",
                workload
                    .get_internals()
                    .get_index("call_idx")
                    .unwrap()
                    .index_read(
                        PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 0)),
                        &columns_cf,
                    )
                    .unwrap_err()
            ),
            format!("row does not exist in index.")
        );
    }
}
