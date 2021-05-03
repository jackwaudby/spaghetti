use crate::common::error::NonFatalError;
use crate::server::scheduler::Protocol;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::tatp::keys::TatpPrimaryKey;
use crate::workloads::tatp::paramgen::{
    DeleteCallForwarding, GetAccessData, GetNewDestination, GetSubscriberData,
    InsertCallForwarding, UpdateLocationData, UpdateSubscriberData,
};
use crate::workloads::PrimaryKey;

use std::convert::TryFrom;
use std::sync::Arc;

/// GetSubscriberData transaction.
pub fn get_subscriber_data(
    params: GetSubscriberData,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
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

    let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));

    let meta = protocol.scheduler.register()?;

    let values = protocol.scheduler.read("subscriber", pk, &columns, &meta)?;

    protocol.scheduler.commit(&meta)?;

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();

    Ok(res)
}

/// GetNewDestination transaction.
pub fn get_new_destination(
    params: GetNewDestination,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let sf_columns: Vec<&str> = vec!["s_id", "sf_type", "is_active"];
    let cf_columns: Vec<&str> = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];

    let sf_pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id,
        params.sf_type.into(),
    ));
    let cf_pk = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
        params.s_id,
        params.sf_type.into(),
        params.start_time.into(),
    ));

    let meta = protocol.scheduler.register().unwrap();

    let sf_res = protocol
        .scheduler
        .read("special_facility", sf_pk.clone(), &sf_columns, &meta)?;

    let val = i64::try_from(sf_res[2].clone()).unwrap();

    if val != 1 {
        protocol.scheduler.abort(&meta).unwrap();
        return Err(NonFatalError::RowNotFound(
            format!("{}", sf_pk),
            "special_facility".to_string(),
        ));
    }

    let cf_res = protocol
        .scheduler
        .read("call_forwarding", cf_pk.clone(), &cf_columns, &meta)?;
    let val = i64::try_from(cf_res[3].clone()).unwrap();

    if params.end_time as i64 >= val {
        protocol.scheduler.abort(&meta).unwrap();
        return Err(NonFatalError::RowNotFound(
            format!("{}", cf_pk),
            "call_forwarding".to_string(),
        ));
    }

    protocol.scheduler.commit(&meta)?;

    let res = datatype::to_result(
        None,
        None,
        None,
        Some(&vec!["number_x"]),
        Some(&vec![cf_res[4].clone()]),
    )
    .unwrap();
    Ok(res)
}

/// GetAccessData transaction.
pub fn get_access_data(
    params: GetAccessData,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let columns: Vec<&str> = vec!["data_1", "data_2", "data_3", "data_4"];

    let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(
        params.s_id,
        params.ai_type.into(),
    ));

    let meta = protocol.scheduler.register().unwrap();

    let values = protocol
        .scheduler
        .read("access_info", pk, &columns, &meta)?;

    protocol.scheduler.commit(&meta)?;

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();

    Ok(res)
}

/// Update subscriber transaction.
pub fn update_subscriber_data(
    params: UpdateSubscriberData,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let columns_sb: Vec<String> = vec!["bit_1".to_string()];
    let values_sb = vec![Data::Int(params.bit_1.into())];
    let update = |_columns: Vec<String>,
                  _current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let value = i64::try_from(params[0].clone()).unwrap();
        let new_values = vec![value.to_string()];
        let columns = vec!["bit_1".to_string()];
        Ok((columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap();

    protocol.scheduler.update(
        "subscriber",
        pk_sb,
        columns_sb,
        false,
        values_sb,
        &update,
        &meta,
    )?;

    let pk_sp = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id,
        params.sf_type.into(),
    ));
    let columns_sp = vec!["data_a".to_string()];
    let values_sp = vec![Data::Int(params.data_a.into())];
    let update_sp = |_columns: Vec<String>,
                     _current: Option<Vec<Data>>,
                     params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let value = i64::try_from(params[0].clone()).unwrap();
        let new_values = vec![value.to_string()];
        let columns = vec!["data_a".to_string()];
        Ok((columns, new_values))
    };
    protocol.scheduler.update(
        "special_facility",
        pk_sp,
        columns_sp,
        false,
        values_sp,
        &update_sp,
        &meta,
    )?;

    protocol.scheduler.commit(&meta)?;
    let res = datatype::to_result(None, Some(2), None, None, None).unwrap();

    Ok(res)
}

/// Update location transaction.
pub fn update_location(
    params: UpdateLocationData,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let columns_sb: Vec<String> = vec!["vlr_location".to_string()];
    let values_sb = vec![Data::Int(params.vlr_location.into())];
    let update = |_columns: Vec<String>,
                  _current: Option<Vec<Data>>,
                  params: Vec<Data>|
     -> Result<(Vec<String>, Vec<String>), NonFatalError> {
        let value = i64::try_from(params[0].clone()).unwrap();
        let new_values = vec![value.to_string()];
        let columns = vec!["bit_1".to_string()];
        Ok((columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap();

    protocol.scheduler.update(
        "subscriber",
        pk_sb,
        columns_sb,
        false,
        values_sb,
        &update,
        &meta,
    )?;

    protocol.scheduler.commit(&meta)?;
    let res = datatype::to_result(None, Some(1), None, None, None).unwrap();

    Ok(res)
}

/// Insert call forwarding transaction.
pub fn insert_call_forwarding(
    params: InsertCallForwarding,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    // debug!(
    //     "SELECT <s_id bind subid s_id>
    //        FROM Subscriber
    //        WHERE sub_nbr = {};
    //      SELECT <sf_type bind sfid sf_type>
    //        FROM Special_Facility
    //        WHERE s_id = {}:
    //      INSERT INTO Call_Forwarding
    //        VALUES ({}, {}, {}, {}, {});",
    //     helper::to_sub_nbr(params.s_id.into()),
    //     params.s_id,
    //     params.s_id,
    //     params.sf_type,
    //     params.start_time,
    //     params.end_time,
    //     params.number_x
    // );

    // Construct primary keys.
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let pk_sf = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id,
        params.sf_type.into(),
    ));

    // Register with scheduler.
    let meta = protocol.scheduler.register().unwrap();
    // Get record from subscriber table.
    let columns_sb: Vec<&str> = vec!["s_id"];
    protocol
        .scheduler
        .read("subscriber", pk_sb, &columns_sb, &meta)?;
    // Get record from special facility.
    let columns_sf: Vec<&str> = vec!["sf_type"];
    protocol
        .scheduler
        .read("special_facility", pk_sf, &columns_sf, &meta)?;

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

    let values_cf: Vec<&str> = vec![&s_id, &sf_type, &start_time, &end_time, &params.number_x];

    // Execute insert operation.
    protocol
        .scheduler
        .create(cf_name, pk_cf, &columns_cf, &values_cf, &meta)?;

    // Commit transaction.
    protocol.scheduler.commit(&meta)?;
    let res = datatype::to_result(Some(1), None, None, None, None).unwrap();

    Ok(res)
}

/// Delete call forwarding transaction.
pub fn delete_call_forwarding(
    params: DeleteCallForwarding,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let pk_cf = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
        params.s_id,
        params.sf_type.into(),
        params.start_time.into(),
    ));

    let meta = protocol.scheduler.register().unwrap();

    let cols: Vec<&str> = vec!["s_id"];
    protocol.scheduler.read("subscriber", pk_sb, &cols, &meta)?;
    protocol.scheduler.delete("call_forwarding", pk_cf, &meta)?;

    protocol.scheduler.commit(&meta)?;
    let res = datatype::to_result(None, None, Some(1), None, None).unwrap();

    Ok(res)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::workloads::tatp::loader;
    use crate::workloads::{Internal, Workload};
    use config::Config;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::convert::TryInto;
    use test_env_log::test;

    #[test]
    fn transactions_test() {
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-tpl.toml"))
            .unwrap();
        let config = Arc::new(c);

        // Workload with fixed seed.
        let schema = "./schema/tatp_schema.txt".to_string();
        let internals = Internal::new(&schema, Arc::clone(&config)).unwrap();
        let seed = config.get_int("seed").unwrap();
        let mut rng = StdRng::seed_from_u64(seed.try_into().unwrap());
        loader::populate_tables(&internals, &mut rng).unwrap();
        let workload = Arc::new(Workload::Tatp(internals));

        // Scheduler.
        let workers = config.get_int("workers").unwrap();
        let protocol = Arc::new(Protocol::new(Arc::clone(&workload), workers as usize).unwrap());

        ///////////////////////////////////////
        //// GetSubscriberData ////
        ///////////////////////////////////////
        assert_eq!(
            get_subscriber_data(GetSubscriberData { s_id: 1 }, Arc::clone(&protocol)).unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"bit_1\":\"0\",\"bit_10\":\"0\",\"bit_2\":\"1\",\"bit_3\":\"0\",\"bit_4\":\"1\",\"bit_5\":\"1\",\"bit_6\":\"1\",\"bit_7\":\"0\",\"bit_8\":\"0\",\"bit_9\":\"1\",\"byte_2_1\":\"222\",\"byte_2_10\":\"52\",\"byte_2_2\":\"248\",\"byte_2_3\":\"210\",\"byte_2_4\":\"100\",\"byte_2_5\":\"205\",\"byte_2_6\":\"163\",\"byte_2_7\":\"118\",\"byte_2_8\":\"127\",\"byte_2_9\":\"77\",\"hex_1\":\"8\",\"hex_10\":\"9\",\"hex_2\":\"6\",\"hex_3\":\"10\",\"hex_4\":\"8\",\"hex_5\":\"2\",\"hex_6\":\"13\",\"hex_7\":\"8\",\"hex_8\":\"10\",\"hex_9\":\"1\",\"msc_location\":\"18\",\"s_id\":\"1\",\"sub_nbr\":\"000000000000001\"}}"
        );

        assert_eq!(
            format!(
                "{}",
                get_subscriber_data(GetSubscriberData { s_id: 100 }, Arc::clone(&protocol))
                    .unwrap_err()
            ),
            format!("not found: Subscriber(100) in sub_idx")
        );

        ///////////////////////////////////////
        //// GetNewDestination ////
        ///////////////////////////////////////
        assert_eq!(
            get_new_destination(
                GetNewDestination {
                    s_id: 1,
                    sf_type: 1,
                    start_time: 16,
                    end_time: 12,
                },
                Arc::clone(&protocol)
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"number_x\":\"563603967554067\"}}"

        );
        assert_eq!(
            format!(
                "{}",
                get_new_destination(
                    GetNewDestination {
                        s_id: 200,
                        sf_type: 1,
                        start_time: 0,
                        end_time: 1,
                    },
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("not found: SpecialFacility(200, 1) in special_idx")
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
                Arc::clone(&protocol)
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"data_1\":\"24\",\"data_2\":\"188\",\"data_3\":\"NRL\",\"data_4\":\"CWBOF\"}}"
        );

        assert_eq!(
            format!(
                "{}",
                get_access_data(
                    GetAccessData {
                        s_id: 19,
                        ai_type: 12
                    },
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("not found: AccessInfo(19, 12) in access_idx")
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
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                &columns_sb,
                "2pl",
                "t1",
            )
            .unwrap();
        let values_sf = workload
            .get_internals()
            .get_index("special_idx")
            .unwrap()
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 1)),
                &columns_sf,
                "2pl",
                "t1",
            )
            .unwrap();

        let res_sb = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_sb),
            Some(&values_sb.get_values().unwrap()),
        )
        .unwrap();
        let res_sf = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_sf),
            Some(&values_sf.get_values().unwrap()),
        )
        .unwrap();
        assert_eq!(
            res_sb,
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"bit_1\":\"0\"}}"
        );
        assert_eq!(
            res_sf,
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"data_a\":\"81\"}}"
        );

        assert_eq!(
            update_subscriber_data(
                UpdateSubscriberData {
                    s_id: 1,
                    sf_type: 1,
                    bit_1: 1,
                    data_a: 29,
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":2,\"deleted\":null,\"val\":null}"
        );

        // After
        let values_sb = workload
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                &columns_sb,
                "2pl",
                "t1",
            )
            .unwrap();
        let values_sf = workload
            .get_internals()
            .get_index("special_idx")
            .unwrap()
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 1)),
                &columns_sf,
                "2pl",
                "t1",
            )
            .unwrap();

        let res_sb = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_sb),
            Some(&values_sb.get_values().unwrap()),
        )
        .unwrap();
        let res_sf = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_sf),
            Some(&values_sf.get_values().unwrap()),
        )
        .unwrap();
        assert_eq!(
            res_sb,
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"bit_1\":\"1\"}}"
        );
        assert_eq!(
            res_sf,
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"data_a\":\"29\"}}"
        );

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
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("not found: Subscriber(1345) in sub_idx")
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
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                &columns_sb,
                "2pl",
                "t1",
            )
            .unwrap();
        let res_sb = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_sb),
            Some(&values_sb.get_values().unwrap()),
        )
        .unwrap();
        assert_eq!(res_sb,"{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"vlr_location\":\"14\"}}");

        assert_eq!(
            update_location(
                UpdateLocationData {
                    s_id: 1,
                    vlr_location: 4
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":1,\"deleted\":null,\"val\":null}"
        );

        // After
        let values_sb = workload
            .get_internals()
            .get_index("sub_idx")
            .unwrap()
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                &columns_sb,
                "2pl",
                "t1",
            )
            .unwrap();

        let res_sb = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_sb),
            Some(&values_sb.get_values().unwrap()),
        )
        .unwrap();
        assert_eq!(
            res_sb,
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"vlr_location\":\"4\"}}"
        );

        assert_eq!(
            format!(
                "{}",
                update_location(
                    UpdateLocationData {
                        s_id: 1345,
                        vlr_location: 7,
                    },
                    Arc::clone(&protocol)
                )
                .unwrap_err()
            ),
            format!("not found: Subscriber(1345) in sub_idx")
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
                    .read(
                        PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 3, 0)),
                        &columns_cf,
                        "2pl",
                        "t1",
                    )
                    .unwrap_err()
            ),
            format!("not found: CallForwarding(1, 3, 0) in call_idx")
        );

        assert_eq!(
            insert_call_forwarding(
                InsertCallForwarding {
                    s_id: 2,
                    sf_type: 2,
                    start_time: 0,
                    end_time: 19,
                    number_x: "551795089196026".to_string()
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":1,\"updated\":null,\"deleted\":null,\"val\":null}"
        );

        let values_cf = workload
            .get_internals()
            .get_index("call_idx")
            .unwrap()
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(2, 2, 0)),
                &columns_cf,
                "2pl",
                "t1",
            )
            .unwrap();
        let res_cf = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_cf),
            Some(&values_cf.get_values().unwrap()),
        )
        .unwrap();

        assert_eq!(
            res_cf,
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"number_x\":\"551795089196026\"}}"

        );

        //////////////////////////////////////////
        //// DeleteCallForwarding ////
        /////////////////////////////////////////

        let columns_cf = vec!["number_x"];

        let values_cf = workload
            .get_internals()
            .get_index("call_idx")
            .unwrap()
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(2, 2, 0)),
                &columns_cf,
                "2pl",
                "t1",
            )
            .unwrap();
        let res_cf = datatype::to_result(
            None,
            None,
            None,
            Some(&columns_cf),
            Some(&values_cf.get_values().unwrap()),
        )
        .unwrap();

        assert_eq!(res_cf,
"{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"number_x\":\"551795089196026\"}}"
);

        assert_eq!(
            delete_call_forwarding(
                DeleteCallForwarding {
                    s_id: 2,
                    sf_type: 2,
                    start_time: 0,
                },
                Arc::clone(&protocol)
            )
            .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":1,\"val\":null}"
        );

        assert_eq!(
            format!(
                "{}",
                workload
                    .get_internals()
                    .get_index("call_idx")
                    .unwrap()
                    .read(
                        PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(2, 2, 0)),
                        &columns_cf,
                        "2pl",
                        "t1",
                    )
                    .unwrap_err()
            ),
            format!("not found: CallForwarding(2, 2, 0) in call_idx")
        );
    }
}
