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
    let columns = [
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

    let values = protocol
        .scheduler
        .read("subscriber", &pk, &columns, &meta)?;

    protocol.scheduler.commit(&meta)?;

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();

    Ok(res)
}

/// GetNewDestination transaction.
pub fn get_new_destination(
    params: GetNewDestination,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    use TatpPrimaryKey::*;

    let sf_columns = ["s_id", "sf_type", "is_active"];
    let cf_columns = ["s_id", "sf_type", "start_time", "end_time", "number_x"];

    let sf_pk = PrimaryKey::Tatp(SpecialFacility(params.s_id, params.sf_type.into()));
    let cf_pk = PrimaryKey::Tatp(CallForwarding(
        params.s_id,
        params.sf_type.into(),
        params.start_time.into(),
    ));

    let meta = protocol.scheduler.register().unwrap();

    let sf_res = protocol
        .scheduler
        .read("special_facility", &sf_pk, &sf_columns, &meta)?;

    let is_active = i64::try_from(sf_res[2].clone()).unwrap();

    if is_active != 1 {
        protocol.scheduler.abort(&meta).unwrap();
        return Err(NonFatalError::RowNotFound(
            sf_pk.to_string(),
            "special_facility".to_string(),
        ));
    }

    let cf_res = protocol
        .scheduler
        .read("call_forwarding", &cf_pk, &cf_columns, &meta)?;

    let end_time = i64::try_from(cf_res[3].clone()).unwrap();

    if params.end_time as i64 >= end_time {
        protocol.scheduler.abort(&meta).unwrap();
        return Err(NonFatalError::RowNotFound(
            cf_pk.to_string(),
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
        .read("access_info", &pk, &columns, &meta)?;

    protocol.scheduler.commit(&meta)?;

    let res = datatype::to_result(None, None, None, Some(&columns), Some(&values)).unwrap();

    Ok(res)
}

/// Update subscriber transaction.
pub fn update_subscriber_data(
    params: UpdateSubscriberData,
    protocol: Arc<Protocol>,
) -> Result<String, NonFatalError> {
    let pk1 = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let pk2 = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id,
        params.sf_type.into(),
    ));

    let columns1 = ["bit_1"];
    let columns2 = ["data_a"];

    let values1 = vec![Data::Int(params.bit_1.into())];
    let values2 = vec![Data::Int(params.data_a.into())];

    let update = |columns: &[&str],
                  _current: Option<Vec<Data>>,
                  params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let new_values = vec![params.unwrap()[0].clone().clone()];
        Ok((new_columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap();

    protocol.scheduler.update(
        "subscriber",
        &pk1,
        &columns1,
        false,
        Some(&values1),
        &update,
        &meta,
    )?;

    protocol.scheduler.update(
        "special_facility",
        &pk2,
        &columns2,
        false,
        Some(&values2),
        &update,
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
    let columns = ["vlr_location"];
    let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let params = vec![Data::Int(params.vlr_location.into())];

    let update_vlr = |columns: &[&str],
                      _current: Option<Vec<Data>>,
                      params: Option<&[Data]>|
     -> Result<(Vec<String>, Vec<Data>), NonFatalError> {
        let new_columns: Vec<String> = columns.into_iter().map(|s| s.to_string()).collect();
        let new_values = vec![params.unwrap()[0].clone()b.clone()];
        Ok((new_columns, new_values))
    };

    let meta = protocol.scheduler.register().unwrap();

    protocol.scheduler.update(
        "subscriber",
        &pk,
        &columns,
        false,
        Some(&params),
        &update_vlr,
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
    let pk_sb = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(params.s_id));
    let pk_sf = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
        params.s_id,
        params.sf_type.into(),
    ));
    let pk_cf = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
        params.s_id,
        params.sf_type.into(),
        params.start_time.into(),
    ));

    let meta = protocol.scheduler.register().unwrap();

    protocol
        .scheduler
        .read("subscriber", &pk_sb, &vec!["s_id"], &meta)?;

    protocol
        .scheduler
        .read("special_facility", &pk_sf, &vec!["sf_type"], &meta)?;

    let s_id = Data::from(params.s_id);
    let sf_type = Data::from(params.sf_type as u64);
    let start_time = Data::from(params.start_time as u64);
    let end_time = Data::from(params.end_time as u64);
    let number_x = Data::from(params.number_x);
    let values_cf = vec![s_id, sf_type, start_time, end_time, number_x];

    protocol.scheduler.create(
        "call_forwarding",
        &pk_cf,
        &vec!["s_id", "sf_type", "start_time", "end_time", "number_x"],
        &values_cf,
        &meta,
    )?;

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

    protocol
        .scheduler
        .read("subscriber", &pk_sb, &vec!["s_id"], &meta)?;
    protocol
        .scheduler
        .delete("call_forwarding", &pk_cf, &meta)?;

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
