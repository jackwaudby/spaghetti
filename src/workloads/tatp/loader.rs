use crate::datagen::tatp::{AccessInfo, CallForwarding, SpecialFacility, Subscriber};
use crate::server::storage::row::Row;
use crate::workloads::tatp::helper;
use crate::workloads::tatp::keys::TatpPrimaryKey;
use crate::workloads::tatp::TATP_SF_MAP;
use crate::workloads::{Internal, PrimaryKey};
use crate::Result;

use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::Rng;

use std::sync::Arc;
use tracing::{debug, info};

/// Load subscriber table from csv files.
pub fn load_sub_table(data: &Internal) -> Result<()> {
    let config = data.get_config();
    let subscriber = data.get_table("subscriber")?;
    let sub_idx = data.get_index(&subscriber.get_primary_index()?)?;
    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")?;
    let path = format!("./data/tatp/sf-{}/subscribers.csv", sf); // dir

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let s: Subscriber = result?;
        let mut row = Row::new(Arc::clone(&subscriber), track_access, track_delayed);
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(s.s_id.parse::<u64>()?));
        row.set_primary_key(pk.clone());
        row.init_value("s_id", &s.s_id)?;

        row.init_value("bit_1", &s.bit_1)?;
        row.init_value("bit_2", &s.bit_2)?;
        row.init_value("bit_3", &s.bit_3)?;
        row.init_value("bit_4", &s.bit_4)?;
        row.init_value("bit_5", &s.bit_5)?;
        row.init_value("bit_6", &s.bit_6)?;
        row.init_value("bit_7", &s.bit_7)?;
        row.init_value("bit_8", &s.bit_8)?;
        row.init_value("bit_9", &s.bit_9)?;
        row.init_value("bit_10", &s.bit_10)?;

        row.init_value("hex_1", &s.hex_1)?;
        row.init_value("hex_2", &s.hex_2)?;
        row.init_value("hex_3", &s.hex_3)?;
        row.init_value("hex_4", &s.hex_4)?;
        row.init_value("hex_5", &s.hex_5)?;
        row.init_value("hex_6", &s.hex_6)?;
        row.init_value("hex_7", &s.hex_7)?;
        row.init_value("hex_8", &s.hex_8)?;
        row.init_value("hex_9", &s.hex_9)?;
        row.init_value("hex_10", &s.hex_10)?;

        row.init_value("byte_2_1", &s.byte_2_1)?;
        row.init_value("byte_2_2", &s.byte_2_2)?;
        row.init_value("byte_2_3", &s.byte_2_3)?;
        row.init_value("byte_2_4", &s.byte_2_4)?;
        row.init_value("byte_2_5", &s.byte_2_5)?;
        row.init_value("byte_2_6", &s.byte_2_6)?;
        row.init_value("byte_2_7", &s.byte_2_7)?;
        row.init_value("byte_2_8", &s.byte_2_8)?;
        row.init_value("byte_2_9", &s.byte_2_9)?;
        row.init_value("byte_2_10", &s.byte_2_10)?;

        row.init_value("msc_location", &s.msc_location)?;
        row.init_value("vlr_location", &s.vlr_location)?;

        sub_idx.insert(&pk, row)?;
    }
    info!("Loaded {} rows into subscriber", subscriber.get_num_rows());

    Ok(())
}

/// Load access_info table from csv files.
pub fn load_access_info_table(data: &Internal) -> Result<()> {
    let config = data.get_config();
    let access_info = data.get_table("access_info")?;
    let access_idx = data.get_index(&access_info.get_primary_index()?)?;
    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")?;
    let path = format!("./data/tatp/sf-{}/access_info.csv", sf); // dir

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let ai: AccessInfo = result?;

        let mut row = Row::new(Arc::clone(&access_info), track_access, track_delayed);

        let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(
            ai.s_id.parse::<u64>()?,
            ai.ai_type.parse::<u64>()?,
        ));
        row.set_primary_key(pk.clone());
        row.init_value("s_id", &ai.s_id)?;
        row.init_value("ai_type", &ai.ai_type)?;
        row.init_value("data_1", &ai.data_1)?;
        row.init_value("data_2", &ai.data_2)?;
        row.init_value("data_3", &ai.data_3)?;
        row.init_value("data_4", &ai.data_4)?;

        access_idx.insert(&pk, row)?;
    }
    info!(
        "Loaded {} rows into access_info",
        access_info.get_num_rows()
    );
    Ok(())
}

/// Load call_forwarding table from csv files.
pub fn load_call_forwarding_table(data: &Internal) -> Result<()> {
    let config = data.get_config();
    let call_forwarding = data.get_table("call_forwarding")?;
    let call_idx = data.get_index(&call_forwarding.get_primary_index()?)?;
    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")?;
    let path = format!("./data/tatp/sf-{}/call_forwarding.csv", sf);

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let cf: CallForwarding = result?;
        let mut row = Row::new(Arc::clone(&call_forwarding), track_access, track_delayed);
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
            cf.s_id.parse::<u64>()?,
            cf.sf_type.parse::<u64>()?,
            cf.start_time.parse::<u64>()?,
        ));
        row.set_primary_key(pk.clone());
        row.init_value("s_id", &cf.s_id)?;
        row.init_value("sf_type", &cf.sf_type)?;
        row.init_value("start_time", &cf.start_time)?;
        row.init_value("end_time", &cf.end_time)?;
        row.init_value("number_x", &cf.number_x)?;

        call_idx.insert(&pk, row)?;
    }
    info!(
        "Loaded {} rows into call_forwarding",
        call_forwarding.get_num_rows()
    );
    Ok(())
}

/// Load special_facility table from csv files.
pub fn load_special_facility_table(data: &Internal) -> Result<()> {
    let special_facility = data.get_table("special_facility")?;
    let special_idx = data.get_index(&special_facility.get_primary_index()?)?;

    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let path = format!("./data/tatp/sf-{}/special_facility.csv", sf); // dir

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let sf: SpecialFacility = result?;
        let mut row = Row::new(Arc::clone(&special_facility), track_access, track_delayed);
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
            sf.s_id.parse::<u64>()?,
            sf.sf_type.parse::<u64>()?,
        ));
        row.set_primary_key(pk.clone());
        row.init_value("s_id", &sf.s_id)?;
        row.init_value("sf_type", &sf.sf_type)?;
        row.init_value("is_active", &sf.is_active)?;
        row.init_value("error_cntrl", &sf.error_cntrl)?;
        row.init_value("data_a", &sf.data_a)?;
        row.init_value("data_b", &sf.data_b)?;

        special_idx.insert(&pk, row)?;
    }
    info!(
        "Loaded {} rows into special_facility",
        special_facility.get_num_rows()
    );
    Ok(())
}

/// Populate tables.
pub fn populate_tables(data: &Internal, rng: &mut StdRng) -> Result<()> {
    populate_subscriber_table(data, rng)?;
    populate_access_info(data, rng)?;
    populate_special_facility_call_forwarding(data, rng)?;
    Ok(())
}

/// Populate the `Subscriber` table.
pub fn populate_subscriber_table(data: &Internal, rng: &mut StdRng) -> Result<()> {
    let config = data.get_config();

    let subscriber = data.get_table("subscriber")?;
    let sub_idx = data.get_index(&subscriber.get_primary_index()?)?;

    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")? as u64;
    let subs = *TATP_SF_MAP.get(&sf).unwrap();

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    for s_id in 1..=subs {
        let mut row = Row::new(Arc::clone(&subscriber), track_access, track_delayed);
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(s_id));
        row.set_primary_key(pk.clone());
        row.init_value("s_id", &s_id.to_string())?;
        row.init_value("sub_nbr", &helper::to_sub_nbr(s_id))?;
        for i in 1..=10 {
            row.init_value(
                format!("bit_{}", i).as_str(),
                &rng.gen_range(0..=1).to_string(),
            )?;
            row.init_value(
                format!("hex_{}", i).as_str(),
                &rng.gen_range(0..=15).to_string(),
            )?;
            row.init_value(
                format!("byte_2_{}", i).as_str(),
                &rng.gen_range(0..=255).to_string(),
            )?;
        }
        row.init_value("msc_location", &rng.gen_range(1..(2 ^ 32)).to_string())?;
        row.init_value("vlr_location", &rng.gen_range(1..(2 ^ 32)).to_string())?;

        sub_idx.insert(&pk, row)?;
    }
    info!("Loaded {} rows into subscriber", subscriber.get_num_rows());

    Ok(())
}

/// Populate the `AccessInfo` table.
pub fn populate_access_info(data: &Internal, rng: &mut StdRng) -> Result<()> {
    let config = data.get_config();
    let access_info = data.get_table("access_info")?;
    let access_idx = data.get_index(&access_info.get_primary_index()?)?;

    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")? as u64;
    let subscribers = *TATP_SF_MAP.get(&sf).unwrap();

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let ai_type_values = vec![1, 2, 3, 4]; // range of values for ai_type records

    for s_id in 1..=subscribers {
        let n_ai = rng.gen_range(1..=4); // generate number of records for a given s_id

        let sample = ai_type_values.iter().choose_multiple(rng, n_ai); // randomly sample w.o. replacement from range of ai_type values
        for record in 1..=n_ai {
            let mut row = Row::new(Arc::clone(&access_info), track_access, track_delayed);
            let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(s_id, record as u64));
            row.set_primary_key(pk.clone());
            row.init_value("s_id", &s_id.to_string())?;
            row.init_value("ai_type", &sample[record - 1].to_string())?;
            row.init_value("data_1", &rng.gen_range(0..=255).to_string())?;
            row.init_value("data_2", &rng.gen_range(0..=255).to_string())?;
            row.init_value("data_3", &helper::get_data_x(3, rng))?;
            row.init_value("data_4", &helper::get_data_x(5, rng))?;

            access_idx.insert(&pk, row)?;
        }
    }

    info!(
        "Loaded {} rows into access_info",
        access_info.get_num_rows()
    );

    Ok(())
}

/// Populate the `SpecialFacility` table and `CallForwarding` table.
pub fn populate_special_facility_call_forwarding(data: &Internal, rng: &mut StdRng) -> Result<()> {
    debug!("Populating special_facility table");
    debug!("Populating call_forwarding table");
    let config = data.get_config();
    let protocol = config.get_str("protocol")?;

    let special_facility = data.get_table("special_facility")?;
    let special_idx = data.get_index(&special_facility.get_primary_index()?)?;

    let call_forwarding = data.get_table("call_forwarding")?;
    let call_idx = data.get_index(&call_forwarding.get_primary_index()?)?;

    let sf_type_values = vec![1, 2, 3, 4]; // range of values for ai_type records
    let start_time_values = vec![0, 8, 16]; // range of values for start_time

    let sf = config.get_int("scale_factor")? as u64;
    let subscribers = *TATP_SF_MAP.get(&sf).unwrap();

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    for s_id in 1..=subscribers {
        let n_sf = rng.gen_range(1..=4); // generate number of records for a given s_id
        let sample = sf_type_values.iter().choose_multiple(rng, n_sf); // randomly sample w.o. replacement from range of ai_type values

        for record in 1..=n_sf {
            let mut row = Row::new(Arc::clone(&special_facility), track_access, track_delayed); // initialise empty row
            let is_active = helper::is_active(rng); // calculate is_active
            let pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(s_id, record as u64)); // calculate primary key
            row.set_primary_key(pk.clone());
            row.init_value("s_id", &s_id.to_string())?;
            row.init_value("sf_type", &sample[record - 1].to_string())?;
            row.init_value("is_active", &is_active.to_string())?;
            row.init_value("error_cntrl", &rng.gen_range(0..=255).to_string())?;
            row.init_value("data_a", &rng.gen_range(0..=255).to_string())?;
            row.init_value("data_b", &helper::get_data_x(5, rng))?;

            special_idx.insert(&pk, row)?;

            // for each row, insert [0,3] into call forwarding table
            let n_cf = rng.gen_range(0..=3); // generate the number to insert
            let start_times = start_time_values.iter().choose_multiple(rng, n_cf); // randomly sample w.o. replacement from range of ai_type values
            if n_cf != 0 {
                for i in 1..=n_cf {
                    // s_id from above
                    // sf_type from above
                    let st = *start_times[i - 1];
                    let et = st + rng.gen_range(1..=8);
                    let nx = helper::get_number_x(rng);

                    let pk =
                        PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(s_id, record as u64, st));

                    let mut row =
                        Row::new(Arc::clone(&call_forwarding), track_access, track_delayed);
                    row.set_primary_key(pk.clone());
                    row.init_value("s_id", &s_id.to_string())?;
                    row.init_value("sf_type", &sample[record - 1].to_string())?;
                    row.init_value("start_time", &st.to_string())?;
                    row.init_value("end_time", &et.to_string())?;
                    row.init_value("number_x", &nx)?;

                    call_idx.insert(&pk, row)?;
                }
            }
        }
    }
    info!(
        "Loaded {} rows into special facility",
        special_facility.get_num_rows()
    );
    info!(
        "Loaded {} rows into call_forwarding",
        call_forwarding.get_num_rows()
    );
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::server::storage::datatype;
    use config::Config;
    use rand::SeedableRng;

    #[test]
    fn populate_tables_test() {
        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-tpl.toml"))
            .unwrap();
        let config = Arc::new(c);

        let c = Arc::clone(&config);
        let internals = Internal::new("./schema/tatp_schema.txt", c).unwrap();
        let mut rng = StdRng::seed_from_u64(1);

        // Subscriber.
        populate_subscriber_table(&internals, &mut rng).unwrap();
        assert_eq!(
            internals.get_table("subscriber").unwrap().get_next_row_id(),
            10
        );
        let index = internals.indexes.get("sub_idx").unwrap();

        let cols_s = vec![
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
            "vlr_location",
        ];

        let res = index
            .read(
                PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
                &cols_s,
                "2pl",
                "t1",
            )
            .unwrap()
            .get_values()
            .unwrap();
        assert_eq!(
            datatype::to_result(
                None,
                None,
                None,
                Some(&cols_s),
                Some(&res)
            ).unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"bit_1\":\"1\",\"bit_10\":\"0\",\"bit_2\":\"0\",\"bit_3\":\"1\",\"bit_4\":\"1\",\"bit_5\":\"0\",\"bit_6\":\"0\",\"bit_7\":\"0\",\"bit_8\":\"1\",\"bit_9\":\"0\",\"byte_2_1\":\"55\",\"byte_2_10\":\"203\",\"byte_2_2\":\"65\",\"byte_2_3\":\"99\",\"byte_2_4\":\"138\",\"byte_2_5\":\"93\",\"byte_2_6\":\"228\",\"byte_2_7\":\"150\",\"byte_2_8\":\"132\",\"byte_2_9\":\"121\",\"hex_1\":\"3\",\"hex_10\":\"0\",\"hex_2\":\"12\",\"hex_3\":\"15\",\"hex_4\":\"8\",\"hex_5\":\"2\",\"hex_6\":\"3\",\"hex_7\":\"5\",\"hex_8\":\"4\",\"hex_9\":\"7\",\"msc_location\":\"22\",\"s_id\":\"1\",\"sub_nbr\":\"000000000000001\",\"vlr_location\":\"7\"}}"
        );

        // Access info.
        populate_access_info(&internals, &mut rng).unwrap();
        assert_eq!(
            internals
                .get_table("access_info")
                .unwrap()
                .get_next_row_id(),
            24
        );

        let cols_ai = vec!["s_id", "ai_type", "data_1", "data_2", "data_3", "data_4"];

        let index = internals.indexes.get("access_idx").unwrap();
        assert_eq!(
            datatype::to_result(
                        None,
                None,
                None,
                Some(
                &cols_ai),
                Some(&index
                    .read(PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2)), &cols_ai,"2pl","t1")
                      .unwrap().get_values().unwrap())

            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"ai_type\":\"2\",\"data_1\":\"118\",\"data_2\":\"249\",\"data_3\":\"QYU\",\"data_4\":\"PTUKB\",\"s_id\":\"1\"}}"
        );

        // Special facillity.
        populate_special_facility_call_forwarding(&internals, &mut rng).unwrap();
        assert_eq!(
            internals
                .get_table("special_facility")
                .unwrap()
                .get_next_row_id(),
            28
        );

        let cols_sf = vec![
            "s_id",
            "sf_type",
            "is_active",
            "error_cntrl",
            "data_a",
            "data_b",
        ];
        let index = internals.indexes.get("special_idx").unwrap();

        assert_eq!(
            datatype::to_result(        None,
                None,
                None,
                Some(
                &cols_sf),
                Some(&index
                    .read(
                        PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 2)),
                        &cols_sf,
                        "2pl",
                        "t1",
                    )
                    .unwrap().get_values().unwrap())
            )
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"data_a\":\"217\",\"data_b\":\"IWPXS\",\"error_cntrl\":\"30\",\"is_active\":\"1\",\"s_id\":\"1\",\"sf_type\":\"3\"}}"


        );

        // Call forwarding.
        assert_eq!(
            internals
                .get_table("call_forwarding")
                .unwrap()
                .get_next_row_id(),
            48
        );
        let cols_cf = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
        let index = internals.indexes.get("call_idx").unwrap();
        assert_eq!(
            datatype::to_result(
                None,
                None,
                None,
                Some(&cols_cf),
                Some(&index
                    .read(
                        PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 2, 16)),
                        &cols_cf,
                        "2pl",
                        "t1",
                    )
                    .unwrap().get_values().unwrap()
            ))
                .unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"end_time\":\"22\",\"number_x\":\"255859837238459\",\"s_id\":\"1\",\"sf_type\":\"3\",\"start_time\":\"16\"}}"

        );
    }
}
