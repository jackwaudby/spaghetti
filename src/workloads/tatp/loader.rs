use crate::storage::datatype::Data;
use crate::storage::row::Row;
use crate::workloads::tatp::helper;
use crate::workloads::tatp::keys::TatpPrimaryKey;
use crate::workloads::tatp::TATP_SF_MAP;
use crate::workloads::{Index, PrimaryKey, Table};
use crate::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::Rng;
use std::collections::HashMap;

use std::sync::Arc;
use tracing::{debug, info};

/// Populate tables.
pub fn populate_tables(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
    rng: &mut StdRng,
) -> Result<()> {
    populate_subscriber_table(Arc::clone(&config), tables, indexes, rng)?;
    populate_access_info(Arc::clone(&config), tables, indexes, rng)?;
    populate_special_facility_call_forwarding(Arc::clone(&config), tables, indexes, rng)?;
    Ok(())
}

/// Populate the `Subscriber` table.
pub fn populate_subscriber_table(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
    rng: &mut StdRng,
) -> Result<()> {
    let subscriber = tables.get("subscriber").unwrap();
    let sub_idx = indexes.get_mut("sub_idx").unwrap();

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
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(s_id));
        let mut row = Row::new(
            pk.clone(),
            Arc::clone(&subscriber),
            track_access,
            track_delayed,
        );

        row.init_value("s_id", Data::from(s_id))?;
        row.init_value("sub_nbr", Data::from(helper::to_sub_nbr(s_id)))?;
        for i in 1..=10 {
            row.init_value(
                format!("bit_{}", i).as_str(),
                Data::from(rng.gen_range(0..=1) as u64),
            )?;
            row.init_value(
                format!("hex_{}", i).as_str(),
                Data::from(rng.gen_range(0..=15) as u64),
            )?;
            row.init_value(
                format!("byte_2_{}", i).as_str(),
                Data::from(rng.gen_range(0..=255) as u64),
            )?;
        }
        row.init_value(
            "msc_location",
            Data::from(rng.gen_range(1..(2 ^ 32)) as u64),
        )?;
        row.init_value(
            "vlr_location",
            Data::from(rng.gen_range(1..(2 ^ 32)) as u64),
        )?;

        sub_idx.insert(&pk, row);
    }
    info!("Loaded {} rows into subscriber", subscriber.get_num_rows());

    Ok(())
}

/// Populate the `AccessInfo` table.
pub fn populate_access_info(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
    rng: &mut StdRng,
) -> Result<()> {
    let access_info = tables.get_mut("access_info").unwrap();
    let access_idx = indexes.get_mut("access_idx").unwrap();

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
            let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(s_id, record as u64));
            let mut row = Row::new(
                pk.clone(),
                Arc::clone(&access_info),
                track_access,
                track_delayed,
            );

            row.init_value("s_id", Data::from(s_id))?;
            row.init_value("ai_type", Data::from(*sample[record - 1] as u64))?;
            row.init_value("data_1", Data::from(rng.gen_range(0..=255) as u64))?;
            row.init_value("data_2", Data::from(rng.gen_range(0..=255) as u64))?;
            row.init_value("data_3", Data::from(helper::get_data_x(3, rng)))?;
            row.init_value("data_4", Data::from(helper::get_data_x(5, rng)))?;

            access_idx.insert(&pk, row);
        }
    }

    info!(
        "Loaded {} rows into access_info",
        access_info.get_num_rows()
    );

    Ok(())
}

/// Populate the `SpecialFacility` table and `CallForwarding` table.
pub fn populate_special_facility_call_forwarding(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
    rng: &mut StdRng,
) -> Result<()> {
    debug!("Populating special_facility table");
    debug!("Populating call_forwarding table");

    let protocol = config.get_str("protocol")?;

    let special_facility = tables.get("special_facility").unwrap();

    let call_forwarding = tables.get("call_forwarding").unwrap();

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
            let pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(s_id, record as u64)); // calculate primary key
            let mut row = Row::new(
                pk.clone(),
                Arc::clone(&special_facility),
                track_access,
                track_delayed,
            ); // initialise empty row
            let is_active = helper::is_active(rng); // calculate is_active

            row.init_value("s_id", Data::from(s_id))?;
            row.init_value("sf_type", Data::from(*sample[record - 1] as u64))?;
            row.init_value("is_active", Data::from(is_active))?;
            row.init_value("error_cntrl", Data::from(rng.gen_range(0..=255) as u64))?;
            row.init_value("data_a", Data::from(rng.gen_range(0..=255) as u64))?;
            row.init_value("data_b", Data::from(helper::get_data_x(5, rng)))?;
            let special_idx = indexes.get_mut("special_idx").unwrap();
            special_idx.insert(&pk, row);
            drop(special_idx);

            // for each row, insert [0,3] into call forwarding table
            let n_cf = rng.gen_range(0..=3); // generate the number to insert
            let start_times = start_time_values.iter().choose_multiple(rng, n_cf); // randomly sample w.o. replacement from range of ai_type values
            if n_cf != 0 {
                for i in 1..=n_cf {
                    let st = *start_times[i - 1];
                    let et = st + rng.gen_range(1..=8);
                    let nx = helper::get_number_x(rng);

                    let pk =
                        PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(s_id, record as u64, st));

                    let mut row = Row::new(
                        pk.clone(),
                        Arc::clone(&call_forwarding),
                        track_access,
                        track_delayed,
                    );

                    row.init_value("s_id", Data::from(s_id))?;
                    row.init_value("sf_type", Data::from(*sample[record - 1] as u64))?;
                    row.init_value("start_time", Data::from(st))?;
                    row.init_value("end_time", Data::from(et))?;
                    row.init_value("number_x", Data::from(nx))?;
                    let call_idx = indexes.get_mut("call_idx").unwrap();
                    call_idx.insert(&pk, row);
                    drop(call_idx);
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
