use crate::storage::datatype::Data;
use crate::storage::PrimaryKey;
use crate::workloads::tatp::keys::TatpPrimaryKey;
use crate::workloads::tatp::{self, TatpDatabase};
use crate::Result;

use rand::prelude::IteratorRandom;
use rand::rngs::StdRng;
use rand::Rng;
use tracing::info;

pub fn populate_tables(
    population: usize,
    database: &mut TatpDatabase,
    rng: &mut StdRng,
) -> Result<()> {
    // Subscriber
    // Included: s_id; sub_nbr; bit_1; msc_location; vlr_location
    let sub = database.get_mut_table(0);
    for sid in 0..population {
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(sid as u64)); // create pk
        sub.get_mut_exists().insert(pk, sid); // insert into exists

        sub.get_tuple(0, sid)
            .get()
            .init_value(Data::Uint(sid as u64))?; // s_id

        let sub_nbr = tatp::helper::to_sub_nbr(sid as u64);
        sub.get_tuple(1, sid)
            .get()
            .init_value(Data::VarChar(sub_nbr))?; // sub_nbr

        sub.get_tuple(2, sid)
            .get()
            .init_value(Data::Uint(rng.gen_range(0..=1) as u64))?; // bit_1

        sub.get_tuple(3, sid)
            .get()
            .init_value(Data::Uint(rng.gen_range(1..(2 ^ 32)) as u64))?; // msc_location

        sub.get_tuple(4, sid)
            .get()
            .init_value(Data::Uint(rng.gen_range(1..(2 ^ 32)) as u64))?; // vlr_location
    }

    info!("Loaded {} rows into subscriber", population);

    // Access Info
    let ai = database.get_mut_table(1);
    let ai_type_values = vec![1, 2, 3, 4];
    let mut ai_offset = 0;
    for sid in 0..population {
        let n_ai = rng.gen_range(1..=4);
        let sample = ai_type_values.iter().choose_multiple(rng, n_ai);

        for record in 1..=n_ai {
            let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(sid as u64, record as u64));
            ai.get_mut_exists().insert(pk, ai_offset); // TODO

            let s_id = Data::Uint(sid as u64);
            ai.get_tuple(0, ai_offset).get().init_value(s_id)?;

            let ai_type = Data::Uint(*sample[record - 1] as u64);
            ai.get_tuple(1, ai_offset).get().init_value(ai_type)?;

            let data1 = Data::Uint(rng.gen_range(0..=255));
            ai.get_tuple(2, ai_offset).get().init_value(data1)?;

            let data2 = Data::Uint(rng.gen_range(0..=255));
            ai.get_tuple(3, ai_offset).get().init_value(data2)?;

            let data3 = Data::VarChar(tatp::helper::get_data_x(3, rng));
            ai.get_tuple(4, ai_offset).get().init_value(data3)?;

            let data4 = Data::VarChar(tatp::helper::get_data_x(5, rng));
            ai.get_tuple(5, ai_offset).get().init_value(data4)?;

            ai_offset += 1;
        }
    }
    info!("Loaded {} rows into access_info", ai_offset);

    Ok(())
}

// /// Populate the `AccessInfo` table.
// pub fn populate_access_info(
//     config: Arc<Config>,
//     tables: &mut HashMap<String, Arc<Table>>,
//     indexes: &mut HashMap<String, Index>,
//     rng: &mut StdRng,
// ) -> Result<()> {
//     let access_info = tables.get_mut("access_info").unwrap();
//     let access_idx = indexes.get_mut("access_idx").unwrap();

//     let protocol = config.get_str("protocol")?;
//     let sf = config.get_int("scale_factor")? as u64;
//     let subscribers = *TATP_SF_MAP.get(&sf).unwrap();

//     for s_id in 1..=subscribers {
//         let n_ai = rng.gen_range(1..=4); // generate number of records for a given s_id

//         let sample = ai_type_values.iter().choose_multiple(rng, n_ai); // randomly sample w.o. replacement from range of ai_type values
//         for record in 1..=n_ai {
//             let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(s_id, record as u64));
//             let mut row = Row::new(pk.clone(), Arc::clone(&access_info));

//             row.init_value("s_id", Data::from(s_id))?;
//             row.init_value("ai_type", Data::from(*sample[record - 1] as u64))?;
//             row.init_value("data_1", Data::from(rng.gen_range(0..=255) as u64))?;
//             row.init_value("data_2", Data::from(rng.gen_range(0..=255) as u64))?;
//             row.init_value("data_3", Data::from(helper::get_data_x(3, rng)))?;
//             row.init_value("data_4", Data::from(helper::get_data_x(5, rng)))?;

//             access_idx.insert(&pk, row);
//         }
//     }

//     info!(
//         "Loaded {} rows into access_info",
//         access_info.get_num_rows()
//     );

//     Ok(())
// }

// /// Populate the `SpecialFacility` table and `CallForwarding` table.
// pub fn populate_special_facility_call_forwarding(
//     config: Arc<Config>,
//     tables: &mut HashMap<String, Arc<Table>>,
//     indexes: &mut HashMap<String, Index>,
//     rng: &mut StdRng,
// ) -> Result<()> {
//     debug!("Populating special_facility table");
//     debug!("Populating call_forwarding table");

//     let protocol = config.get_str("protocol")?;

//     let special_facility = tables.get("special_facility").unwrap();

//     let call_forwarding = tables.get("call_forwarding").unwrap();

//     let sf_type_values = vec![1, 2, 3, 4]; // range of values for ai_type records
//     let start_time_values = vec![0, 8, 16]; // range of values for start_time

//     let sf = config.get_int("scale_factor")? as u64;
//     let subscribers = *TATP_SF_MAP.get(&sf).unwrap();

//     for s_id in 1..=subscribers {
//         let n_sf = rng.gen_range(1..=4); // generate number of records for a given s_id
//         let sample = sf_type_values.iter().choose_multiple(rng, n_sf); // randomly sample w.o. replacement from range of ai_type values

//         for record in 1..=n_sf {
//             let pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(s_id, record as u64)); // calculate primary key
//             let mut row = Row::new(pk.clone(), Arc::clone(&special_facility)); // initialise empty row
//             let is_active = helper::is_active(rng); // calculate is_active

//             row.init_value("s_id", Data::from(s_id))?;
//             row.init_value("sf_type", Data::from(*sample[record - 1] as u64))?;
//             row.init_value("is_active", Data::from(is_active))?;
//             row.init_value("error_cntrl", Data::from(rng.gen_range(0..=255) as u64))?;
//             row.init_value("data_a", Data::from(rng.gen_range(0..=255) as u64))?;
//             row.init_value("data_b", Data::from(helper::get_data_x(5, rng)))?;
//             let special_idx = indexes.get_mut("special_idx").unwrap();
//             special_idx.insert(&pk, row);
//             drop(special_idx);

//             // for each row, insert [0,3] into call forwarding table
//             let n_cf = rng.gen_range(0..=3); // generate the number to insert
//             let start_times = start_time_values.iter().choose_multiple(rng, n_cf); // randomly sample w.o. replacement from range of ai_type values
//             if n_cf != 0 {
//                 for i in 1..=n_cf {
//                     let st = *start_times[i - 1];
//                     let et = st + rng.gen_range(1..=8);
//                     let nx = helper::get_number_x(rng);

//                     let pk =
//                         PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(s_id, record as u64, st));

//                     let mut row = Row::new(pk.clone(), Arc::clone(&call_forwarding));

//                     row.init_value("s_id", Data::from(s_id))?;
//                     row.init_value("sf_type", Data::from(*sample[record - 1] as u64))?;
//                     row.init_value("start_time", Data::from(st))?;
//                     row.init_value("end_time", Data::from(et))?;
//                     row.init_value("number_x", Data::from(nx))?;
//                     let call_idx = indexes.get_mut("call_idx").unwrap();
//                     call_idx.insert(&pk, row);
//                     drop(call_idx);
//                 }
//             }
//         }
//     }
//     info!(
//         "Loaded {} rows into special facility",
//         special_facility.get_num_rows()
//     );
//     info!(
//         "Loaded {} rows into call_forwarding",
//         call_forwarding.get_num_rows()
//     );
//     Ok(())
// }
