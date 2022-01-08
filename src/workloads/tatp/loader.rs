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

    for sid in 0..population {
        let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(sid as u64)); // create pk

        database.get_mut_table(0).get_mut_exists().insert(pk, sid); // insert into exists

        database.insert_value(0, 0, sid, Data::Uint(sid as u64)); // s_id
        let sub_nbr = tatp::helper::to_sub_nbr(sid as u64);
        database.insert_value(0, 1, sid, Data::VarChar(sub_nbr)); // sub_nbr
        database.insert_value(0, 2, sid, Data::Uint(rng.gen_range(0..=1) as u64)); // bit_1
        database.insert_value(0, 3, sid, Data::Uint(rng.gen_range(1..(2 ^ 32)) as u64)); // msc_location
        database.insert_value(0, 4, sid, Data::Uint(rng.gen_range(1..(2 ^ 32)) as u64));
        // vlr_location
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
            ai.get_mut_exists().insert(pk, ai_offset);

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

    // special facility

    let sf_type_values = vec![1, 2, 3, 4];
    let start_time_values = vec![0, 8, 16];
    let mut sf_offset = 0;

    // call forwarding
    let mut cf_offset = 0;

    for sid in 0..population {
        let n_sf = rng.gen_range(1..=4);
        let sample = sf_type_values.iter().choose_multiple(rng, n_sf);

        for record in 1..=n_sf {
            let sf = database.get_mut_table(2);
            let pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(sid as u64, record as u64));
            sf.get_mut_exists().insert(pk, sf_offset);

            let s_id = Data::Uint(sid as u64);
            sf.get_tuple(0, sf_offset).get().init_value(s_id)?;

            let sf_type = Data::Uint(*sample[record - 1] as u64);
            sf.get_tuple(1, sf_offset).get().init_value(sf_type)?;

            let is_active = Data::Uint(tatp::helper::is_active(rng));
            sf.get_tuple(2, sf_offset).get().init_value(is_active)?;

            let error_cntrl = Data::Uint(rng.gen_range(0..=255));
            sf.get_tuple(3, sf_offset).get().init_value(error_cntrl)?;

            let data_a = Data::Uint(rng.gen_range(0..=255));
            sf.get_tuple(4, sf_offset).get().init_value(data_a)?;

            let data_b = Data::VarChar(tatp::helper::get_data_x(5, rng));
            sf.get_tuple(5, sf_offset).get().init_value(data_b)?;

            drop(sf);

            // for each row, insert [0,3] into call forwarding table
            let n_cf = rng.gen_range(0..=3);
            let start_times = start_time_values.iter().choose_multiple(rng, n_cf);
            if n_cf != 0 {
                for i in 1..=n_cf {
                    let cf = database.get_mut_table(3);
                    let st = *start_times[i - 1];
                    let pk = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
                        sid as u64,
                        record as u64,
                        st,
                    ));

                    cf.get_mut_exists().insert(pk, cf_offset);

                    let s_id = Data::Uint(sid as u64);
                    cf.get_tuple(0, cf_offset).get().init_value(s_id)?;

                    let sf_type = Data::Uint(*sample[record - 1] as u64);
                    cf.get_tuple(1, cf_offset).get().init_value(sf_type)?;

                    let start_time = Data::Uint(st);
                    cf.get_tuple(2, cf_offset).get().init_value(start_time)?;

                    let end_time = Data::Uint(st + rng.gen_range(1..=8));
                    cf.get_tuple(3, cf_offset).get().init_value(end_time)?;

                    let number_x = Data::VarChar(tatp::helper::get_number_x(rng));
                    cf.get_tuple(4, cf_offset).get().init_value(number_x)?;

                    drop(cf);

                    cf_offset += 1;
                }
            }

            sf_offset += 1;
        }
    }

    info!("Loaded {} rows into special_facility", sf_offset);

    info!("Loaded {} rows into call_forwarding", cf_offset);

    Ok(())
}
