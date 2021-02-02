use crate::server::storage::row::Row;
use crate::workloads::tatp::helper;
use crate::workloads::Internal;
use crate::Result;

use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::Rng;

use std::sync::Arc;
use tracing::{debug, info};

//////////////////////////////
/// Table Loaders. ///
//////////////////////////////

/// Populate tables.
pub fn populate_tables(data: &Internal, rng: &mut StdRng) -> Result<()> {
    populate_subscriber_table(data, rng)?;
    populate_access_info(data, rng)?;
    populate_special_facility_call_forwarding(data, rng)?;
    Ok(())
}

/// Populate the `Subscriber` table.
///
/// Schema:
/// Primary key: s_id
pub fn populate_subscriber_table(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Loading subscriber table");
    let s_name = "subscriber";
    let t = data.get_table(s_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;

    let subs = data.config.get_int("subscribers")? as u64;
    for s_id in 1..=subs {
        let mut row = Row::new(Arc::clone(&t));
        row.set_primary_key(s_id);
        row.set_value("s_id", &s_id.to_string())?;
        row.set_value("sub_nbr", &helper::to_sub_nbr(s_id))?;
        for i in 1..=10 {
            row.set_value(
                format!("bit_{}", i).as_str(),
                &rng.gen_range(0..=1).to_string(),
            )?;
            row.set_value(
                format!("hex_{}", i).as_str(),
                &rng.gen_range(0..=15).to_string(),
            )?;
            row.set_value(
                format!("byte_2_{}", i).as_str(),
                &rng.gen_range(0..=255).to_string(),
            )?;
        }
        row.set_value("msc_location", &rng.gen_range(1..=2 ^ 32 - 1).to_string())?;
        row.set_value("vlr_location", &rng.gen_range(1..=2 ^ 32 - 1).to_string())?;
        debug!("{}", row);
        i.index_insert(s_id, row)?;
    }
    info!("Loaded {} rows into subscriber", t.get_next_row_id());
    Ok(())
}

/// Populate the `AccessInfo` table.
///
/// Schema: (int,s_id) (int,ai_type) (int,data_1) (int,data_2) (string,data_3) (string,data_4)
/// Primary key: (s_id, ai_type)
pub fn populate_access_info(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Loading access_info table");
    // Get handle to `Table` and `Index`.
    let ai_name = "access_info";
    let t = data.get_table(ai_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;

    // Range of values for ai_type records.
    let ai_type_values = vec![1, 2, 3, 4];
    let subscribers = data.config.get_int("subscribers")? as u64;
    for s_id in 1..=subscribers {
        // Generate number of records for a given s_id.
        let n_ai = rng.gen_range(1..=4);
        // Randomly sample w.o. replacement from range of ai_type values.
        let sample = ai_type_values.iter().choose_multiple(rng, n_ai);
        for record in 0..n_ai {
            // Initialise empty row.
            let mut row = Row::new(Arc::clone(&t));
            // Calculate primary key
            let pk = helper::access_info_key(s_id, *sample[record]);
            row.set_primary_key(pk);
            row.set_value("s_id", &s_id.to_string())?;
            row.set_value("ai_type", &sample[record].to_string())?;
            row.set_value("data_1", &rng.gen_range(0..=255).to_string())?;
            row.set_value("data_2", &rng.gen_range(0..=255).to_string())?;
            row.set_value("data_3", &helper::get_data_x(3, rng))?;
            row.set_value("data_4", &helper::get_data_x(5, rng))?;
            debug!("{}", row);
            i.index_insert(pk, row)?;
        }
    }
    info!("Loaded {} rows into access_info", t.get_next_row_id());
    Ok(())
}

/// Populate the `SpecialFacility` table and `CallForwarding` table.
///
/// SpecialFacility
///
/// Schema: (int,s_id) (int,sf_type) (int,is_active) (int,error_cntrl) (string,data_a) (string,data_b)
/// Primary key: (s_id, sf_type,is_active)
///
/// CallForwarding
///
/// Schema:
///
/// Primary key:
pub fn populate_special_facility_call_forwarding(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Loading special_facility table");
    info!("Loading call_forwarding table");
    // Get handle to `Table` and `Index`.
    let sf_name = "special_facility";
    let t = data.get_table(sf_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;

    // Get handle to `CallForwarding` and `Index`.
    let cf_name = "call_forwarding";
    let cf_t = data.get_table(cf_name)?;
    let cf_i_name = cf_t.get_primary_index()?;
    let cf_i = data.get_index(&cf_i_name)?;

    // Range of values for ai_type records.
    let sf_type_values = vec![1, 2, 3, 4];

    // Range of values for start_time.
    let start_time_values = vec![0, 8, 16];

    let subscribers = data.config.get_int("subscribers")? as u64;
    for s_id in 1..=subscribers {
        // Generate number of records for a given s_id.
        let n_sf = rng.gen_range(1..=4);
        // Randomly sample w.o. replacement from range of ai_type values.
        let sample = sf_type_values.iter().choose_multiple(rng, n_sf);
        for record in 0..n_sf {
            // Initialise empty row.
            let mut row = Row::new(Arc::clone(&t));
            // Calculate is_active
            let is_active = helper::is_active(rng);
            // Calculate primary key
            let pk = helper::special_facility_key(s_id, *sample[record], is_active);
            row.set_primary_key(pk);
            row.set_value("s_id", &s_id.to_string())?;
            row.set_value("sf_type", &sample[record].to_string())?;
            row.set_value("is_active", &is_active.to_string())?;
            row.set_value("error_cntrl", &rng.gen_range(0..=255).to_string())?;
            row.set_value("data_a", &rng.gen_range(0..=255).to_string())?;
            row.set_value("data_b", &helper::get_data_x(5, rng))?;
            debug!("{}", row);
            i.index_insert(pk, row)?;

            // For each row, insert [0,3] into call forwarding table
            // Generate the number to insert
            let n_cf = rng.gen_range(0..=3);
            // Randomly sample w.o. replacement from range of ai_type values.
            let start_times = start_time_values.iter().choose_multiple(rng, n_cf);
            for i in 0..n_cf {
                // s_id from above
                // sf_type from above
                let st = *start_times[i];
                let et = st + rng.gen_range(1..=8);
                let nx = helper::get_number_x(rng);

                // Calculate primary key
                let pk = helper::call_forwarding_key(s_id, *sample[record], st);

                // Initialise empty row.
                let mut row = Row::new(Arc::clone(&cf_t));
                row.set_primary_key(pk);
                row.set_value("s_id", &s_id.to_string())?;
                row.set_value("sf_type", &sample[record].to_string())?;
                row.set_value("start_time", &st.to_string())?;
                row.set_value("end_time", &et.to_string())?;
                row.set_value("number_x", &nx)?;
                debug!("{}", row);
                cf_i.index_insert(pk, row)?;
            }
        }
    }
    info!("Loaded {} rows into special facility", t.get_next_row_id());
    info!(
        "Loaded {} rows into call_forwarding",
        cf_t.get_next_row_id()
    );
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::server::storage::datatype;
    use config::Config;
    use lazy_static::lazy_static;
    use rand::SeedableRng;
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
    fn populate_tables_test() {
        logging(false);
        let c = Arc::clone(&CONFIG);
        let internals = Internal::new("tatp_schema.txt", c).unwrap();
        let mut rng = StdRng::seed_from_u64(42);

        // Subscriber.
        populate_subscriber_table(&internals, &mut rng).unwrap();
        assert_eq!(
            internals.get_table("subscriber").unwrap().get_next_row_id(),
            2
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

        let res = index.index_read(1, &cols_s).unwrap();
        assert_eq!(
            datatype::to_result(&cols_s, &res).unwrap(),
            "[s_id=1, sub_nbr=000000000000001, bit_1=0, bit_2=1, bit_3=0, bit_4=1, bit_5=1, bit_6=1, bit_7=0, bit_8=0, bit_9=1, bit_10=0, hex_1=8, hex_2=6, hex_3=10, hex_4=8, hex_5=2, hex_6=13, hex_7=8, hex_8=10, hex_9=1, hex_10=9, byte_2_1=222, byte_2_2=248, byte_2_3=210, byte_2_4=100, byte_2_5=205, byte_2_6=163, byte_2_7=118, byte_2_8=127, byte_2_9=77, byte_2_10=52, msc_location=16, vlr_location=12]"
        );

        // Access info.
        populate_access_info(&internals, &mut rng).unwrap();
        assert_eq!(
            internals
                .get_table("access_info")
                .unwrap()
                .get_next_row_id(),
            5
        );

        let cols_ai = vec!["s_id", "ai_type", "data_1", "data_2", "data_3", "data_4"];

        let index = internals.indexes.get("access_idx").unwrap();
        assert_eq!(
            datatype::to_result(&cols_ai, &index.index_read(12, &cols_ai).unwrap()).unwrap(),
            "[s_id=1, ai_type=2, data_1=63, data_2=7, data_3=EMZ, data_4=WOVGK]"
        );

        // Special facillity.
        populate_special_facility_call_forwarding(&internals, &mut rng).unwrap();
        assert_eq!(
            internals
                .get_table("special_facility")
                .unwrap()
                .get_next_row_id(),
            5
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
            datatype::to_result(&cols_sf, &index.index_read(16, &cols_sf).unwrap()).unwrap(),
            "[s_id=1, sf_type=1, is_active=1, error_cntrl=122, data_a=73, data_b=PXESG]"
        );

        // Call forwarding.
        assert_eq!(
            internals
                .get_table("call_forwarding")
                .unwrap()
                .get_next_row_id(),
            10
        );
        let cols_cf = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
        let index = internals.indexes.get("call_idx").unwrap();
        assert_eq!(
            datatype::to_result(&cols_cf, &index.index_read(121, &cols_cf).unwrap()).unwrap(),
            "[s_id=1, sf_type=2, start_time=0, end_time=5, number_x=707677987012384]"
        );
    }
}
