use crate::server::storage::row::Row;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE, NUM_ACCOUNTS};
use crate::workloads::{Internal, PrimaryKey};
use crate::Result;

use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::Rng;

use std::sync::Arc;
use tracing::info;

//////////////////////////////
/// Table Loaders. ///
//////////////////////////////

// pub fn load_sub_table(data: &Internal) -> Result<()> {
//     info!("Loading subscriber table");
//     let s_name = "subscriber";
//     let t = data.get_table(s_name)?;
//     let i_name = t.get_primary_index()?;
//     let i = data.get_index(&i_name)?;
//     let protocol = data.config.get_str("protocol")?;

//     let mut rdr = csv::Reader::from_path("data/subscribers.csv")?;
//     for result in rdr.deserialize() {
//         // Deserialise.
//         let s: Subscriber = result?;
//         // Initialise empty row.
//         let mut row = Row::new(Arc::clone(&t), &protocol);
//         // Calculate primary key
//         let pk = PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(s.s_id.parse::<u64>()?));
//         row.set_primary_key(pk);
//         row.init_value("s_id", &s.s_id)?;

//         row.init_value("bit_1", &s.bit_1)?;
//         row.init_value("bit_2", &s.bit_2)?;
//         row.init_value("bit_3", &s.bit_3)?;
//         row.init_value("bit_4", &s.bit_4)?;
//         row.init_value("bit_5", &s.bit_5)?;
//         row.init_value("bit_6", &s.bit_6)?;
//         row.init_value("bit_7", &s.bit_7)?;
//         row.init_value("bit_8", &s.bit_8)?;
//         row.init_value("bit_9", &s.bit_9)?;
//         row.init_value("bit_10", &s.bit_10)?;

//         row.init_value("hex_1", &s.hex_1)?;
//         row.init_value("hex_2", &s.hex_2)?;
//         row.init_value("hex_3", &s.hex_3)?;
//         row.init_value("hex_4", &s.hex_4)?;
//         row.init_value("hex_5", &s.hex_5)?;
//         row.init_value("hex_6", &s.hex_6)?;
//         row.init_value("hex_7", &s.hex_7)?;
//         row.init_value("hex_8", &s.hex_8)?;
//         row.init_value("hex_9", &s.hex_9)?;
//         row.init_value("hex_10", &s.hex_10)?;

//         row.init_value("byte_2_1", &s.byte_2_1)?;
//         row.init_value("byte_2_2", &s.byte_2_2)?;
//         row.init_value("byte_2_3", &s.byte_2_3)?;
//         row.init_value("byte_2_4", &s.byte_2_4)?;
//         row.init_value("byte_2_5", &s.byte_2_5)?;
//         row.init_value("byte_2_6", &s.byte_2_6)?;
//         row.init_value("byte_2_7", &s.byte_2_7)?;
//         row.init_value("byte_2_8", &s.byte_2_8)?;
//         row.init_value("byte_2_9", &s.byte_2_9)?;
//         row.init_value("byte_2_10", &s.byte_2_10)?;

//         row.init_value("msc_location", &s.msc_location)?;
//         row.init_value("vlr_location", &s.vlr_location)?;

//         i.insert(pk, row)?;
//     }
//     info!("Loaded {} rows into subscriber", t.get_num_rows());
//     Ok(())
// }

// pub fn load_access_info_table(data: &Internal) -> Result<()> {
//     info!("Loading access_info table");
//     // Get handle to `Table` and `Index`.
//     let ai_name = "access_info";
//     let t = data.get_table(ai_name)?;
//     let i_name = t.get_primary_index()?;
//     let i = data.get_index(&i_name)?;
//     let protocol = data.config.get_str("protocol")?;

//     let mut rdr = csv::Reader::from_path("data/access_info.csv")?;
//     for result in rdr.deserialize() {
//         // Deserialise.
//         let ai: AccessInfo = result?;
//         // Initialise empty row.
//         let mut row = Row::new(Arc::clone(&t), &protocol);
//         // Calculate primary key
//         let pk = PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(
//             ai.s_id.parse::<u64>()?,
//             ai.ai_type.parse::<u64>()?,
//         ));
//         row.set_primary_key(pk);
//         row.init_value("s_id", &ai.s_id)?;
//         row.init_value("ai_type", &ai.ai_type)?;
//         row.init_value("data_1", &ai.data_1)?;
//         row.init_value("data_2", &ai.data_2)?;
//         row.init_value("data_3", &ai.data_3)?;
//         row.init_value("data_4", &ai.data_4)?;

//         i.insert(pk, row)?;
//     }
//     info!("Loaded {} rows into access_info", t.get_num_rows());
//     Ok(())
// }

// pub fn load_call_forwarding_table(data: &Internal) -> Result<()> {
//     info!("Loading call_forwarding table");
//     // Get handle to `Table` and `Index`.
//     let cf_name = "call_forwarding";
//     let t = data.get_table(cf_name)?;
//     let i_name = t.get_primary_index()?;
//     let i = data.get_index(&i_name)?;
//     let protocol = data.config.get_str("protocol")?;

//     let mut rdr = csv::Reader::from_path("data/call_forwarding.csv")?;
//     for result in rdr.deserialize() {
//         // Deserialise.
//         let cf: CallForwarding = result?;
//         // Initialise empty row.
//         let mut row = Row::new(Arc::clone(&t), &protocol);
//         // Calculate primary key
//         let pk = PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(
//             cf.s_id.parse::<u64>()?,
//             cf.sf_type.parse::<u64>()?,
//             cf.start_time.parse::<u64>()?,
//         ));
//         row.set_primary_key(pk);
//         row.init_value("s_id", &cf.s_id)?;
//         row.init_value("sf_type", &cf.sf_type)?;
//         row.init_value("start_time", &cf.start_time)?;
//         row.init_value("end_time", &cf.end_time)?;
//         row.init_value("number_x", &cf.number_x)?;

//         i.insert(pk, row)?;
//     }
//     info!("Loaded {} rows into call_forwarding", t.get_num_rows());
//     Ok(())
// }

// pub fn load_special_facility_table(data: &Internal) -> Result<()> {
//     info!("Loading special_facility table");
//     // Get handle to `Table` and `Index`.
//     let sf_name = "special_facility";
//     let t = data.get_table(sf_name)?;
//     let i_name = t.get_primary_index()?;
//     let i = data.get_index(&i_name)?;
//     let protocol = data.config.get_str("protocol")?;

//     let mut rdr = csv::Reader::from_path("data/special_facility.csv")?;
//     for result in rdr.deserialize() {
//         // Deserialise.
//         let sf: SpecialFacility = result?;
//         // Initialise empty row.
//         let mut row = Row::new(Arc::clone(&t), &protocol);
//         // Calculate primary key
//         let pk = PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(
//             sf.s_id.parse::<u64>()?,
//             sf.sf_type.parse::<u64>()?,
//         ));
//         row.set_primary_key(pk);
//         row.init_value("s_id", &sf.s_id)?;
//         row.init_value("sf_type", &sf.sf_type)?;
//         row.init_value("is_active", &sf.is_active)?;
//         row.init_value("error_cntrl", &sf.error_cntrl)?;
//         row.init_value("data_a", &sf.data_a)?;
//         row.init_value("data_b", &sf.data_b)?;

//         i.insert(pk, row)?;
//     }
//     info!("Loaded {} rows into special_facility", t.get_num_rows());
//     Ok(())
// }

///////////////////////////////////////////////
/// Table Generate and Load. ///
///////////////////////////////////////////////

/// Populate tables.
pub fn populate_tables(data: &Internal, rng: &mut StdRng) -> Result<()> {
    populate_account(data, rng)?;
    populate_savings(data, rng)?;
    populate_checking(data, rng)?;
    Ok(())
}

/// Populate the `Account` table.
pub fn populate_account(data: &Internal, rng: &mut StdRng) -> Result<()> {
    // Get table and index.
    let table_name = "account";
    let t = data.get_table(table_name)?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;

    // Get protocol.
    let protocol = data.config.get_str("protocol")?;

    info!("Populating accounts table: {}", NUM_ACCOUNTS);

    for a_id in 1..=NUM_ACCOUNTS {
        let mut row = Row::new(Arc::clone(&t), &protocol);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(a_id));
        row.set_primary_key(pk);
        row.init_value("name", &format!("customer{}", a_id))?;
        row.init_value("customer_id", &a_id.to_string())?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into account", t.get_num_rows());

    Ok(())
}

/// Populate the `Savings` table.
pub fn populate_savings(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Populating savings table");
    // Get handle to `Table` and `Index`.
    let table_name = "savings";
    let t = data.get_table(table_name)?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;

    // Get protocol.
    let protocol = data.config.get_str("protocol")?;

    for customer_id in 1..=NUM_ACCOUNTS {
        let mut row = Row::new(Arc::clone(&t), &protocol);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(customer_id));
        row.set_primary_key(pk);
        row.init_value("customer_id", &customer_id.to_string())?;
        let balance = rng.gen_range(MIN_BALANCE..=MAX_BALANCE);
        row.init_value("balance", &balance.to_string())?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into savings", t.get_num_rows());
    Ok(())
}

/// Populate the `Checking` table.
pub fn populate_checking(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Populating checking table");
    // Get handle to `Table` and `Index`.
    let table_name = "checking";
    let t = data.get_table(table_name)?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;

    // Get protocol.
    let protocol = data.config.get_str("protocol")?;

    for customer_id in 1..=NUM_ACCOUNTS {
        let mut row = Row::new(Arc::clone(&t), &protocol);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(customer_id));
        row.set_primary_key(pk);
        row.init_value("customer_id", &customer_id.to_string())?;
        let balance = rng.gen_range(MIN_BALANCE..=MAX_BALANCE);
        row.init_value("balance", &balance.to_string())?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into savings", t.get_num_rows());
    Ok(())
}

// #[cfg(test)]
// mod tests {

//     use super::*;
//     use crate::server::storage::datatype;
//     use config::Config;
//     use rand::SeedableRng;

//     #[test]
//     fn populate_tables_test() {
//         // Initialise configuration.
//         let mut c = Config::default();
//         c.merge(config::File::with_name("Test-tpl.toml")).unwrap();
//         let config = Arc::new(c);

//         let c = Arc::clone(&config);
//         let internals = Internal::new("tatp_schema.txt", c).unwrap();
//         let mut rng = StdRng::seed_from_u64(1);

//         // Subscriber.
//         populate_subscriber_table(&internals, &mut rng).unwrap();
//         assert_eq!(
//             internals.get_table("subscriber").unwrap().get_next_row_id(),
//             3
//         );
//         let index = internals.indexes.get("sub_idx").unwrap();

//         let cols_s = vec![
//             "s_id",
//             "sub_nbr",
//             "bit_1",
//             "bit_2",
//             "bit_3",
//             "bit_4",
//             "bit_5",
//             "bit_6",
//             "bit_7",
//             "bit_8",
//             "bit_9",
//             "bit_10",
//             "hex_1",
//             "hex_2",
//             "hex_3",
//             "hex_4",
//             "hex_5",
//             "hex_6",
//             "hex_7",
//             "hex_8",
//             "hex_9",
//             "hex_10",
//             "byte_2_1",
//             "byte_2_2",
//             "byte_2_3",
//             "byte_2_4",
//             "byte_2_5",
//             "byte_2_6",
//             "byte_2_7",
//             "byte_2_8",
//             "byte_2_9",
//             "byte_2_10",
//             "msc_location",
//             "vlr_location",
//         ];

//         let res = index
//             .read(
//                 PrimaryKey::Tatp(TatpPrimaryKey::Subscriber(1)),
//                 &cols_s,
//                 "2pl",
//                 "t1",
//             )
//             .unwrap()
//             .get_values()
//             .unwrap();
//         assert_eq!(
//             datatype::to_result(&cols_s, &res).unwrap(),
//           "{s_id=\"1\", sub_nbr=\"000000000000001\", bit_1=\"1\", bit_2=\"0\", bit_3=\"1\", bit_4=\"1\", bit_5=\"0\", bit_6=\"0\", bit_7=\"0\", bit_8=\"1\", bit_9=\"0\", bit_10=\"0\", hex_1=\"3\", hex_2=\"12\", hex_3=\"15\", hex_4=\"8\", hex_5=\"2\", hex_6=\"3\", hex_7=\"5\", hex_8=\"4\", hex_9=\"7\", hex_10=\"0\", byte_2_1=\"55\", byte_2_2=\"65\", byte_2_3=\"99\", byte_2_4=\"138\", byte_2_5=\"93\", byte_2_6=\"228\", byte_2_7=\"150\", byte_2_8=\"132\", byte_2_9=\"121\", byte_2_10=\"203\", msc_location=\"8\", vlr_location=\"9\"}"
//         );

//         // Access info.
//         populate_access_info(&internals, &mut rng).unwrap();
//         assert_eq!(
//             internals
//                 .get_table("access_info")
//                 .unwrap()
//                 .get_next_row_id(),
//             10
//         );

//         let cols_ai = vec!["s_id", "ai_type", "data_1", "data_2", "data_3", "data_4"];

//         let index = internals.indexes.get("access_idx").unwrap();
//         assert_eq!(
//             datatype::to_result(
//                 &cols_ai,
//                 &index
//                     .read(PrimaryKey::Tatp(TatpPrimaryKey::AccessInfo(1, 2)), &cols_ai,"2pl","t1")
//                       .unwrap().get_values().unwrap()

//             )
//                 .unwrap(),
// "{s_id=\"1\", ai_type=\"2\", data_1=\"77\", data_2=\"7\", data_3=\"GZH\", data_4=\"HITFS\"}"
//         );

//         // Special facillity.
//         populate_special_facility_call_forwarding(&internals, &mut rng).unwrap();
//         assert_eq!(
//             internals
//                 .get_table("special_facility")
//                 .unwrap()
//                 .get_next_row_id(),
//             10
//         );

//         let cols_sf = vec![
//             "s_id",
//             "sf_type",
//             "is_active",
//             "error_cntrl",
//             "data_a",
//             "data_b",
//         ];
//         let index = internals.indexes.get("special_idx").unwrap();

//         assert_eq!(
//             datatype::to_result(
//                 &cols_sf,
//                 &index
//                     .read(
//                         PrimaryKey::Tatp(TatpPrimaryKey::SpecialFacility(1, 2)),
//                         &cols_sf,
//                         "2pl",
//                         "t1",
//                     )
//                     .unwrap().get_values().unwrap()
//             )
//                 .unwrap(),
//             "{s_id=\"1\", sf_type=\"4\", is_active=\"1\", error_cntrl=\"90\", data_a=\"95\", data_b=\"RCSAA\"}"
//         );

//         // Call forwarding.
//         assert_eq!(
//             internals
//                 .get_table("call_forwarding")
//                 .unwrap()
//                 .get_next_row_id(),
//             15
//         );
//         let cols_cf = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
//         let index = internals.indexes.get("call_idx").unwrap();
//         assert_eq!(
//             datatype::to_result(
//                 &cols_cf,
//                 &index
//                     .read(
//                         PrimaryKey::Tatp(TatpPrimaryKey::CallForwarding(1, 2, 16)),
//                         &cols_cf,
//                         "2pl",
//                         "t1",
//                     )
//                     .unwrap().get_values().unwrap()
//             )
//                 .unwrap(),
//             "{s_id=\"1\", sf_type=\"4\", start_time=\"16\", end_time=\"17\", number_x=\"365430140201306\"}"
//         );
//     }
// }
