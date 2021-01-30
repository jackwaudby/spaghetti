use crate::common::message::Message;
use crate::common::parameter_generation::Generator;
use crate::server::storage::row::Row;
use crate::workloads::Internal;
use crate::workloads::Workload;
use crate::Result;

use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

pub mod helper;

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
    for s_id in 0..subs {
        let mut row = Row::new(Arc::clone(&t));
        row.set_primary_key(s_id);
        row.set_value("s_id", &s_id.to_string())?;
        row.set_value("sub_nbr", &helper::to_sub_nbr(s_id))?;
        for i in 1..10 {
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
        i.index_insert(s_id, row);
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
    for s_id in 0..subscribers {
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
            i.index_insert(pk, row);
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
    for s_id in 0..subscribers {
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
            i.index_insert(pk, row);

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
                row.set_value("start_time", &st.to_string())?;
                row.set_value("end_time", &et.to_string())?;
                row.set_value("number_x", &nx)?;
                debug!("{}", row);
                cf_i.index_insert(pk, row);
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

/////////////////////////////////////
/// Stored Procedures. ///
/////////////////////////////////////

pub fn get_subscriber_data(s_id: u64, workload: Arc<Workload>) -> Result<String> {
    debug!(
        "  SELECT s_id, sub_nbr,
            bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7,
            bit_8, bit_9, bit_10,
            hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7,
            hex_8, hex_9, hex_10,
            byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,
            byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,
            msc_location, vlr_location
   FROM Subscriber
   WHERE s_id = {:?};",
        s_id
    );

    let result = match *workload {
        Workload::Tatp(ref internals) => {
            let key = s_id;
            let index = internals.get_index("sub_idx")?;
            let row = index.index_read(key).unwrap();
            row.get_value("sub_nbr")?
        }
        Workload::Tpcc(ref _internals) => Some(String::from("test")),
    };

    match result {
        Some(res) => Ok(res),
        None => Ok("null".to_string()),
    }
}

/////////////////////////////////////////
/// Parameter Generator. ///
////////////////////////////////////////

pub struct TatpGenerator {
    subscribers: u64,
    rng: StdRng,
}

impl TatpGenerator {
    pub fn new(subscribers: u64) -> TatpGenerator {
        // Initialise the thread local rng.
        let rng: StdRng = SeedableRng::from_entropy();

        TatpGenerator { subscribers, rng }
    }
}

impl Generator for TatpGenerator {
    fn generate(&mut self) -> Message {
        let n: f32 = self.rng.gen();

        let _transaction = match n {
            x if x < 0.35 => {
                // GET_SUBSCRIBER_DATA
                // TODO: implement non-uniform distribution.
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let payload = GetSubscriberData { s_id };
                TatpTransaction::GetSubscriberData(payload)
            }
            x if x < 0.45 => {
                // GET_NEW_DESTINATION
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let end_time = self.rng.gen_range(1..=24);
                let payload = GetNewDestination {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                };
                TatpTransaction::GetNewDestination(payload)
            }
            x if x < 0.8 => {
                // GET_ACCESS_DATA
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let ai_type = self.rng.gen_range(1..=4);
                let payload = GetAccessData { s_id, ai_type };
                TatpTransaction::GetAccessData(payload)
            }
            x if x < 0.82 => {
                // UPDATE_SUBSCRIBER_DATA
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let bit_1 = self.rng.gen_range(0..=1);
                let data_a = self.rng.gen_range(0..=255);
                let payload = UpdateSubscriberData {
                    s_id,
                    sf_type,
                    bit_1,
                    data_a,
                };
                TatpTransaction::UpdateSubscriberData(payload)
            }
            x if x < 0.96 => {
                // UPDATE_LOCATION
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let vlr_location = self.rng.gen_range(1..=2 ^ 32 - 1);
                let payload = UpdateLocationData { s_id, vlr_location };
                TatpTransaction::UpdateLocationData(payload)
            }
            x if x < 0.98 => {
                // INSERT CALL_FORWARDING
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let end_time = self.rng.gen_range(1..=24);
                let number_x = helper::get_number_x(&mut self.rng);
                let payload = InsertCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                    number_x,
                };
                TatpTransaction::InsertCallForwarding(payload)
            }
            _ => {
                // DELETE_CALL_FORWARDING
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let sf_type = self.rng.gen_range(1..=4);
                let start_time = helper::get_start_time(&mut self.rng);
                let payload = DeleteCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                };
                TatpTransaction::DeleteCallForwarding(payload)
            }
        };

        // TODO: Remove.
        let dat = GetSubscriberData { s_id: 0 };
        let transaction = TatpTransaction::GetSubscriberData(dat);

        Message::TatpTransaction(transaction)
    }
}

///////////////////////////////////////
/// Transaction Profiles. ///
//////////////////////////////////////
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct GetSubscriberData {
    pub s_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct GetNewDestination {
    s_id: u64,
    sf_type: u8,
    start_time: u8,
    end_time: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct GetAccessData {
    s_id: u64,
    ai_type: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct UpdateSubscriberData {
    s_id: u64,
    sf_type: u8,
    bit_1: u8,
    data_a: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct UpdateLocationData {
    s_id: u64,
    vlr_location: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct InsertCallForwarding {
    s_id: u64,
    sf_type: u8,
    start_time: u8,
    end_time: u8,
    number_x: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DeleteCallForwarding {
    s_id: u64,
    sf_type: u8,
    start_time: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TatpTransaction {
    GetSubscriberData(GetSubscriberData),
    GetNewDestination(GetNewDestination),
    GetAccessData(GetAccessData),
    UpdateSubscriberData(UpdateSubscriberData),
    UpdateLocationData(UpdateLocationData),
    InsertCallForwarding(InsertCallForwarding),
    DeleteCallForwarding(DeleteCallForwarding),
}

#[cfg(test)]
mod tests {

    use super::*;
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
    fn pop_sub_table_test() {
        logging(false);
        let c = Arc::clone(&CONFIG);
        let internals = Internal::new("tatp_schema.txt", c).unwrap();
        let mut rng = StdRng::seed_from_u64(42);
        populate_subscriber_table(&internals, &mut rng).unwrap();

        assert_eq!(
            internals.get_table("subscriber").unwrap().get_next_row_id(),
            1
        );

        let index = internals.indexes.get("sub_idx").unwrap();
        let pk = 0;
        let row = index.index_read(pk).unwrap();
        let value = row.get_value("bit_1").unwrap().unwrap();
        assert_eq!(value, "0");
    }
}
