use crate::common::error::SpaghettiError;
use crate::common::message::Message;
use crate::common::parameter_generation::Generator;
use crate::server::scheduler::Scheduler;
use crate::server::storage::datatype::{self, Data};
use crate::server::storage::row::Row;
use crate::workloads::Internal;
use crate::Result;

use chrono::{DateTime, Utc};
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt::Write;
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

/////////////////////////////////////
/// Stored Procedures. ///
/////////////////////////////////////

/// GetSubscriberData transaction.
pub fn get_subscriber_data(
    params: GetSubscriberData,
    t_id: &str,
    t_ts: DateTime<Utc>,
    scheduler: Arc<Scheduler>,
) -> Result<String> {
    info!(
        "SELECT s_id, sub_nbr,
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
    let pk = params.s_id;
    // Register with scheduler.
    scheduler.register(t_id).unwrap();
    // Execute read operation.
    let values = scheduler.read("sub_idx", pk, &columns, t_id, t_ts)?;
    // Commit transaction.
    scheduler.commit(t_id);
    // Convert to result
    let res = datatype::to_result(&columns, &values)?;

    Ok(res)
}

pub fn get_new_destination(
    params: GetNewDestination,
    t_id: &str,
    t_ts: DateTime<Utc>,
    scheduler: Arc<Scheduler>,
) -> Result<String> {
    info!(
        "SELECT cf.numberx
           FROM Special_Facility AS sf, Call_Forwarding AS cf
           WHERE
             (sf.s_id = {} AND sf.sf_type = {} AND sf.is_active = 1)
             AND
             (cf.s_id = {} AND cf.sf_type = {})
             AND
            (cf.start_time <= {} AND  cf.end_time > {});",
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
    // Construct PK
    let sf_pk = helper::special_facility_key(params.s_id.into(), params.sf_type.into(), 1);
    let cf_pk = helper::call_forwarding_key(
        params.s_id.into(),
        params.sf_type.into(),
        params.start_time.into(),
    );
    // Register with scheduler.
    scheduler.register(t_id).unwrap();
    // Execute read operations.
    let sf_res = scheduler.read("special_idx", sf_pk, &sf_columns, t_id, t_ts)?;
    let cf_res = scheduler.read("call_idx", cf_pk, &cf_columns, t_id, t_ts)?;

    // LOGIC
    let is_active = match sf_res[2] {
        Data::Int(val) => val as u8,
        _ => panic!("Mismatch"),
    };

    let start_time = match cf_res[2] {
        Data::Int(val) => val as u8,
        _ => panic!("Mismatch"),
    };

    let end_time = match cf_res[3] {
        Data::Int(val) => val as u8,
        _ => panic!("Mismatch"),
    };

    if is_active == 1 && start_time <= params.start_time && end_time > params.end_time {
        let mut res: String;
        res = "[".to_string();
        write!(res, "{}={}]", cf_columns[4], cf_res[4])?;
        scheduler.commit(t_id);

        Ok(res)
    } else {
        Err(Box::new(SpaghettiError::RowDoesNotExist))
    }
}

/// GetAccessData transaction.
pub fn get_access_data(
    params: GetAccessData,
    t_id: &str,
    t_ts: DateTime<Utc>,
    scheduler: Arc<Scheduler>,
) -> Result<String> {
    info!(
        "SELECT data1, data2, data3, data4
           FROM Access_Info
         WHERE s_id = {:?}
           AND ai_type = {:?} ",
        params.s_id, params.ai_type
    );

    // Columns to read.
    let columns: Vec<&str> = vec!["data_1", "data_2", "data_3", "data_4"];
    // Construct primary key.
    let pk = helper::access_info_key(params.s_id, params.ai_type.into());
    debug!("{}", pk);
    // Register with scheduler.
    scheduler.register(t_id).unwrap();
    // Execute read operation.
    let values = scheduler.read("access_idx", pk, &columns, t_id, t_ts)?;
    // Commit transaction.
    scheduler.commit(t_id);
    // Convert to result
    let res = datatype::to_result(&columns, &values)?;

    Ok(res)
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

        let transaction = match n {
            x if x < 0.01 => {
                // x if x < 0.35 => {
                // GET_SUBSCRIBER_DATA
                // TODO: implement non-uniform distribution.
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let payload = GetSubscriberData { s_id };
                TatpTransaction::GetSubscriberData(payload)
            }
            _ => {
                // x if x < 0.45 => {
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
            } // x if x < 0.8 => {
              //     // GET_ACCESS_DATA
              //     let s_id = self.rng.gen_range(1..=self.subscribers);
              //     let ai_type = self.rng.gen_range(1..=4);
              //     let payload = GetAccessData { s_id, ai_type };
              //     TatpTransaction::GetAccessData(payload)
              // }
              // x if x < 0.82 => {
              //     // UPDATE_SUBSCRIBER_DATA
              //     let s_id = self.rng.gen_range(1..=self.subscribers);
              //     let sf_type = self.rng.gen_range(1..=4);
              //     let bit_1 = self.rng.gen_range(0..=1);
              //     let data_a = self.rng.gen_range(0..=255);
              //     let payload = UpdateSubscriberData {
              //         s_id,
              //         sf_type,
              //         bit_1,
              //         data_a,
              //     };
              //     TatpTransaction::UpdateSubscriberData(payload)
              // }
              // x if x < 0.96 => {
              //     // UPDATE_LOCATION
              //     let s_id = self.rng.gen_range(1..=self.subscribers);
              //     let vlr_location = self.rng.gen_range(1..=2 ^ 32 - 1);
              //     let payload = UpdateLocationData { s_id, vlr_location };
              //     TatpTransaction::UpdateLocationData(payload)
              // }
              // x if x < 0.98 => {
              //     // INSERT CALL_FORWARDING
              //     let s_id = self.rng.gen_range(1..=self.subscribers);
              //     let sf_type = self.rng.gen_range(1..=4);
              //     let start_time = helper::get_start_time(&mut self.rng);
              //     let end_time = self.rng.gen_range(1..=24);
              //     let number_x = helper::get_number_x(&mut self.rng);
              //     let payload = InsertCallForwarding {
              //         s_id,
              //         sf_type,
              //         start_time,
              //         end_time,
              //         number_x,
              //     };
              //     TatpTransaction::InsertCallForwarding(payload)
              // }
              // _ => {
              //     // DELETE_CALL_FORWARDING
              //     let s_id = self.rng.gen_range(1..=self.subscribers);
              //     let sf_type = self.rng.gen_range(1..=4);
              //     let start_time = helper::get_start_time(&mut self.rng);
              //     let payload = DeleteCallForwarding {
              //         s_id,
              //         sf_type,
              //         start_time,
              //     };
              //     TatpTransaction::DeleteCallForwarding(payload)
              // }
        };

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
    use crate::workloads::Workload;
    use config::Config;
    use lazy_static::lazy_static;
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
    fn populate_tables_test() {
        logging(false);
        let c = Arc::clone(&CONFIG);
        let internals = Internal::new("tatp_schema.txt", c).unwrap();
        let mut rng = StdRng::seed_from_u64(42);

        // Subscriber.
        populate_subscriber_table(&internals, &mut rng).unwrap();
        assert_eq!(
            internals.get_table("subscriber").unwrap().get_next_row_id(),
            1
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
            4
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
            4
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
            9
        );
        let cols_cf = vec!["s_id", "sf_type", "start_time", "end_time", "number_x"];
        let index = internals.indexes.get("call_idx").unwrap();
        assert_eq!(
            datatype::to_result(&cols_cf, &index.index_read(121, &cols_cf).unwrap()).unwrap(),
            "[s_id=1, sf_type=2, start_time=0, end_time=5, number_x=707677987012384]"
        );
    }

    #[test]
    fn transactions_test() {
        logging(false);
        // Workload with fixed seed
        let mut rng = StdRng::seed_from_u64(42);
        let config = Arc::clone(&CONFIG);
        let internals = Internal::new("tatp_schema.txt", config).unwrap();
        populate_tables(&internals, &mut rng).unwrap();
        let workload = Arc::new(Workload::Tatp(internals));
        // Scheduler
        let scheduler = Arc::new(Scheduler::new(workload));
        let sys_time = SystemTime::now();
        let datetime: DateTime<Utc> = sys_time.into();
        let t_id = datetime.to_string();
        let t_ts = datetime;
        // params
        let params_gsd = GetSubscriberData { s_id: 1 };
        let params_gns = GetNewDestination {
            s_id: 1,
            sf_type: 1,
            start_time: 0,
            end_time: 1,
        };
        let params_gad = GetAccessData {
            s_id: 1,
            ai_type: 2,
        };

        assert_eq!(
            get_subscriber_data(params_gsd, &t_id, t_ts, Arc::clone(&scheduler)).unwrap(),
            "[s_id=1, sub_nbr=000000000000001, bit_1=0, bit_2=1, bit_3=0, bit_4=1, bit_5=1, bit_6=1, bit_7=0, bit_8=0, bit_9=1, bit_10=0, hex_1=8, hex_2=6, hex_3=10, hex_4=8, hex_5=2, hex_6=13, hex_7=8, hex_8=10, hex_9=1, hex_10=9, byte_2_1=222, byte_2_2=248, byte_2_3=210, byte_2_4=100, byte_2_5=205, byte_2_6=163, byte_2_7=118, byte_2_8=127, byte_2_9=77, byte_2_10=52, msc_location=16]"
        );
        let t_ts: DateTime<Utc> = sys_time.into();
        assert_eq!(
            get_new_destination(params_gns, &t_id, t_ts, Arc::clone(&scheduler)).unwrap(),
            "[number_x=287996544237071]"
        );
        let t_ts: DateTime<Utc> = sys_time.into();
        assert_eq!(
            get_access_data(params_gad, &t_id, t_ts, Arc::clone(&scheduler)).unwrap(),
            "[data_1=63, data_2=7, data_3=EMZ, data_4=WOVGK]"
        );
    }
}
