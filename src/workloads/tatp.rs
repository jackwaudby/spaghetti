use crate::frame::Frame;
use crate::parameter_generation::Generator;
use crate::storage::row::Row;
use crate::transaction::Transaction;
use crate::workloads::Internal;
use crate::workloads::Workload;

use bytes::Bytes;
use rand::rngs::ThreadRng;
use rand::seq::IteratorRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::any::Any;

use std::sync::Arc;
use tracing::{debug, info};

pub mod helper;

//////////////////////////////
/// Table Loaders. ///
//////////////////////////////

/// Populate tables.
pub fn populate_tables(data: &Internal, rng: &mut ThreadRng) {
    populate_subscriber_table(data, rng);
    populate_access_info(data, rng);
}

/// Populate the `Subscriber` table.
///
/// Schema:
/// Primary key: s_id
pub fn populate_subscriber_table(data: &Internal, rng: &mut ThreadRng) {
    info!("Loading subscriber table");
    let s_name = String::from("subscriber");
    let t = data.tables.get(&s_name).unwrap();
    let i_name = t.get_primary_index().unwrap();
    let i = data.indexes.get(&i_name).unwrap();

    let subs = data.config.get_int("subscribers").unwrap() as u64;
    for s_id in 0..subs {
        let mut row = Row::new(Arc::clone(&t));
        row.set_primary_key(s_id);
        row.set_value("s_id", s_id.to_string());
        row.set_value("sub_nbr", helper::to_sub_nbr(s_id));
        for i in 1..10 {
            row.set_value(
                format!("bit_{}", i).as_str(),
                rng.gen_range(0..=1).to_string(),
            );
            row.set_value(
                format!("hex_{}", i).as_str(),
                rng.gen_range(0..=15).to_string(),
            );
            row.set_value(
                format!("byte_2_{}", i).as_str(),
                rng.gen_range(0..=255).to_string(),
            );
        }
        row.set_value("msc_location", rng.gen_range(1..=2 ^ 32 - 1).to_string());
        row.set_value("vlr_location", rng.gen_range(1..=2 ^ 32 - 1).to_string());
        debug!("{}", row);
        i.index_insert(s_id, row);
    }
    info!("Loaded: {} rows", t.get_next_row_id());
}

/// Populate the `AccessInfo` table.
///
/// Schema: (int,s_id) (int,ai_type) (int,data1) (int,data2) (string,data3) (string,data4)
/// Primary key: (s_id, ai_type)
pub fn populate_access_info(data: &Internal, rng: &mut ThreadRng) {
    info!("Loading access_info table");
    // Get handle to `Table` and `Index`.
    let ai_name = String::from("access_info");
    let t = data.tables.get(&ai_name).unwrap();
    let i_name = t.get_primary_index().unwrap();
    let i = data.indexes.get(&i_name).unwrap();

    // Range of values for ai_type records.
    let ai_type_values = vec![1, 2, 3, 4];
    let subscribers = data.config.get_int("subscribers").unwrap() as u64;
    for s_id in 0..subscribers {
        // Generate number of records for a given s_id.
        let n_ai = rng.gen_range(1..=4);
        // Randomly sample w.o. replacement from range of ai_type values.
        let sample = ai_type_values.iter().choose_multiple(rng, n_ai);
        for record in 1..n_ai {
            // Initialise empty row.
            let mut row = Row::new(Arc::clone(&t));
            // Calculate primary key
            let pk = helper::access_info_key(s_id, *sample[record]);
            row.set_primary_key(pk);
            row.set_value("s_id", s_id.to_string());
            row.set_value("ai_type", sample[record].to_string());
            row.set_value("data_1", rng.gen_range(0..=255).to_string());
            row.set_value("data_2", rng.gen_range(0..=255).to_string());
            row.set_value("data_3", helper::get_data_x(3, rng));
            row.set_value("data_4", helper::get_data_x(5, rng));
            debug!("{}", row);
            i.index_insert(pk, row);
        }
    }
    info!("Loaded: {} rows", t.get_next_row_id());
}

/////////////////////////////////////
/// Stored Procedures. ///
/////////////////////////////////////

pub fn get_subscriber_data(s_id: u64, workload: Arc<Workload>) -> String {
    info!(
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

    match *workload {
        Workload::Tatp(ref internals) => {
            let key = s_id;
            let index = internals.indexes.get("sub_idx").unwrap();
            let row = index.index_read(key).unwrap();
            row.get_value("sub_nbr".to_string()).unwrap()
        }
        Workload::Tpcc(ref internals) => String::from("test"),
    }
}

/////////////////////////////////////////
/// Parameter Generator. ///
////////////////////////////////////////

pub struct TatpGenerator {
    subscribers: u64,
    rng: ThreadRng,
}

impl TatpGenerator {
    pub fn new(subscribers: u64) -> TatpGenerator {
        // Initialise the thread local rng.
        let rng = rand::thread_rng();
        TatpGenerator { subscribers, rng }
    }
}

impl Generator<TatpTransaction> for TatpGenerator {
    fn generate(&mut self) -> Box<TatpTransaction> {
        let n: f32 = self.rng.gen();

        let transaction = match n {
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
            _ => {
                // GET_ACCESS_DATA
                // TODO: should be 0.80
                let s_id = self.rng.gen_range(1..=self.subscribers);
                let ai_type = self.rng.gen_range(1..=4);
                let payload = GetAccessData { s_id, ai_type };
                TatpTransaction::GetAccessData(payload)
            } // x if x < 0.82 {
              //     // UPDATE_SUBSCRIBER_DATA
              // },
              // x if x < 0.96 {
              //     // UPDATE_LOCATION
              // },
              // x if x < 0.98 {
              //       // INSERT_CALL_FORWARDING
              // }
              // x if x < 1.0 {
              //     // DELETE_CALL_FORWARDING
              // }
        };
        Box::new(transaction)
    }
}

///////////////////////////////////////
/// Transaction Profiles. ///
//////////////////////////////////////
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GetSubscriberData {
    s_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GetNewDestination {
    s_id: u64,
    sf_type: u8,
    start_time: u8,
    end_time: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GetAccessData {
    s_id: u64,
    ai_type: u8,
}

// #[derive(Serialize, Deserialize, PartialEq, Debug)]
// pub struct UpdateSubscriberData {
//     s_id: u64,
//     sf_type: u8,
//     bit_1: u8,
//     data_a: u
// }

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum TatpTransaction {
    GetSubscriberData(GetSubscriberData),
    GetNewDestination(GetNewDestination),
    GetAccessData(GetAccessData),
    // UpdateSubscriberData(UpdateSubscriberData),
}

impl Transaction for TatpTransaction {
    fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn to_sub_nbr_test() {
        let s_id = 40958;
        let sub_nbr = to_sub_nbr(s_id);
        info!("{:?}", sub_nbr.to_string().len());
        assert_eq!(sub_nbr.len(), 15);
    }
}
