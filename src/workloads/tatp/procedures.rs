use crate::common::error::SpaghettiError;
use crate::server::scheduler::Scheduler;
use crate::server::storage::datatype::{self, Data};
use crate::workloads::tatp::helper;
use crate::workloads::tatp::profiles::{
    DeleteCallForwarding, GetAccessData, GetNewDestination, GetSubscriberData,
    InsertCallForwarding, TatpTransaction, UpdateLocationData, UpdateSubscriberData,
};
use crate::Result;

use chrono::{DateTime, Utc};
use std::fmt::Write;
use std::sync::Arc;
use tracing::{debug, info};

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

/// GetNewDestination transaction.
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

pub fn update_subscriber_data(
    params: UpdateSubscriberData,
    t_id: &str,
    t_ts: DateTime<Utc>,
    scheduler: Arc<Scheduler>,
) -> Result<String> {
    info!(
        "UPDATE Subscriber
           SET bit_1 = {:?}
           WHERE s_id = {:?}
         UPDATE Special_Facility
           SET data_a = {:?}
           WHERE s_id = {:?}
             AND sf_type = {:?};",
        params.bit_1, params.s_id, params.data_a, params.s_id, params.sf_type
    );

    // Columns to write.
    let columns_sb: Vec<&str> = vec!["bit_1"];
    let columns_sp = vec!["data_a"];
    // Values to write.
    let values_sb = vec![params.bit_1.to_string()];
    let values_sb: Vec<&str> = values_sb.iter().map(|s| s as &str).collect();
    let values_sp = vec![params.data_a.to_string()];
    let values_sp: Vec<&str> = values_sp.iter().map(|s| s as &str).collect();

    // Construct primary key.
    let pk_sb = params.s_id;
    // TODO: change special facility pk.
    let pk_sp = helper::special_facility_key(params.s_id, params.sf_type.into(), 1);

    // Register with scheduler.
    scheduler.register(t_id).unwrap();

    // Execute write operation.
    scheduler.write("sub_idx", pk_sb, &columns_sb, &values_sb, t_id, t_ts)?;
    scheduler.write("special_idx", pk_sp, &columns_sp, &values_sp, t_id, t_ts)?;

    // Commit transaction.
    scheduler.commit(t_id);

    Ok("updated".to_string())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::workloads::tatp::loader;
    use crate::workloads::{Internal, Workload};
    use config::Config;
    use lazy_static::lazy_static;
    use rand::rngs::StdRng;
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
    fn transactions_test() {
        logging(false);
        // Workload with fixed seed
        let mut rng = StdRng::seed_from_u64(42);
        let config = Arc::clone(&CONFIG);
        let internals = Internal::new("tatp_schema.txt", config).unwrap();
        loader::populate_tables(&internals, &mut rng).unwrap();
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
        let params_usd = UpdateSubscriberData {
            s_id: 1,
            sf_type: 1,
            bit_1: 1,
            data_a: 29,
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
        let t_ts: DateTime<Utc> = sys_time.into();

        assert_eq!(
            update_subscriber_data(params_usd, &t_id, t_ts, Arc::clone(&scheduler)).unwrap(),
            "updated"
        );

        assert_eq!(
            get_subscriber_data(params_gsd, &t_id, t_ts, Arc::clone(&scheduler)).unwrap(),
            "[s_id=1, sub_nbr=000000000000001, bit_1=1, bit_2=1, bit_3=0, bit_4=1, bit_5=1, bit_6=1, bit_7=0, bit_8=0, bit_9=1, bit_10=0, hex_1=8, hex_2=6, hex_3=10, hex_4=8, hex_5=2, hex_6=13, hex_7=8, hex_8=10, hex_9=1, hex_10=9, byte_2_1=222, byte_2_2=248, byte_2_3=210, byte_2_4=100, byte_2_5=205, byte_2_6=163, byte_2_7=118, byte_2_8=127, byte_2_9=77, byte_2_10=52, msc_location=16]"
        );

        // TODO: check other value has changed.
    }
}
