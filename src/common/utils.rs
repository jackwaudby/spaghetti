use crate::common::message::Response;
use crate::storage::Database;
// use crate::workloads::acid::paramgen::{AcidGenerator, AcidTransactionProfile};
// use crate::workloads::dummy::paramgen::{DummyGenerator, DummyTransactionProfile};
// use crate::workloads::tatp::paramgen::{TatpGenerator, TatpTransactionProfile};
// use crate::workloads::ycsb::paramgen::{YcsbGenerator, YcsbTransactionProfil
// e};
// use crate::workloads::{acid, dummy, smallbank, tatp, ycsb};

use config::Config;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub fn init_config(file: &str) -> Config {
    info!("initialise configuration using {}", file);
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file)).unwrap();
    settings
}

pub fn set_log_level(config: &Config) {
    let level = match config.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };

    let file_appender =
        tracing_appender::rolling::hourly("/Users/jackwaudby/Documents/spaghetti", "prefix.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_thread_names(true)
        .with_target(false)
        .with_writer(non_blocking)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

pub fn create_log_dir(config: &Config) {
    let log_results = config.get_bool("log_results").unwrap();

    if log_results {
        let workload = config.get_str("workload").unwrap();
        let protocol = config.get_str("protocol").unwrap();

        let dir;
        if workload.as_str() == "acid" {
            let anomaly = config.get_str("anomaly").unwrap();
            dir = format!("./log/{}/{}/{}/", workload, protocol, anomaly);
        } else {
            dir = format!("./log/{}/{}/", workload, protocol);
        }

        if Path::new(&dir).exists() {
            fs::remove_dir_all(&dir).unwrap(); // if exists remove dir
        }

        fs::create_dir_all(&dir).unwrap(); // create directory
    }
}

pub fn init_database(config: &Config) -> Database {
    Database::new(config).unwrap()
}

pub fn append_to_log(fh: &mut Option<std::fs::File>, response: &Response) {
    if let Some(ref mut fh) = fh {
        let res = serde_json::to_string(response).unwrap();
        writeln!(fh, "{}", res).unwrap();
    }
}

pub fn spin(prv: u64, lsn: &AtomicU64) {
    let mut i = 0;
    while lsn.load(Ordering::Relaxed) != prv {
        i += 1;
        if i >= 10000 {
            std::thread::yield_now();
        }
    }
}

//    Transaction::Tatp(_) => {
//         if let Parameters::Tatp(params) = parameters {
//             match params {
//                 TatpTransactionProfile::GetSubscriberData(params) => {
//                     tatp::procedures::get_subscriber_data(
//                         params, scheduler, workload, isolation,
//                     )
//                 }
//                 TatpTransactionProfile::GetAccessData(params) => {
//                     tatp::procedures::get_access_data(params, scheduler, workload, isolation)
//                 }
//                 TatpTransactionProfile::GetNewDestination(params) => {
//                     tatp::procedures::get_new_destination(
//                         params, scheduler, workload, isolation,
//                     )
//                 }
//                 TatpTransactionProfile::UpdateLocationData(params) => {
//                     tatp::procedures::update_location(params, scheduler, workload, isolation)
//                 }
//                 TatpTransactionProfile::UpdateSubscriberData(params) => {
//                     tatp::procedures::update_subscriber_data(
//                         params, scheduler, workload, isolation,
//                     )
//                 }
//             }
//         } else {
//             panic!("transaction type and parameters do not match");
//         }
//     }

//     Transaction::Dummy(_) => {
//         if let Parameters::Dummy(params) = parameters {
//             match params {
//                 DummyTransactionProfile::Read(_params) => {
//                     unimplemented!()
//                     // tatp::procedures::get_subscriber_data(
//                     //     params, scheduler, workload, isolation,
//                     // )
//                 }
//                 DummyTransactionProfile::ReadWrite(_params) => {
//                     unimplemented!()
//                     // tatp::procedures::get_subscriber_data(
//                     //     params, scheduler, workload, isolation,
//                     // )
//                 }
//                 DummyTransactionProfile::Write(params) => {
//                     dummy::procedures::write_only(params, scheduler, workload)
//                 }

//                 DummyTransactionProfile::WriteAbort(params) => {
//                     dummy::procedures::write_only_abort(params, scheduler, workload)
//                 }
//             }
//         } else {
//             panic!("transaction type and parameters do not match");
//         }
//     }

//     Transaction::Ycsb(_) => {
//         if let Parameters::Ycsb(params) = parameters {
//             match params {
//                 YcsbTransactionProfile::General(params) => {
//                     ycsb::procedures::transaction(params, scheduler, workload, isolation)
//                 }
//             }
//         } else {
//             panic!("transaction type and parameters do not match");
//         }
//     }

//     Transaction::Acid(_) => {
//         if let Parameters::Acid(params) = parameters {
//             match params {
//                 AcidTransactionProfile::G0Write(params) => {
//                     acid::procedures::g0_write(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::G0Read(params) => {
//                     acid::procedures::g0_read(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::G1aRead(params) => {
//                     acid::procedures::g1a_read(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::G1aWrite(params) => {
//                     acid::procedures::g1a_write(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::G1cReadWrite(params) => {
//                     acid::procedures::g1c_read_write(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::ImpRead(params) => {
//                     acid::procedures::imp_read(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::ImpWrite(params) => {
//                     acid::procedures::imp_write(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::OtvRead(params) => {
//                     acid::procedures::otv_read(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::OtvWrite(params) => {
//                     acid::procedures::otv_write(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::LostUpdateRead(params) => {
//                     acid::procedures::lu_read(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::LostUpdateWrite(params) => {
//                     acid::procedures::lu_write(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::G2itemRead(params) => {
//                     acid::procedures::g2_item_read(params, scheduler, workload)
//                 }
//                 AcidTransactionProfile::G2itemWrite(params) => {
//                     acid::procedures::g2_item_write(params, scheduler, workload)
//                 }
//             }
//         } else {
//             panic!("transaction type and parameters do not match");
//         }
//     }
//     Transaction::SmallBank(_) => {
//         if let Parameters::SmallBank(params) = parameters {
//             match params {
//                 SmallBankTransactionProfile::Amalgamate(params) => {
//                     smallbank::procedures::amalgmate(params, scheduler, workload, isolation)
//                 }
//                 SmallBankTransactionProfile::Balance(params) => {
//                     smallbank::procedures::balance(params, scheduler, workload, isolation)
//                 }
//                 SmallBankTransactionProfile::DepositChecking(params) => {
//                     smallbank::procedures::deposit_checking(
//                         params, scheduler, workload, isolation,
//                     )
//                 }
//                 SmallBankTransactionProfile::SendPayment(params) => {
//                     smallbank::procedures::send_payment(params, scheduler, workload, isolation)
//                 }
//                 SmallBankTransactionProfile::TransactSaving(params) => {
//                     smallbank::procedures::transact_savings(
//                         params, scheduler, workload, isolation,
//                     )
//                 }
//                 SmallBankTransactionProfile::WriteCheck(params) => {
//                     smallbank::procedures::write_check(params, scheduler, workload, isolation)
//                 }
//             }
//         } else {
//             panic!("transaction type and parameters do not match");
//         }
//     }
// };

pub fn create_transaction_log(config: &Config, core_id: usize) -> Option<File> {
    // assumes directory created by this point
    // let log_response = config.get_bool("log_results").unwrap();
    let log_response = true;

    if log_response {
        let workload = config.get_str("workload").unwrap();
        let protocol = config.get_str("protocol").unwrap();

        let dir;
        let file;
        if workload.as_str() == "acid" {
            let anomaly = config.get_str("anomaly").unwrap();
            dir = format!("./log/{}/{}/{}/", workload, protocol, anomaly);
            file = format!(
                "./log/acid/{}/{}/thread-{}.json",
                protocol, anomaly, core_id
            );
        } else {
            dir = format!("./log/{}/{}/", workload, protocol);
            file = format!("./log/{}/{}/thread-{}.json", workload, protocol, core_id);
        }

        if Path::new(&dir).exists() {
            if Path::new(&file).exists() {
                fs::remove_file(&file).unwrap(); // remove file if already exists
            }
        } else {
            panic!("dir should exist");
        }

        Some(
            OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(&file)
                .expect("cannot open file"),
        )
    } else {
        None
    }
}
