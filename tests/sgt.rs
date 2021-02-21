// use config::Config;
// use rand::rngs::StdRng;
// use rand::SeedableRng;
// use spaghetti::common::message::Request;
// use spaghetti::server::manager::TransactionManager;
// use spaghetti::workloads::tatp;
// use spaghetti::workloads::{Internal, Workload};
// use std::sync::mpsc::{Receiver, Sender};
// use std::sync::Arc;
// use tracing::Level;
// use tracing_subscriber::FmtSubscriber;

// fn setup_logging(settings: Arc<Config>) {
//     let level = match settings.get_str("log").unwrap().as_str() {
//         "info" => Level::INFO,
//         "debug" => Level::DEBUG,
//         "trace" => Level::TRACE,
//         _ => Level::WARN,
//     };
//     let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
//     tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
// }

// fn setup_config() -> Arc<Config> {
//     // Initialise configuration.
//     let mut c = Config::default();
//     // Load from test file.
//     c.merge(config::File::with_name("Sgt.toml")).unwrap();
//     Arc::new(c)
// }

// // #[test]
// fn run() {
//     let config = setup_config();
//     let c1 = Arc::clone(&config);
//     let c2 = Arc::clone(&config);
//     setup_logging(config);

//     use std::thread;

//     let server_handle = std::thread::spawn(move || {
//         tokio_test::block_on(spaghetti::server::run(c1));
//     });

//     thread::sleep_ms(3000);

//     let client_handle = std::thread::spawn(move || {
//         tokio_test::block_on(spaghetti::client::run(c2));
//     });

//     client_handle.join();
//     server_handle.join();
// }
