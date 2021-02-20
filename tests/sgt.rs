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

// TODO: move to unit test for sgt.

// fn setup_workload() -> Arc<Workload> {
//     // Initialise configuration.
//     let mut c = Config::default();
//     // Load from test file.
//     c.merge(config::File::with_name("Sgt.toml")).unwrap();
//     let config = Arc::new(c);
//     // Rng with fixed seed.
//     let mut rng = StdRng::seed_from_u64(42);
//     // Initialise internals.
//     let internals = Internal::new("tatp_schema.txt", config).unwrap();
//     // Load tables.
//     tatp::loader::populate_tables(&internals, &mut rng).unwrap();
//     // Create workload.
//     Arc::new(Workload::Tatp(internals))
// }

// fn commit_abort() {
//     // Initialise workload.
//     let workload = setup_workload();

//     // Shutdown channels.
//     let (notify_tm_tx, tm_shutdown_rx) = std::sync::mpsc::channel();
//     let (notify_wh_tx, _) = tokio::sync::broadcast::channel(1);

//     // Work channels.
//     let (_, work_rx): (Sender<Request>, Receiver<Request>) = std::sync::mpsc::channel();

//     // Create transaction manager.
//     let tm = TransactionManager::new(
//         Arc::clone(&workload),
//         work_rx,
//         tm_shutdown_rx,
//         notify_wh_tx.clone(),
//     );

//     // assert_eq!(tm.get_pool().size(), 4);

//     let scheduler1 = Arc::clone(&tm.get_scheduler());
//     let scheduler2 = Arc::clone(&tm.get_scheduler());
//     tm.get_pool().execute(move || {
//         assert_eq!(
//             format!(
//                 "{}",
//                 tatp::procedures::get_new_destination(
//                     tatp::profiles::GetNewDestination {
//                         s_id: 10,
//                         sf_type: 1,
//                         start_time: 0,
//                         end_time: 1,
//                     },
//                     scheduler1
//                 )
//                 .unwrap_err()
//             ),
//             format!("Aborted: row does not exist in index.")
//         );
//     });

//     tm.get_pool().execute(move || {
//         assert_eq!(
//             tatp::procedures::get_new_destination(
//                 tatp::profiles::GetNewDestination {
//                     s_id: 1,
//                     sf_type: 1,
//                     start_time: 8,
//                     end_time: 12,
//                 },
//                 scheduler2
//             )
//             .unwrap(),
//             "{number_x=\"993245295996111\"}"
//         );
//     });

//     drop(notify_tm_tx);
// }
