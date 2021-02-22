use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use spaghetti::common::message::Request;
use spaghetti::server::manager::TransactionManager;
use spaghetti::workloads::tatp;
use spaghetti::workloads::{Internal, Workload};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

fn setup_logging(settings: Arc<Config>) {
    let level = match settings.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn setup_config() -> Arc<Config> {
    // Initialise configuration.
    let mut c = Config::default();
    // Load from test file.
    c.merge(config::File::with_name("Test-sgt.toml")).unwrap();
    Arc::new(c)
}

#[tokio::test]
async fn sgt_integration_test() {
    // Configuration.
    let config = setup_config();
    // Logging.
    setup_logging(Arc::clone(&config));

    let c = Arc::clone(&config);
    let server = tokio::spawn(async move {
        assert_eq!((), spaghetti::server::run(c).await.unwrap());
    });

    sleep(Duration::from_millis(1000)).await;

    let mut clients = vec![];
    for _ in 0..3 {
        let c = Arc::clone(&config);
        let client = tokio::spawn(async move {
            assert_eq!((), spaghetti::client::run(c).await.unwrap());
        });
        clients.push(client);
    }

    for client in clients {
        assert_eq!(client.await.unwrap(), ());
    }

    assert_eq!(server.await.unwrap(), ());
}
