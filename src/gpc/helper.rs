use crate::server::scheduler::Protocol;
use crate::workloads::Workload;

use config::Config;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

/// Initialise configuration.
pub fn init_config(file: &str) -> Arc<Config> {
    tracing::info!("initialise configuration using {}", file);
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file)).unwrap();
    Arc::new(settings)
}

/// Set logging level.
pub fn set_log_level(config: Arc<Config>) {
    let level = match config.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

/// Initialise database.
pub fn init_database(config: Arc<Config>) -> Arc<Workload> {
    let workload = Arc::new(Workload::new(Arc::clone(&config)).unwrap());
    let mut rng: StdRng = SeedableRng::from_entropy();
    workload.populate_tables(&mut rng).unwrap();
    workload
}

/// Initialise the scheduler with a desired number of cores.
pub fn init_scheduler(workload: Arc<Workload>, cores: usize) -> Arc<Protocol> {
    Arc::new(Protocol::new(Arc::clone(&workload), cores).unwrap())
}
