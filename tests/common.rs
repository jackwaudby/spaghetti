use config::Config;
use std::sync::Arc;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

pub fn setup() -> Arc<Config> {

    let mut settings = Config::default();
    settings.merge(config::File::with_name("Test.toml")).unwrap();

    // Logger.
    let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG).finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Wrap configuration in atomic shared reference.
    // This is ok as configuration never changes at runtime.
    let config = Arc::new(settings);

    // Delete file.
    std::fs::remove_file("result.txt").expect("could not remove file");
    config
}
