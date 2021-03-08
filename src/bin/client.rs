//! Entry point for a `spaghetti` client.
use clap::clap_app;
use config::Config;
use spaghetti::client;
use std::sync::Arc;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> spaghetti::Result<()> {
    // Read command line arguments using clap macro.
    let matches = clap_app!( spag =>
                             (version: "0.1.0")
                             (author: "J. Waudby <j.waudby2@newcastle.ac.uk>")
                             (about: "Spaghetti client")
                             (@arg FILE: -c --config +takes_value "Set a custom config file")
                             (@arg PORT: -p --port +takes_value "Set port server is listening on")
                             (@arg ADDRESS: -a --address +takes_value "Set server address")
                             (@arg LOG: -l --log +takes_value "Set log level")

    )
    .get_matches();

    // Initialise configuration.
    let file = matches.value_of("config").unwrap_or("Client.toml");
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file))?;

    // For each flag overwrite default with any supplied runtime value.
    if let Some(p) = matches.value_of("port") {
        settings.set("port", p)?;
    }

    if let Some(a) = matches.value_of("address") {
        settings.set("address", a)?;
    }

    if let Some(l) = matches.value_of("log") {
        settings.set("log", l)?;
    }

    let level = match settings.get_str("log")?.as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };

    // Initialise logging.
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Initialising client");
    // Wrap configuration in atomic shared reference.
    // Ok as configuration never changes at runtime.
    let config = Arc::new(settings);

    client::run(config).await.unwrap_or_else(|error| {
        error!("{:?}", error);
    });

    Ok(())
}
