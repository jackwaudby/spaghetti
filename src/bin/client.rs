//! The entry point for a Spaghetti client.
//! An mpsc channel is used to manage the client connection to the server.
//! Producers send transaction requests to the consumer which sends the over its TCP connection.
//! Producers send a oneshot channel with the request in order to receive the response to its request.
use spaghetti::client;

use clap::clap_app;
use config::Config;
use std::sync::Arc;

use tracing::{info, Level};
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
    // If configuration file is not provided use the default in `Settings.toml`
    let file = matches.value_of("config").unwrap_or("Settings.toml");
    let mut settings = Config::default();
    settings.merge(config::File::with_name(file)).unwrap();

    // For each flag overwrite default with any supplied runtime value.
    if let Some(p) = matches.value_of("port") {
        settings.set("port", p).unwrap();
    }

    if let Some(a) = matches.value_of("address") {
        settings.set("address", a).unwrap();
    }

    if let Some(l) = matches.value_of("log") {
        settings.set("log", l).unwrap();
    }

    let level = match settings.get_str("log").unwrap().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    };

    // All spans/events with a level higher than TRACE written to stdout.
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Initialising client");
    // Wrap configuration in atomic shared reference.
    // This is ok as configuration never changes at runtime.
    let config = Arc::new(settings);

    client::run(config).await;

    Ok(())
}
