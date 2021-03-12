use crate::client::consumer::Consumer;
use crate::client::producer::{GeneratorMode, Producer};
use crate::client::read_handler::ReadHandler;
use crate::client::write_handler::WriteHandler;
use crate::common::connection::{ReadConnection, WriteConnection};
use crate::common::message::Message;
use crate::Result;

use config::Config;
use std::sync::Arc;
use tokio::io;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, error, info};

pub mod producer;

pub mod consumer;

pub mod write_handler;

pub mod read_handler;

/// Run `spaghetti` client.
pub async fn run(config: Arc<Config>) -> Result<()> {
    //// Shutdown channels. ////
    // `Main` to `Producer`
    let (notify_p_tx, listen_m_rx) = tokio::sync::mpsc::channel(1);
    // `Consumer` to `Main`
    let (notify_m_tx, mut listen_c_rx) = tokio::sync::mpsc::channel(1);

    //// Work channels. ////
    // `Producer` to `WriteHandler`.
    let mpsc_size = config.get_int("mpsc_size")?;
    let (write_task_tx, write_task_rx): (Sender<Message>, Receiver<Message>) =
        mpsc::channel(mpsc_size as usize);
    // `ReadHandler` to `Producer` to notify request has received a response.
    let (received_tx, received_rx): (Sender<()>, Receiver<()>) = mpsc::channel(mpsc_size as usize);
    // `ReadHandler` to `Consumer`.
    let (read_task_tx, read_task_rx): (Sender<Message>, Receiver<Message>) =
        mpsc::channel(mpsc_size as usize);

    //// Producer ////
    let p = Producer::new(Arc::clone(&config), write_task_tx, listen_m_rx, received_rx)?;

    //// Consumer ////
    let c = Consumer::new(read_task_rx, notify_m_tx);

    //// Handlers ////
    // Open a connection to the server.
    let add = config.get_str("address")?;
    let port = config.get_str("port")?;
    let socket = TcpStream::connect(&format!("{}:{}", add, port)[..]).await?;
    info!("Client connected to server at {}:{}", add, port);
    // Split socket into reader and writer handlers.
    let (rd, wr) = io::split(socket);
    let w = WriteConnection::new(wr);
    let r = ReadConnection::new(rd);

    //// WriteHandler ////
    let wh = WriteHandler::new(w, write_task_rx);

    //// ReadHandler ////
    let rh = ReadHandler::new(r, read_task_tx, received_tx);

    // Join tasks and listen for shutdown.
    tokio::select! {
        res = async {
            // Spawn tasks
            let phh = producer::run(p);
            let whh = write_handler::run(wh);
            let rhh = read_handler::run(rh);
            let chh = consumer::run(c);
            // Wait on all tasks.
            let (p, rh, wh, ch) = tokio::join!(phh, rhh, whh, chh);
            p.unwrap_or_else(|error| {
                error!("Producer: {}", format!("{}", error));
            });
            rh.unwrap_or_else(|error| {
                error!("Read handler: {}", format!("{}", error));
            });
            wh.unwrap_or_else(|error| {
                error!("Write handler: {}", format!("{}", error));
            });
            ch.unwrap_or_else(|error| {
                error!("Consumer: {}", format!("{}", error));
            });
        } => res,
        _ = signal::ctrl_c() => {
            info!("Keyboard interrupt");

            debug!("Notify producer");
            drop(notify_p_tx);
            debug!("Wait for consumer to terminate");
            // Wait until `Consumer` closes channel.
            listen_c_rx.recv().await;
           debug!("Consumer terminated");
        }
    };

    info!("Client shutdown");
    Ok(())
}
