use crate::client::consumer::Consumer;
use crate::client::producer::Producer;
use crate::client::read_handler::ReadHandler;
use crate::client::write_handler::WriteHandler;
use crate::common::connection::{ReadConnection, WriteConnection};
use crate::common::message::Message;
use crate::Result;

use config::Config;
use std::sync::Arc;
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::info;

pub mod producer;

pub mod consumer;

pub mod write_handler;

pub mod read_handler;

/// Run `spaghetti` client.
pub async fn run(config: Arc<Config>) -> Result<()> {
    //// Shutdown channels. ////
    // `Producer` to `WriteHandler`.
    let (notify_wh_tx, listen_p_rx) = tokio::sync::mpsc::channel(1);
    // `ReadHandler` to `Consumer`
    let (notify_c_tx, listen_rh_rx) = tokio::sync::mpsc::channel(1);
    // `Consumer` to `Producer`
    let (notify_p_tx, listen_c_rx) = tokio::sync::mpsc::channel(1);

    //// Work channels. ////
    // `Producer` to `WriteHandler`.
    let mpsc_size = config.get_int("mpsc_size")?;
    let (write_task_tx, write_task_rx): (Sender<Message>, Receiver<Message>) =
        mpsc::channel(mpsc_size as usize);
    // `ReadHandler` to `Consumer`.
    let (read_task_tx, read_task_rx): (Sender<Message>, Receiver<Message>) =
        mpsc::channel(mpsc_size as usize);

    //// Producer ////
    let mut producer = Producer::new(
        Arc::clone(&config),
        write_task_tx,
        notify_wh_tx,
        listen_c_rx,
    )?;

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
    let wh = WriteHandler::new(w, write_task_rx, listen_p_rx);
    let whh = write_handler::run(wh);

    //// ReadHandler ////
    let rh = ReadHandler::new(r, read_task_tx, notify_c_tx);
    let rhh = read_handler::run(rh);

    //// Consumer ////
    let c = Consumer::new(read_task_rx, listen_rh_rx, notify_p_tx);
    let chh = consumer::run(c);

    // Run tasks.
    let (_p, _rh, _wh, _ch) = tokio::join!(producer.run(), rhh, whh, chh);

    // Drop shutdown channel to write handler.
    let Producer {
        notify_wh_tx,
        mut listen_c_rx,
        ..
    } = producer;
    drop(notify_wh_tx);
    // Wait until `Consumer` closes channel.
    listen_c_rx.recv().await;
    info!("Client shutdown");
    Ok(())
}
