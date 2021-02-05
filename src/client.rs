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
use tokio::signal;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, info};

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

    //// ReadHandler ////
    let rh = ReadHandler::new(r, read_task_tx, notify_c_tx);

    //// Consumer ////
    let c = Consumer::new(read_task_rx, listen_rh_rx, notify_p_tx);

    // run the producer whilst listening for shutdown signal.
    let run = tokio::select! {
         res = async {
             let whh = write_handler::run(wh);
             let rhh = read_handler::run(rh);
             let chh = consumer::run(c);
             let (p, _rh, _wh, _ch) = tokio::join!(producer.run(), rhh, whh, chh);
             producer.wait().await;
             p
         } => res,
        _ = signal::ctrl_c() => {
            info!("Keyboard interrupt");
            // Handles case when client unexpectedly closed.

            // Drop shutdown channel to write handler.
            let Producer {
                mut generator,
                write_task_tx,
                notify_wh_tx,
                mut listen_c_rx,
                ..
            } = producer;

            info!("Generated {} transactions", generator.get_generated());

            // Send close connection message.
            let message = Message::CloseConnection;
            debug!("Send {:?}", message);
            write_task_tx.send(message).await?;

            // Notify write handler of shutdown.
            drop(notify_wh_tx);

            // Wait until `Consumer` closes channel.
            listen_c_rx.recv().await;
            Ok(())
        }
    };

    info!("Client shutdown");
    // Ok(())
    run
}
