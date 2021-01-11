//! The entry point for a Spaghetti client.
//! An mpsc channel is used to manage the client connection to the server.
//! Producers send transaction requests to the consumer which sends the over its TCP connection.
//! Producers send a oneshot channel with the request in order to receive the response to its request.
use spaghetti::client::Client;
use spaghetti::frame::Frame;
use spaghetti::transaction::{Command, Transaction};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> spaghetti::Result<()> {
    // All spans/events with a level higher than TRACE written to stdout.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Initialising client");

    let (tx, mut rx): (Sender<Command>, Receiver<Command>) = mpsc::channel(32);

    // Spawn client resource manager task.
    let manager = tokio::spawn(async move {
        // Open a connection to the server.
        let mut client = Client::connect("127.0.0.1:6142").await.unwrap();
        // Receive messages from producer.
        while let Some(cmd) = rx.recv().await {
            info!("Receive transaction from a producer");
            // Convert to frame.
            let frame = cmd.transaction.into_frame();
            // TODO: this should be a submit function that receives a response.
            info!("Submit message");
            let reply = client.submit(&frame).await;
            info!("Consumer received reply: {:?}", reply);
            // TODO: Ignore errors and send response back to producer, currently replies with unit struct
            let _ = cmd.resp.send(Ok(()));
        }
    });

    // transaction senders
    let tx2 = tx.clone();

    let t1 = tokio::spawn(async move {
        // spawn response channel
        let (resp_tx, resp_rx) = oneshot::channel();

        // generate transaction
        let t = Transaction::GetSubscriberData { s_id: 26 };
        // wrap in command
        let c = Command {
            transaction: t,
            resp: resp_tx,
        };

        // send transaction to client resource manager
        tx.send(c).await;

        // await response
        let res = resp_rx.await;
        info!("Producer 1 received reply: {:?}", res);
    });

    let t2 = tokio::spawn(async move {
        // spawn response channel
        let (resp_tx, resp_rx) = oneshot::channel();

        // generate transaction
        let t = Transaction::GetSubscriberData { s_id: 30 };
        // wrap in command
        let c = Command {
            transaction: t,
            resp: resp_tx,
        };

        // send transaction to client resource manager
        tx2.send(c).await;

        // await response
        let res = resp_rx.await;
        info!("Producer 2 received reply: {:?}", res);
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();

    Ok(())
}
