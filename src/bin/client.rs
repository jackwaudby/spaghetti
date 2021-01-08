/// A dedicated task manages the client resource. Tasks then send messages this manager, which handles sending the message and responds to the task with the message response.
use spaghetti::client::Client;
use spaghetti::frame::Frame;
use spaghetti::transaction::{Command, Transaction};

use tokio::sync::mpsc::{self, Receiver, Sender}; // to send commands to manager
use tokio::sync::oneshot; // to receive command response from manager

#[tokio::main]
async fn main() -> spaghetti::Result<()> {
    // Produces send transactions to consumer.
    // Consumer manages client connection.
    let (tx, mut rx): (Sender<Command>, Receiver<Command>) = mpsc::channel(32);

    // Spawn client resource manager task
    let manager = tokio::spawn(async move {
        // open a connection to the server
        let mut client = Client::connect("127.0.0.1:6142").await.unwrap();
        // receive messages from producer
        while let Some(cmd) = rx.recv().await {
            // receive transaction from producers
            // convert to frame
            let frame = cmd.transaction.into_frame();
            // TODO: this should be a submit function that receives a response.
            client.connection.write_frame(&frame).await;
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
        println!("GOT: {:?}", res);
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
        println!("GOT: {:?}", res);
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();

    Ok(())
}
