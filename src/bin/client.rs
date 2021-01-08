// /// A dedicated task manages the client resource. Tasks then send messages this manager, which handles sending the message and responds to the task with the message response.

use bytes::Bytes;
use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use server::connection::Connection;
use server::transaction::Transaction;
use tokio::io::BufWriter;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc; // to send commands to manager
use tokio::sync::oneshot; // to receive command response from manager

// establish a connection with the server
// generate the message to be sent
// convert into a frame
// write the message to the stream

pub struct Client {
    /// Tcp connection wrapped with read and write buffers.
    connection: Connection,
}

#[tokio::main]
async fn main() -> server::Result<()> {
    // establish a connection with the server
    let mut socket = TcpStream::connect("127.0.0.1:6142").await.unwrap();
    let connection = Connection::new(socket);
    let mut client = Client { connection };

    // generate the message to be sent
    let t = Transaction::GetSubscriberData { s_id: 10 };

    // write frame
    client.connection.write_frame();

    // convert to frame
    let f = t.into_frame();

    // Write data to stream in the background
    let write_task = tokio::spawn(async move {
        client.connection.write_frame(f);
    });
    write_task.await?;

    // Mpsc channel.
    // Produces send transactions to consumer.
    // Consumer manages client connection.
    // let (tx, mut rx) = mpsc::channel(32);

    // Spawn client resource manager task
    // let manager = tokio::spawn(async move {
    //     // open a connection to the server
    //     let client = Client::connect("127.0.0.1:6142");

    //     client.submit();
    //     // receive messages from producer
    //     // while let Some(cmd) = rx.recv().await {
    //     //     // receive transaction from producers
    //     //     let res = client.submit(param).await;
    //     //     // Ignore errors
    //     //     let _ = resp.send(res);
    //     // }
    // });
    Ok(())
}

// // spawn client resource manager task
// let manager = tokio::spawn(async move {

//         // open a connection to the spaghetti server
//         let mut client = client::connect("127.0.0.1:6142").await.unwrap();

//         // receive messages
//         while let Some(cmd) = rx.recv().await {
//             match cmd {
//                 Command::TransactionA { param, resp } => {
//                     let res = client.submit(param).await;
//                     // Ignore errors
//                     let _ = resp.send(res);
//                 }
//                 Command::TransactionB { param1, params2, resp } => {
//                     let res = client.submit(param1,param2).await;
//                     // Ignore errors
//                     let _ = resp.send(res);
//                 }
//             }
//         }
// });

//     // transaction senders
//     let tx2 = tx.clone();

//     let t1 = tokio::spawn(async move {
//         // spawn response channel
//         let (resp_tx, resp_rx) = oneshot::channel();

//         // generate transaction
//         let txn = Command::TransactionA {
//             param: "test".to_string(),
//             resp: resp_tx,
//         };

//         // send transaction to client resource manager
//         tx.send(txn).await.unwrap();

//         // await response
//         let res = resp_rx.await;
//         println!("GOT: {:?}",res);
//     });

//     let t2 = tokio::spawn(async move{
//         // spawn response channel
//         let (resp_tx, resp_rx) = oneshot::channel();

//         // generate transaction
//         let txn = Command::TransactionB {
//             param1: "test".to_string(),
//             param2: "bar".into(),
//             resp: resp_tx,
//         };

//         // send transaction to client resource manager
//         tx2.send(txn).await.unwrap();

//         // await response
//         let res = resp_rx.await;
//         println!("GOT: {:?}",res);
//     });

//     t1.await.unwrap();
//     t2.await.unwrap();
//     manager.await.unwrap();
//     Ok(())
// }
