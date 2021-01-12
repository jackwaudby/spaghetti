use crate::connection::Connection;
use crate::frame::Frame;
use crate::transaction::NewOrderParams;
use crate::transaction::{Command, Transaction};
use crate::Result;

use config::Config;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, info};

/// Runs the client.
pub async fn run(conf: Arc<Config>) -> Result<()> {
    // Mpsc.
    let mpsc_size = conf.get_int("mpsc_size").unwrap();
    let (tx, mut rx): (Sender<Command>, Receiver<Command>) = mpsc::channel(mpsc_size as usize);

    // Spawn client resource manager task.
    let add = conf.get_str("address").unwrap();
    let port = conf.get_str("port").unwrap();
    let manager = tokio::spawn(async move {
        // Open a connection to the server.
        let mut client = Client::connect(&format!("{}:{}", add, port)[..])
            .await
            .unwrap();
        // Receive messages from producer.
        while let Some(cmd) = rx.recv().await {
            debug!("Receive transaction from a producer");
            // Convert to frame.
            let frame = cmd.transaction.into_frame();
            // TODO: this should be a submit function that receives a response.
            info!("Send message: {:?}", frame);
            let reply = client.submit(&frame).await;
            info!("Consumer received reply: {:?}", reply);
            // TODO: Ignore errors and send response back to producer, currently replies with unit struct
            let _ = cmd.resp.send(Ok(()));
        }
    });

    // Producers create transactions.
    let _prod = conf.get_int("producers").unwrap(); // fixed at 1.
    let w = conf.get_int("districts").unwrap() as u64;
    let d = conf.get_int("warehouses").unwrap() as u64;
    let mut rng = rand::thread_rng();
    let mut handles = vec![];

    let params = NewOrderParams::new(1, w, d, &mut rng);

    // for i in 0..prod {
    // Transmitter end of mpsc.
    let tx2 = tx.clone();

    let handle = tokio::spawn(async move {
        // Spawn response channel
        let (resp_tx, resp_rx) = oneshot::channel();

        // Generate transaction
        // let t = Transaction::GetSubscriberData { s_id: 30 };
        let t = Transaction::NewOrder(params);
        // wrap in command
        let c = Command {
            transaction: t,
            resp: resp_tx,
        };

        // Send transaction to client resource manager
        tx2.send(c).await;

        // Await response
        let res = resp_rx.await;
        info!("Producer received reply: {:?}", res);
    });

    handles.push(handle);
    //}

    for handle in handles {
        handle.await.unwrap();
    }

    manager.await.unwrap();

    Ok(())
}

pub struct Client {
    /// Tcp connection wrapped with read and write buffers.
    pub connection: Connection,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Client> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket);
        Ok(Client { connection })
    }
    pub async fn submit(&mut self, frame: &Frame) -> Result<Option<Frame>> {
        // Write request to the stream.
        self.connection.write_frame(frame).await?;
        // TODO: Receive response.
        self.connection.read_frame().await
        // let rframe = match res {
        //     Some(rframe) => rframe,
        //     None => , // handle this better
        // };

        // Ok(rframe)
    }
}
