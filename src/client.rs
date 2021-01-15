use crate::connection::Connection;
use crate::connection::ReadConnection;
use crate::connection::WriteConnection;
use crate::frame::Frame;
use crate::parameter_generation::ParameterGenerator;
use crate::transaction::Transaction;
use crate::workloads::tatp::{TatpGenerator, TatpTransaction};
use crate::workloads::tpcc::TpccGenerator;
use crate::Result;

use config::Config;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, info};

/// Runs the client.
pub async fn run(conf: Arc<Config>) -> Result<()> {
    // Open a connection to the server.
    let add = conf.get_str("address").unwrap();
    let port = conf.get_str("port").unwrap();
    let socket = TcpStream::connect(&format!("{}:{}", add, port)[..]).await?;
    info!("Client connected to server at {}:{}", add, port);

    // Split socket into reader and writer handlers.
    let (mut rd, mut wr) = io::split(socket);
    let mut w = WriteConnection::new(wr);
    let mut r = ReadConnection::new(rd);

    // Initialise the mpsc.
    let mpsc_size = conf.get_int("mpsc_size").unwrap();
    let (tx, mut rx): (
        Sender<Box<dyn Transaction + Send>>,
        Receiver<Box<dyn Transaction + Send>>,
    ) = mpsc::channel(mpsc_size as usize);
    // Spawn task to manage writer.
    let w_manager = tokio::spawn(async move {
        // Receive messages from producer.
        while let Some(transaction) = rx.recv().await {
            // debug!(
            //     "Write half received transaction {:?} from generator",
            //     transaction
            // );
            // Convert to frame.
            let frame = transaction.into_frame();
            w.write_frame(&frame).await;
            debug!("Message written to socket");
        }
        info!("Closing write connection");
    });

    // Initialise parameter generator.
    let workload = conf.get_str("workload").unwrap();
    let mut pg = match workload.as_str() {
        "tatp" => {
            let subscribers = conf.get_int("subscribers").unwrap();
            let gen = TatpGenerator::new(subscribers as u64);
            ParameterGenerator::Tatp(gen)
        }
        "tpcc" => {
            let tpcc_gen = TpccGenerator {
                warehouses: 10,
                districts: 10,
            };
            ParameterGenerator::Tpcc(tpcc_gen)
        }
        _ => unimplemented!(),
    };

    // Spawn producer that sends transaction to writer.
    let producer = tokio::spawn(async move {
        info!("Generating transactions");
        // Generate a transaction
        let transaction = pg.get_transaction();

        // Verify it is a tatp trasaction.
        let t: &TatpTransaction = transaction
            .as_any()
            .downcast_ref::<TatpTransaction>()
            .expect("not a TatpTransaction");
        info!("Generated: {:?}", t);

        // Send transaction to write connection.
        tx.send(transaction).await;
        debug!("Send transaction to write connection.");
    });

    // Spawn task to manage read connection.
    let (tx1, mut rx1): (Sender<Frame>, Receiver<Frame>) = mpsc::channel(mpsc_size as usize);
    let r_manager = tokio::spawn(async move {
        // Receive messages from producer.
        while let Ok(message) = r.read_frame().await {
            let f = match message {
                Some(frame) => {
                    debug!(" Received frame: {:?}", frame);
                    frame
                }
                None => panic!("Server closed connection"),
            };
            tx1.send(f).await;
            debug!("Send response to response consumer task.");
        }
        info!("Closing read connection");
    });

    let consumer = tokio::spawn(async move {
        while let Some(message) = rx1.recv().await {
            info!("Client received {:?} from server", message);
        }
    });

    producer.await.unwrap();
    w_manager.await.unwrap();
    r_manager.await.unwrap();
    consumer.await.unwrap();

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
