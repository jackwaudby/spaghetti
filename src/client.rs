use crate::connection::Connection;
use crate::frame::Frame;
use crate::parameter_generation::ParameterGenerator;
use crate::workloads::tatp::{TatpGenerator, TatpTransaction};
use crate::workloads::tpcc::TpccGenerator;
use crate::Result;

use config::Config;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
// use tokio::sync::oneshot;
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
            // let frame = cmd.transaction.into_frame();
            // TODO: this should be a submit function that receives a response.
            // info!("Send message: {:?}", frame);
            // let reply = client.submit(&frame).await;
            // info!("Consumer received reply: {:?}", reply);
            // TODO: Ignore errors and send response back to producer, currently replies with unit struct
            // let _ = cmd.resp.send(Ok(()));
        }
    });

    // A producer creates transactions.
    // Example output from generator
    let workload = "tpcc";

    let pg = match workload {
        "tatp" => {
            let tatp_gen = TatpGenerator { subscribers: 10 };
            ParameterGenerator::Tatp(tatp_gen)
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
    let out = pg.get_transaction();

    let o: &TatpTransaction = match out.as_any().downcast_ref::<TatpTransaction>() {
        Some(o) => {
            info!("downcast: {:?}", o);
            o
        }
        None => panic!("&out isn't a TatpTransaction"),
    };

    // let gen = ParameterGenerator::Tatp(TatpGenerator::new(10));
    // let t = gen.get_transaction();
    // print_if_string(t);

    // Get generation parameters from config
    // let gen = match conf.get_str("workload").unwrap().as_str() {
    //     "tatp" => {
    //         let subscribers = conf.get_int("subscribers").unwrap() as u64;
    //         ParameterGenerator::Tatp(TatpGen::new(subscribers))
    //     }
    //     "tpcc" => {
    //         let districts = conf.get_int("districts").unwrap() as u64;
    //         let warehouses = conf.get_int("warehouses").unwrap() as u64;
    //         ParameterGenerator::Tpcc(TpccGen::new(districts, warehouses))
    //     }
    //     _ => unimplemented!(),
    // };

    let producer = tokio::spawn(async move {
        // Spawn response channel
        // let (resp_tx, resp_rx) = oneshot::channel();

        // Generate transaction
        // let params = gen.get_transaction();
        // Wrap in command
        // let c = Command {
        //     transaction: Box::new(t),
        //     resp: resp_tx,
        // };

        // // Send transaction to client resource manager
        // tx.send(c).await;

        // // Await response
        // let res = resp_rx.await;
        // info!("Producer received reply: {:?}", res);
    });

    producer.await.unwrap();
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

struct Command {}
