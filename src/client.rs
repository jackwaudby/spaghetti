use crate::connection::{ReadConnection, WriteConnection};
use crate::frame::Frame;
use crate::handler::Response;
use crate::parameter_generation::ParameterGenerator;
use crate::shutdown::Shutdown;
use crate::transaction::Transaction;
use crate::workloads::tatp::{TatpGenerator, TatpTransaction};
use crate::workloads::tpcc::TpccGenerator;
use crate::Result;

use bytes::Bytes;
use config::Config;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::info;

/// Runs the client.
pub async fn run(config: Arc<Config>) {
    //// Shutdown channels. ////
    // `Producer` to `WriteHandler`.
    let (notify_wh_tx, listen_p_rx) = tokio::sync::mpsc::channel(1);
    // `ReadHandler` to `Consumer`
    let (notify_c_tx, listen_rh_rx) = tokio::sync::mpsc::channel(1);
    // `Consumer` to `Producer`
    let (notify_p_tx, listen_c_rx) = tokio::sync::mpsc::channel(1);

    //// Work channels. ////
    // `Producer` to `WriteHandler`.
    let mpsc_size = config.get_int("mpsc_size").unwrap();
    let (write_task_tx, write_task_rx): (Sender<Message>, Receiver<Message>) =
        mpsc::channel(mpsc_size as usize);
    // `ReadHandler` to `Consumer`.
    let (read_task_tx, read_task_rx): (Sender<Response>, Receiver<Response>) =
        mpsc::channel(mpsc_size as usize);

    //// Producer ////
    let mut producer = Producer::new(
        Arc::clone(&config),
        write_task_tx,
        notify_wh_tx,
        listen_c_rx,
    );

    //// Handlers ////
    // Open a connection to the server.
    let add = config.get_str("address").unwrap();
    let port = config.get_str("port").unwrap();
    let socket = TcpStream::connect(&format!("{}:{}", add, port)[..])
        .await
        .unwrap();
    info!("Client connected to server at {}:{}", add, port);
    // Split socket into reader and writer handlers.
    let (rd, wr) = io::split(socket);
    let w = WriteConnection::new(wr);
    let r = ReadConnection::new(rd);

    //// WriteHandler ////
    let mut wh = WriteHandler::new(w, write_task_rx, listen_p_rx);
    tokio::spawn(async move {
        // While no client shutdown message (keyboard trigger).
        while !wh.listen_p_rx.is_shutdown() && !wh.close_sent {
            // Receive messages from producer.
            if let Some(message) = wh.write_task_rx.recv().await {
                info!("Writing {:?} to socket", message);
                // Convert to frame.
                let frame = message.into_frame();
                // Write to socket.
                wh.connection.write_frame(&frame).await.unwrap();

                // If message is a close connection message then write to socket and drop.
                if let Some(CloseConnection) = message.as_any().downcast_ref::<CloseConnection>() {
                    info!("Closing write handler");
                    wh.close_sent = true;
                }
            }
        }
        // Drain outstanding messages
        if !wh.close_sent {
            while let Some(message) = wh.write_task_rx.recv().await {
                info!("Writing {:?} to socket", message);
                // Convert to frame.
                let frame = message.into_frame();
                // Write to socket.
                wh.connection.write_frame(&frame).await.unwrap();
            }
        }
        info!("Write handler dropped");
    });

    //// ReadHandler ////
    let mut rh = ReadHandler::new(r, read_task_tx, notify_c_tx);
    tokio::spawn(async move {
        // Read from connection.
        while let Ok(message) = rh.connection.read_frame().await {
            // Deserialize the response.
            let response = match message {
                Some(frame) => {
                    let decoded: bincode::Result<Response> =
                        bincode::deserialize(&frame.get_payload());
                    match decoded {
                        Ok(response) => response,
                        Err(_) => {
                            info!("Received ClosedConnection message.");
                            // Received a closed connection message.
                            break;
                        }
                    }
                }
                None => panic!("Server closed connection"),
            };
            info!("Received {:?}", response);
            rh.read_task_tx.send(response).await.unwrap();
        }
    });

    //// Consumer ////
    let mut c = Consumer::new(read_task_rx, listen_rh_rx, notify_p_tx);
    tokio::spawn(async move {
        c.run().await;
        //        let maybe_frame = tokio::select! {
        //         res = self.connection.read_frame() => res?,
        //         _ = self.shutdown.recv() => {
        //             // Shutdown signal received, terminate the task.
        //             return Ok(());
        //         }
        //     };

        // // While no shutdown message.
        // while !c.listen_rh_rx.is_shutdown() {
        //     // Receive messages from read handler.
        //     while let Some(message) = c.read_task_rx.recv().await {
        //         info!("Client received {:?} from server", message);
        //         // TODO: log to file.
        //     }
        // }
        // info!("HERE");
        // // Drain outstanding messages
        // while let Some(message) = c.read_task_rx.recv().await {
        //     info!("Client received {:?} from server", message);
        //     // TODO: log to file.
        // }
    });

    // Run producer.
    info!("Start producer");
    producer.run().await.unwrap();

    // // Concurrently run the producer and listen for the shutdown signal.
    // tokio::select! {
    //     res = producer.run() => {
    //         // Runs until max transactions is met.
    //         if let Err(err) = res {
    //             error!("{:?}",err);
    //         }
    //     }
    //     _ = signal::ctrl_c() => {
    //         // Runs until keyboard interrupt
    //         info!("Shutting down client");
    //         // Broadcast message to connections.
    //     }
    // }

    // Drop shutdown channel to write handler.
    let Producer {
        notify_wh_tx,
        mut listen_c_rx,
        ..
    } = producer;
    drop(notify_wh_tx);
    // Wait until `Consumer` closes channel.
    listen_c_rx.recv().await;
    info!("Clean shutdown");
}

// Spawned by client's main thread.
// Shutdown broadcast channel: main -> producer.
struct Producer {
    /// Parameter generator.
    generator: ParameterGenerator,

    /// Number of transactions to generate.
    transactions: u32,

    /// Send transactions writer task.
    write_task_tx: tokio::sync::mpsc::Sender<Message>,

    /// Notify `WriteHandler` of shutdown.
    notify_wh_tx: tokio::sync::mpsc::Sender<()>,

    /// Listen for shutdown notification from `Consumer`.
    listen_c_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
}

impl Producer {
    /// Create new `Producer`.
    fn new(
        configuration: Arc<Config>,
        write_task_tx: tokio::sync::mpsc::Sender<Message>,
        notify_wh_tx: tokio::sync::mpsc::Sender<()>,
        listen_c_rx: tokio::sync::mpsc::Receiver<()>,
    ) -> Producer {
        // Get workload type.
        let workload = configuration.get_str("workload").unwrap();
        // Create generator.
        let generator = match workload.as_str() {
            "tatp" => {
                // Get necessary initialise parameters.
                let subscribers = configuration.get_int("subscribers").unwrap();
                let gen = TatpGenerator::new(subscribers as u64);
                ParameterGenerator::Tatp(gen)
            }
            "tpcc" => {
                // TODO: Get necessary initialise parameters.
                let tpcc_gen = TpccGenerator {
                    warehouses: 10,
                    districts: 10,
                };
                ParameterGenerator::Tpcc(tpcc_gen)
            }
            _ => panic!("Workload not recognised, parameter generator can not be initialised."),
        };
        // Get transaction to generate.
        let transactions = configuration.get_int("transactions").unwrap() as u32;
        // Create shutdown listener.
        let listen_c_rx = Shutdown::new_mpsc(listen_c_rx);
        Producer {
            generator,
            transactions,
            write_task_tx,
            notify_wh_tx,
            listen_c_rx,
        }
    }

    /// Run the producer.
    /// Generate the requested number of transactions.
    /// Return early if shutdown triggered.
    async fn run(&mut self) -> Result<()> {
        info!("Generate {:?} transaction", self.transactions);

        for _ in 0..self.transactions {
            if self.listen_c_rx.is_shutdown() {
                info!("Producer received shutdown notification");
                return Ok(());
            }
            // Generate a transaction.
            let transaction = self.generator.get_transaction();
            info!("Generated {:?}", transaction);
            // Concurretly send trasaction to write task and listen for shutdown notification.
            tokio::select! {
                res =  self.write_task_tx.send(transaction) => res?,
                _ = signal::ctrl_c() => {
                    info!("Keyboard interrupt");
                    // Send `CloseConnection` message.
                    self.terminate().await.unwrap();
                    return Ok(());
                }
            }
        }
        info!("{:?} transaction generated", self.transactions);

        // Send `CloseConnection` message.
        self.terminate().await.unwrap();
        Ok(())
    }

    /// Send `CloseConnection` message.
    async fn terminate(&mut self) -> Result<()> {
        let message = Box::new(CloseConnection);
        info!("Send {:?}", message);
        self.write_task_tx.send(message).await?;
        Ok(())
    }
}

struct Consumer {
    // Channel from read handler.
    read_task_rx: tokio::sync::mpsc::Receiver<Response>,
    // Listen for shutdown from read handler.
    listen_rh_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
    // Notify producer of consumer's shutdown.
    _notify_p_tx: tokio::sync::mpsc::Sender<()>,
}

impl Consumer {
    /// Create new `Consumer`.
    fn new(
        read_task_rx: tokio::sync::mpsc::Receiver<Response>,
        listen_rh_rx: tokio::sync::mpsc::Receiver<()>,
        _notify_p_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Consumer {
        let listen_rh_rx = Shutdown::new_mpsc(listen_rh_rx);
        Consumer {
            read_task_rx,
            listen_rh_rx,
            _notify_p_tx,
        }
    }

    async fn run(&mut self) -> Result<()> {
        while !self.listen_rh_rx.is_shutdown() {
            let message = tokio::select! {
                res = self.read_task_rx.recv() => res,
                _ = self.listen_rh_rx.recv() => {
                    // Shutdown signal received, terminate the task.
                    // Drain outstanding messages
                    while let Some(message) = self.read_task_rx.recv().await {
                        info!("Client received {:?} from server", message);
                        // TODO: log to file.
                    }
                    return Ok(());
                }
            };

            info!("Client received {:?} from server", message);
        }
        Ok(())
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        info!("Drop Consumer");
    }
}

/// Manages the write half of a tcp stream.
pub struct WriteHandler<W: AsyncWrite + Unpin> {
    // Write half of stream.
    connection: WriteConnection<W>,
    // `Message` channel from `Producer`.
    write_task_rx: tokio::sync::mpsc::Receiver<Message>,
    // Listen for `Producer` to close channel (shutdown procedure).
    listen_p_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
    close_sent: bool,
}

impl<W: AsyncWrite + Unpin> WriteHandler<W> {
    /// Create new `WriteHandler`.
    fn new(
        connection: WriteConnection<W>,
        write_task_rx: tokio::sync::mpsc::Receiver<Message>,
        listen_p_rx: tokio::sync::mpsc::Receiver<()>,
    ) -> WriteHandler<W> {
        let listen_p_rx = Shutdown::new_mpsc(listen_p_rx);
        WriteHandler {
            connection,
            write_task_rx,
            listen_p_rx,
            close_sent: false,
        }
    }
}

impl<W: AsyncWrite + Unpin> Drop for WriteHandler<W> {
    fn drop(&mut self) {
        info!("Drop WriteHandler");
    }
}

/// Manages the read half of a tcp stream.
struct ReadHandler<R: AsyncRead + Unpin> {
    // Read half of tcp stream.
    connection: ReadConnection<R>,
    // `Message` channel to `Consumer`.
    read_task_tx: tokio::sync::mpsc::Sender<Response>,
    /// Notify `Consumer` of shutdown.
    _notify_c_tx: tokio::sync::mpsc::Sender<()>,
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    fn new(
        connection: ReadConnection<R>,
        read_task_tx: tokio::sync::mpsc::Sender<Response>,
        _notify_c_tx: tokio::sync::mpsc::Sender<()>,
    ) -> ReadHandler<R> {
        ReadHandler {
            connection,
            read_task_tx,
            _notify_c_tx,
        }
    }
}

impl<R: AsyncRead + Unpin> Drop for ReadHandler<R> {
    fn drop(&mut self) {
        info!("Drop ReadHandler");
    }
}

impl Debug for Message {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Convert trait object to concrete type.
        match self.as_any().downcast_ref::<TatpTransaction>() {
            Some(tatp) => return write!(f, "{:?}", tatp),
            None => {}
        }

        match self.as_any().downcast_ref::<CloseConnection>() {
            Some(close) => write!(f, "{:?}", close),
            None => write!(f, "Unable to cast to concrete type."),
        }
    }
}

pub type Message = Box<dyn Transaction + Sync + Send + 'static>;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CloseConnection;

impl Transaction for CloseConnection {
    fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ConnectionClosed;

impl Transaction for ConnectionClosed {
    fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
