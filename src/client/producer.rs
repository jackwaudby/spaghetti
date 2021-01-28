use crate::common::error::SpaghettiError;
use crate::common::message::Message;
use crate::common::parameter_generation::ParameterGenerator;
use crate::common::shutdown::Shutdown;
use crate::workloads::tatp::TatpGenerator;
use crate::workloads::tpcc::TpccGenerator;
use crate::Result;

use config::Config;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{debug, info};

/// `Producer` generates transactions and sends them to the `client`s write socket.
///
/// Spawned by `client` and lives on its main thread.
/// Generates transactions until the specified amount is produced or the `client`
/// receives a keyboard interrupt.
pub struct Producer {
    /// Parameter generator.
    generator: ParameterGenerator,

    /// Number of transactions to generate.
    transactions: u32,

    /// Send transactions writer task.
    pub write_task_tx: tokio::sync::mpsc::Sender<Message>,

    /// Notify `WriteHandler` of shutdown.
    pub notify_wh_tx: tokio::sync::mpsc::Sender<()>,

    /// Listen for shutdown notification from `Consumer`.
    pub listen_c_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
}

impl Producer {
    /// Create new `Producer`.
    pub fn new(
        configuration: Arc<Config>,
        write_task_tx: tokio::sync::mpsc::Sender<Message>,
        notify_wh_tx: tokio::sync::mpsc::Sender<()>,
        listen_c_rx: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<Producer> {
        // Get workload type.
        let workload = configuration.get_str("workload")?;
        // Create generator.
        let generator = match workload.as_str() {
            "tatp" => {
                // Get necessary initialise parameters.
                let subscribers = configuration.get_int("subscribers")?;
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
            _ => return Err(Box::new(SpaghettiError::IncorrectWorkload)),
        };
        // Get transaction to generate.
        let transactions = configuration.get_int("transactions")? as u32;
        // Create shutdown listener.
        let listen_c_rx = Shutdown::new_mpsc(listen_c_rx);
        Ok(Producer {
            generator,
            transactions,
            write_task_tx,
            notify_wh_tx,
            listen_c_rx,
        })
    }

    /// Run the producer.
    pub async fn run(&mut self) -> Result<()> {
        info!("Generate {:?} transaction", self.transactions);

        for _ in 0..self.transactions {
            if self.listen_c_rx.is_shutdown() {
                info!("Producer received shutdown notification");
                return Ok(());
            }
            // Generate a transaction.
            let transaction = self.generator.get_transaction();
            info!("Generated {:?}", transaction);
            sleep(Duration::from_millis(5000)).await;
            let res = self.write_task_tx.send(transaction).await;
            if let Err(_) = res {
                debug!("producer to write handler channel unexpectedly closed");
            }
        }
        info!("{:?} transaction generated", self.transactions);

        // Send `CloseConnection` message.
        self.terminate().await.unwrap();
        Ok(())
    }

    /// Send `CloseConnection` message.
    pub async fn terminate(&mut self) -> Result<()> {
        let message = Message::CloseConnection;
        info!("Send {:?}", message);
        let res = self.write_task_tx.send(message).await;
        if let Err(_) = res {
            debug!("producer to write handler channel unexpectedly closed");
        }
        Ok(())
    }

    /// Wait for consumer to shutdown.
    pub async fn wait(&mut self) {
        self.listen_c_rx.recv().await;
    }
}