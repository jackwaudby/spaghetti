use crate::common::error::SpaghettiError;
use crate::common::message::Message;
use crate::common::parameter_generation::ParameterGenerator;
use crate::common::shutdown::Shutdown;
use crate::workloads::tatp::generator::TatpGenerator;
use crate::workloads::tpcc::generator::TpccGenerator;
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
    pub generator: ParameterGenerator,

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
                let gen = TatpGenerator::new(subscribers as u64, false);
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

        for i in 0..self.transactions {
            // Generate transactions and concurrently listen for server closing.
            let maybe_transaction = tokio::select! {
                res = self.generator.get_transaction() => res,
                _ = self.listen_c_rx.recv() => {
                    self.terminate().await?;
                    info!("Generated {:?} transactions", i);
                    return Ok(());
                }
            };
            // Normal execution.
            debug!("Generated {:?}", maybe_transaction);

            sleep(Duration::from_millis(1000)).await;
            self.write_task_tx.send(maybe_transaction).await?;
        }
        self.terminate().await?;
        debug!("Generated all messages");
        Ok(())
    }

    /// Send `CloseConnection` message.
    pub async fn terminate(&mut self) -> Result<()> {
        let message = Message::CloseConnection;
        debug!("Send {:?}", message);
        self.write_task_tx.send(message).await?;

        Ok(())
    }

    /// Wait for consumer to shutdown.
    pub async fn wait(&mut self) {
        self.listen_c_rx.recv().await;
    }
}
