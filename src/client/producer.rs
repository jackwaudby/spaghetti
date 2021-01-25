use crate::message::{CloseConnection, Message};
use crate::parameter_generation::ParameterGenerator;
use crate::shutdown::Shutdown;
use crate::workloads::tatp::TatpGenerator;
use crate::workloads::tpcc::TpccGenerator;
use crate::Result;

use config::Config;
use std::sync::Arc;
use tokio::signal;
use tracing::info;

// Spawned by client's main thread.
// Shutdown broadcast channel: main -> producer.
pub struct Producer {
    /// Parameter generator.
    generator: ParameterGenerator,

    /// Number of transactions to generate.
    transactions: u32,

    /// Send transactions writer task.
    write_task_tx: tokio::sync::mpsc::Sender<Message>,

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
