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
use tracing::info;

/// `Producer` generates transactions and sends them to the 'WriteHandler`.
pub struct Producer {
    /// Parameter generator.
    pub generator: ParameterGenerator,

    /// Number of transactions to generate.
    pub transactions: u32,

    /// Transactions sent to write handler.
    pub sent: u32,

    /// Channel to `WriteHandler`.
    pub write_task_tx: tokio::sync::mpsc::Sender<Message>,

    /// Listen for client shutdown notification.
    pub listen_m_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
}

impl Producer {
    /// Create new `Producer`.
    pub fn new(
        configuration: Arc<Config>,
        write_task_tx: tokio::sync::mpsc::Sender<Message>,
        listen_m_rx: tokio::sync::mpsc::Receiver<()>,
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
        // Create shutdown listener.
        let listen_m_rx = Shutdown::new_mpsc(listen_m_rx);
        // Get transaction to generate.
        let transactions = configuration.get_int("transactions")? as u32;
        Ok(Producer {
            generator,
            sent: 0,
            transactions,
            write_task_tx,
            listen_m_rx,
        })
    }

    /// Send close connection message.
    pub async fn terminate(&mut self) -> Result<()> {
        // Create message.
        let message = Message::CloseConnection;
        // Send message to write handler.
        self.write_task_tx.send(message).await?;
        Ok(())
    }
}

/// Run the producer.
pub async fn run(mut producer: Producer) -> Result<()> {
    let handle = tokio::spawn(async move {
        info!("Generate {} transaction(s)", producer.transactions);
        // Generate transactions and listen for shutdown notification.
        for _i in 1..=producer.transactions {
            let maybe_transaction = tokio::select! {
                   res = producer.generator.get_transaction() => res,
                   _ = producer.listen_m_rx.recv() => {
                       producer.terminate().await?;
            //           debug!("Generated {} transactions", producer.sent);
                       return Ok(());
                   }
               };
            // Delay
            sleep(Duration::from_millis(1000)).await;
            // Send to write handler, waiting until capacity.
            producer.write_task_tx.send(maybe_transaction).await?;
            // Increment transactions sent.
            producer.sent += 1;
        }
        // Send close connection message.
        producer.terminate().await?;
        info!("Generated all messages");
        Ok(())
    });

    handle.await?
}

impl Drop for Producer {
    fn drop(&mut self) {
        //      debug!("Drop producer");
        //     debug!("Sent {} transctions to write handler", self.sent);
    }
}
