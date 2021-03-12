use crate::common::error::FatalError;
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

    /// Generator mode.
    pub mode: GeneratorMode,

    /// Number of transactions to generate.
    pub transactions: u32,

    /// Transactions sent to write handler.
    pub sent: u32,

    /// Channel to `WriteHandler`.
    pub write_task_tx: tokio::sync::mpsc::Sender<Message>,

    /// Channel from `ReadHandler` notifying a responses has been received.
    pub received_rx: tokio::sync::mpsc::Receiver<()>,

    /// Listen for client shutdown notification.
    pub listen_m_rx: Shutdown<tokio::sync::mpsc::Receiver<()>>,
}

impl Producer {
    /// Create new `Producer`.
    pub fn new(
        configuration: Arc<Config>,
        write_task_tx: tokio::sync::mpsc::Sender<Message>,
        listen_m_rx: tokio::sync::mpsc::Receiver<()>,
        received_rx: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<Producer> {
        // Get mode.
        let mode = configuration.get_str("generator_mode")?;
        let think_time = configuration.get_int("think_time")? as u64;
        let gen_mode = match mode.as_str() {
            "closed" => GeneratorMode::Closed { think_time },
            "open" => GeneratorMode::Open { think_time },
            "flood" => GeneratorMode::Flood,
            _ => return Err(FatalError::IncorrectGeneratorMode(mode.to_string()).into()),
        };
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
            _ => return Err(FatalError::IncorrectWorkload(workload).into()),
        };
        // Create shutdown listener.
        let listen_m_rx = Shutdown::new_mpsc(listen_m_rx);
        // Get transaction to generate.
        let transactions = configuration.get_int("transactions")? as u32;

        Ok(Producer {
            generator,
            mode: gen_mode,
            sent: 0,
            transactions,
            write_task_tx,
            received_rx,
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
                    return Ok(());
                }
            };

            // Send transaction.
            producer.write_task_tx.send(maybe_transaction).await?;

            match producer.mode {
                GeneratorMode::Closed { think_time } => {
                    // Wait until response received
                    producer.received_rx.recv().await.unwrap();
                    // Think time
                    if think_time > 0 {
                        sleep(Duration::from_millis(think_time)).await;
                    }
                }
                GeneratorMode::Flood => {
                    // continue.
                }
                GeneratorMode::Open { think_time } => {
                    sleep(Duration::from_millis(think_time)).await;
                }
            }

            // Increment transactions sent.
            producer.sent += 1;

            if producer.sent % 1000 == 0 {
                info!("Sent: {}", producer.sent);
            }
        }
        // Send close connection message.
        producer.terminate().await?;
        info!("Generated all messages");
        Ok(())
    });

    handle.await?
}

pub enum GeneratorMode {
    /// New requests triggered by the completions of old requests + optional think time.
    Closed { think_time: u64 },
    /// Produce requests following some distribution - set as fixed interval think time for now.
    Open { think_time: u64 },
    /// Produce requests as fast as they can, don't wait for completion of old jobs, and there is no think time.
    Flood,
}

impl Drop for Producer {
    fn drop(&mut self) {
        //      debug!("Drop producer");
        //     debug!("Sent {} transctions to write handler", self.sent);
    }
}
