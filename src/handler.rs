use crate::connection::{ReadConnection, WriteConnection};
use crate::frame::Frame;
use crate::shutdown::Shutdown;
use crate::workloads::{tatp, tpcc};
use crate::Result;
use bytes::Bytes;
use config::Config;
use core::fmt::Debug;
use std::marker::Unpin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info};

/// Represents the handler for the read half of a client connection.
#[derive(Debug)]
pub struct ReadHandler<R: AsyncRead + Unpin> {
    /// Read half of tcp stream with buffers and frame parsing utils.
    pub connection: ReadConnection<R>,

    /// Listen for server shutdown notifications.
    pub shutdown: Shutdown,

    /// Channnel for sending transaction request to the transaction manager.
    /// Communication channel between async and sync code.
    pub notify_tm_job_tx: std::sync::mpsc::Sender<tatp::TatpTransaction>,

    /// Channel for notify the transaction manager of shutdown.
    /// Implicitly dropped when handler is dropped (safely finished).
    /// Communication channel between async and sync code.
    pub _notify_tm_tx: std::sync::mpsc::Sender<()>,
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    /// Process a single connection.
    ///
    /// Frames are requested from the socket and then processed.
    /// Responses are written back to the socket.
    pub async fn run(&mut self, config: Arc<Config>) -> Result<()> {
        debug!("Processing connection");
        // While shutdown signal not received try to read frames.
        while !self.shutdown.is_shutdown() {
            // While reading a requested frame listen for shutdown.
            debug!("Attempting to read frames");
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // Shutdown signal received, terminate the task.
                    return Ok(());
                }
            };

            // If `None` returned then the socket has been closed, terminate task.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Deserialise frame.
            let w = config.get_str("workload").unwrap();
            match w.as_str() {
                "tatp" => {
                    let decoded: tatp::TatpTransaction =
                        bincode::deserialize(&frame.get_payload()).unwrap();
                    info!("Received: {:?}", decoded);
                }
                "tpcc" => {
                    let decoded: tpcc::TpccTransaction =
                        bincode::deserialize(&frame.get_payload()).unwrap();
                    info!("Received: {:?}", decoded);
                }
                _ => unimplemented!(),
            };

            // TODO: fixed as GetSubscriberData transaction.
            let dat = tatp::GetSubscriberData { s_id: 0 };
            // let t = Box::new(tatp::TatpTransaction::GetSubscriberData(dat));
            let t = tatp::TatpTransaction::GetSubscriberData(dat);

            info!("Pushed transaction to work queue");
            // TODO: change type of channel to trait bound.
            self.notify_tm_job_tx.send(t).unwrap();
            // self.work_queue.push(t);

            // Response placeholder.
            // let b = Bytes::copy_from_slice(b"ok");
            // let f = Frame::new(b); // Response placeholder
            // debug!("Sending reply: {:?}", f);
            // self.connection.write_frame(&f).await?;
        }
        Ok(())
    }
}

/// Represents the handler for the write half of a client connection.
// TODO: Graceful shutdown.
#[derive(Debug)]
pub struct WriteHandler<R: AsyncWrite + Unpin> {
    /// Write half of tcp stream with buffers.
    pub connection: WriteConnection<R>,
    // Listen for server shutdown notifications.
    // pub shutdown: Shutdown,

    // Channnel for sending transaction request to the transaction manager.
    // Communication channel between async and sync code.
    // pub notify_tm_job_tx: std::sync::mpsc::Sender<tatp::TatpTransaction>,

    // Channel for notify the transaction manager of shutdown.
    // Implicitly dropped when handler is dropped (safely finished).
    // Communication channel between async and sync code.
    // pub _notify_tm_tx: std::sync::mpsc::Sender<()>,
}

impl<R: AsyncWrite + Unpin> WriteHandler<R> {
    /// Process a single connection.
    ///
    /// Frames are requested from the socket and then processed.
    /// Responses are written back to the socket.
    pub async fn run(&mut self, _config: Arc<Config>) -> Result<()> {
        // Response placeholder.
        let b = Bytes::copy_from_slice(b"ok");
        let f = Frame::new(b); // Response placeholder
                               // debug!("Sending reply: {:?}", f);
        self.connection.write_frame(&f).await?;

        Ok(())
    }
}
