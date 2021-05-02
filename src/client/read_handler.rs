use crate::common::connection::ReadConnection;
use crate::common::error::FatalError;
use crate::common::frame::{ParseError, ParseErrorKind};
use crate::common::message::Message;
use crate::Result;
use tokio::io::AsyncRead;
use tracing::debug;

/// Manages the read half of the `client` TCP stream.
pub struct ReadHandler<R: AsyncRead + Unpin> {
    /// Read half of TCP stream wrapped with buffers.
    pub connection: ReadConnection<R>,
    /// `Message` channel to `Consumer`.
    pub read_task_tx: tokio::sync::mpsc::Sender<Message>,
    /// Notify producer request has received a response
    pub received_tx: tokio::sync::mpsc::Sender<()>,
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    /// Create new `ReadHandler`.
    pub fn new(
        connection: ReadConnection<R>,
        read_task_tx: tokio::sync::mpsc::Sender<Message>,
        received_tx: tokio::sync::mpsc::Sender<()>,
    ) -> ReadHandler<R> {
        ReadHandler {
            connection,
            read_task_tx,
            received_tx,
        }
    }
}

impl<R: AsyncRead + Unpin> Drop for ReadHandler<R> {
    fn drop(&mut self) {
        //        debug!("Drop read handler");
    }
}

/// Run `ReadHandler`.
///
/// # Errors
///
/// + Returns `ConnectionUnexpectedlyClosed` if the TCP stream has been unexpectedly
/// closed.
/// + Returns  `Parse(ParseError)` if a message can not be correctly parsed from the TCP/// stream.
/// + Returns `UnexpectedMessage` if the client receives an unanticipated message.
pub async fn run<R: AsyncRead + Unpin + Send + 'static>(mut rh: ReadHandler<R>) -> Result<()> {
    // Spawn tokio task.
    let handle = tokio::spawn(async move {
        // Attempt to read frame from connection until error or connection is closed.
        loop {
            if let Ok(message) = rh.connection.read_frame().await {
                // Deserialize the response.
                let response = match message {
                    // A frame has been received.
                    Some(frame) => {
                        // Attempt to deserialise.
                        let decoded: bincode::Result<Message> =
                            bincode::deserialize(&frame.get_payload());
                        match decoded {
                            Ok(decoded) => match decoded {
                                // Connection gracefully closed.
                                Message::ConnectionClosed => {
                                    rh.read_task_tx.send(decoded).await.unwrap();
                                    return Ok(());
                                }
                                // Response received.
                                Message::Response {
                                    request_no,
                                    outcome,
                                } => {
                                    // Notify producer.
                                    debug!("Notify producer");

                                    if rh.received_tx.send(()).await.is_err() {
                                        debug!("Producer's received dropped");
                                    }

                                    Message::Response {
                                        request_no,
                                        outcome,
                                    }
                                }
                                // Received unexpected message.
                                _ => return Err(FatalError::UnexpectedMessage),
                            },
                            Err(e) => {
                                let error: ParseError = e.into();
                                return Err(error.into());
                            }
                        }
                    }
                    // The connection has been unexpectedly closed.
                    None => {
                        //            debug!("Server unexpectedly closed");
                        return Err(FatalError::ConnectionUnexpectedlyClosed);
                    }
                };
                rh.read_task_tx.send(response).await.unwrap();
            } else {
                // There has been an encoding error.
                return Err(FatalError::from(ParseError::new(ParseErrorKind::Invalid)));
            }
        }
    });
    // Get value from future.
    let value = handle.await;
    // Return () or error.
    match value {
        Ok(res) => match res {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        },
        Err(e) => Err(Box::new(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::{Message, Outcome};

    use test_env_log::test;
    use tokio::sync::mpsc::{self, Receiver, Sender};
    use tokio_test::io::Builder;

    /// Unable to parse frame from underlying connection.
    #[test]
    fn read_handler_encoding_error_test() {
        // Initialise script builder.
        let mut builder = Builder::new();
        // Schedule read of incorrect encoding.
        builder.read(b"%");
        // Create mock io object.
        let mock = builder.build();
        // Create read handler.
        let r = ReadConnection::new(mock);
        let (read_task_tx, _): (Sender<Message>, Receiver<Message>) = mpsc::channel(32);
        let (recevied_tx, _): (Sender<()>, Receiver<()>) = mpsc::channel(32);

        let rh = ReadHandler::new(r, read_task_tx, recevied_tx);
        // Run read handler.
        let res = tokio_test::block_on(run(rh));

        assert_eq!(
            *res.unwrap_err().downcast::<FatalError>().unwrap(),
            FatalError::Parse(ParseError::new(ParseErrorKind::Invalid))
        );
    }

    /// Receive a response and then a closed connection message.
    #[test]
    fn read_handler_happy_path_test() {
        // Initialise script builder.
        let mut builder = Builder::new();

        // Schedule response message
        let r = Outcome::Committed {
            value: Some(String::from("Test")),
        };
        let response = Message::Response {
            request_no: 1,
            outcome: r,
        };
        let response_frame = response.into_frame();
        let r_len = response_frame.payload.len();
        let r_lens: Vec<u8> = bincode::serialize(&r_len).unwrap().into();
        let mut response_vec = Vec::new();
        response_vec.extend_from_slice(b"$");
        response_vec.extend_from_slice(&r_lens[..]);
        response_vec.extend_from_slice(b"\r\n");
        response_vec.extend_from_slice(&response_frame.payload);
        response_vec.extend_from_slice(b"\r\n");
        builder.read(&response_vec[..]);

        // Schedule connection closed message.
        let cc = Message::ConnectionClosed;
        let f = cc.into_frame();
        // Get length and serialize
        let len = f.payload.len();
        let lens: Vec<u8> = bincode::serialize(&len).unwrap().into();
        // Create byte array.
        let mut vec = Vec::new();
        vec.extend_from_slice(b"$");
        vec.extend_from_slice(&lens[..]);
        vec.extend_from_slice(b"\r\n");
        vec.extend_from_slice(&f.payload);
        vec.extend_from_slice(b"\r\n");
        // Schedule read
        builder.read(&vec[..]);

        // Create mock io object.
        let mock = builder.build();
        // Create read handler.
        let r = ReadConnection::new(mock);
        let (read_task_tx, mut read_task_rx): (Sender<Message>, Receiver<Message>) =
            mpsc::channel(32);
        let (recevied_tx, _): (Sender<()>, Receiver<()>) = mpsc::channel(32);

        let rh = ReadHandler::new(r, read_task_tx, recevied_tx);
        // Run read handler.
        let res = tokio_test::block_on(run(rh));
        assert_eq!(res.unwrap(), ());
        let value = tokio_test::block_on(read_task_rx.recv());

        assert_eq!(value.unwrap(), response);
    }

    /// Receive an unexpected message type
    #[test]
    fn read_handler_unexpected_message_test() {
        // Initialise script builder.
        let mut builder = Builder::new();
        // Schedule close connection message; read handler should never receive this.
        let cc = Message::CloseConnection;
        let f = cc.into_frame();
        // Get length and serialize
        let len = f.payload.len();
        let lens: Vec<u8> = bincode::serialize(&len).unwrap().into();
        // Create byte array.
        let mut vec = Vec::new();
        vec.extend_from_slice(b"$");
        vec.extend_from_slice(&lens[..]);
        vec.extend_from_slice(b"\r\n");
        vec.extend_from_slice(&f.payload);
        vec.extend_from_slice(b"\r\n");
        // Schedule read
        builder.read(&vec[..]);

        // Create mock io object.
        let mock = builder.build();
        // Create read handler.
        let r = ReadConnection::new(mock);
        let (read_task_tx, _): (Sender<Message>, Receiver<Message>) = mpsc::channel(32);
        let (recevied_tx, _): (Sender<()>, Receiver<()>) = mpsc::channel(32);

        let rh = ReadHandler::new(r, read_task_tx, recevied_tx);
        // Run read handler.
        let res = tokio_test::block_on(run(rh));
        assert_eq!(
            *res.unwrap_err().downcast::<FatalError>().unwrap(),
            FatalError::UnexpectedMessage
        );
    }
}
