use crate::common::connection::ReadConnection;
use crate::common::error::SpaghettiError;
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
}

impl<R: AsyncRead + Unpin> ReadHandler<R> {
    /// Create new `ReadHandler`.
    pub fn new(
        connection: ReadConnection<R>,
        read_task_tx: tokio::sync::mpsc::Sender<Message>,
    ) -> ReadHandler<R> {
        ReadHandler {
            connection,
            read_task_tx,
        }
    }
}

impl<R: AsyncRead + Unpin> Drop for ReadHandler<R> {
    fn drop(&mut self) {
        debug!("Drop read handler");
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
                                    debug!("Connection closed");
                                    debug!("Read handler has sent connection closed message to consumer");
                                    rh.read_task_tx.send(decoded).await.unwrap();
                                    return Ok(());
                                }
                                // Response received.
                                Message::Response {
                                    request_no,
                                    resp: response,
                                } => Message::Response {
                                    request_no,
                                    resp: response,
                                },
                                // Received unexpected message.
                                _ => return Err(SpaghettiError::UnexpectedMessage),
                            },
                            Err(e) => {
                                let error: ParseError = e.into();
                                return Err(error.into());
                            }
                        }
                    }
                    // The connection has been unexpectedly closed.
                    None => {
                        debug!("Server unexpectedly closed");
                        return Err(SpaghettiError::ConnectionUnexpectedlyClosed);
                    }
                };
                debug!("Received {:?}", response);
                rh.read_task_tx.send(response).await.unwrap();
            } else {
                // There has been an encoding error.
                return Err(SpaghettiError::from(ParseError::new(
                    ParseErrorKind::Invalid,
                )));
            }
        }
    });
    // Get value from future.
    let value = handle.await;
    // Return () or error.
    match value {
        Ok(res) => match res {
            Ok(_) => return Ok(()),
            Err(e) => return Err(Box::new(e)),
        },
        Err(e) => return Err(Box::new(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::{Message, Response};
    use std::sync::Once;
    use tokio::sync::mpsc::{self, Receiver, Sender};
    use tokio_test::io::Builder;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    static LOG: Once = Once::new();

    fn logging(on: bool) {
        if on {
            LOG.call_once(|| {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting default subscriber failed");
            });
        }
    }

    /// Unable to parse frame from underlying connection.
    #[test]
    fn read_handler_encoding_error_test() {
        // Initialise logging.
        logging(false);
        // Initialise script builder.
        let mut builder = Builder::new();
        // Schedule read of incorrect encoding.
        builder.read(b"%");
        // Create mock io object.
        let mock = builder.build();
        // Create read handler.
        let r = ReadConnection::new(mock);
        let (read_task_tx, _): (Sender<Message>, Receiver<Message>) = mpsc::channel(32);
        let (notify_c_tx, _) = mpsc::channel(1);
        let rh = ReadHandler::new(r, read_task_tx, notify_c_tx);
        // Run read handler.
        let res = tokio_test::block_on(run(rh));

        assert_eq!(
            *res.unwrap_err().downcast::<SpaghettiError>().unwrap(),
            SpaghettiError::Parse(ParseError::new(ParseErrorKind::Invalid))
        );
    }

    /// Receive a response and then a closed connection message.
    #[test]
    fn read_handler_happy_path_test() {
        // Initialise logging.
        logging(false);
        // Initialise script builder.
        let mut builder = Builder::new();

        // Schedule response message
        let r = Response::Committed {
            value: Some(String::from("Test")),
        };
        let response = Message::Response {
            request_no: 1,
            resp: r,
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
        let (notify_c_tx, _) = mpsc::channel(1);
        let rh = ReadHandler::new(r, read_task_tx, notify_c_tx);
        // Run read handler.
        let res = tokio_test::block_on(run(rh));
        assert_eq!(res.unwrap(), ());
        let value = tokio_test::block_on(read_task_rx.recv());

        assert_eq!(value.unwrap(), response);
    }

    /// Receive an unexpected message type
    #[test]
    fn read_handler_unexpected_message_test() {
        // Initialise logging.
        logging(false);
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
        let (notify_c_tx, _) = mpsc::channel(1);
        let rh = ReadHandler::new(r, read_task_tx, notify_c_tx);
        // Run read handler.
        let res = tokio_test::block_on(run(rh));
        assert_eq!(
            *res.unwrap_err().downcast::<SpaghettiError>().unwrap(),
            SpaghettiError::UnexpectedMessage
        );
    }

    /// Connection with server unexpectedly dropped.
    #[test]
    fn read_handler_tcp_connection_unexpectedly_dropped() {
        // TODO
        assert!(true);
    }
}
