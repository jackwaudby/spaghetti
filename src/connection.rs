use crate::frame::{Frame, ParseError, ParseErrorKind};
use crate::SpagError;

use bytes::{Buf, BytesMut}; // traits for working with buffer implementations
use std::io::Cursor;
use tokio::io::BufWriter; // acts as a write buffer around the tcp stream
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream; // byte stream between peers

#[derive(Debug)]
pub struct Connection {
    // BufWriter provides write-level buffering around the stream.
    pub stream: BufWriter<TcpStream>,
    // Read bytes from the stream into this buffer.
    buffer: BytesMut,
}

impl Connection {
    /// Create a new connection and initialise buffers.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // 4KB buffer
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    // Read a single frame from the underlying TCP connection.
    // Returns:
    // - Ok(Some(frame)) if frame can be read.
    // - Ok(None) if the connection is closed.
    // - Error if there has been an encoding error.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, SpagError> {
        loop {
            println!("Attempt to parse");
            // Attempt to parse a frame from buffered data.
            match self.parse_frame() {
                Ok(frame) => return Ok(Some(frame)),
                Err(e) => match e.kind {
                    ParseErrorKind::Incomplete => {
                        // Not enough buffered data to read a frame.
                        println!("Not enough data in buffer");
                        // Attempt to read more from the socket.
                        if 0 == self.stream.read_buf(&mut self.buffer).await? {
                            println!("Socket is empty");
                            // If socket is empty so should the buffer.
                            if self.buffer.is_empty() {
                                println!("Buffer is empty");
                                // Remote cleanly closed the connection.
                                return Ok(None);
                            } else {
                                println!("Partial frame");
                                // Remote closed while sending a frame.
                                return Err(ParseError::new(ParseErrorKind::CorruptedFrame).into());
                            }
                        }
                    }
                    ParseErrorKind::CorruptedFrame => return Err(Box::new(e)),
                    ParseErrorKind::Invalid => return Err(Box::new(e)),
                },
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Frame, ParseError> {
        println!("Attempting parse");
        // create cursor over buffer
        let mut buff = Cursor::new(&self.buffer[..]);
        println!("Validating");
        Frame::validate(&mut buff)?;
        println!("Validated");
        // Validate function advanced cursor to the end of the frame.
        let len = buff.position();
        // Reset cursor to 0, ready for parse.
        buff.set_position(0);
        println!("Actual parse");
        // Parse
        let frame = Frame::parse(&mut buff)?;

        // discard from buffer
        self.buffer.advance(len as usize);
        Ok(frame)
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), std::io::Error> {
        // Get length and serialize
        let len = frame.payload.len();
        let lens: Vec<u8> = bincode::serialize(&len).unwrap().into();
        // Write data to stream in the background
        self.stream.write_all(b"$").await?;
        self.stream.write_all(&lens[..]).await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.write_all(&frame.payload).await?;
        self.stream.write_all(b"\r\n").await?;
        // Flush from buffer to socket
        self.stream.flush().await
    }
}
