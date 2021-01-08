//! Provides a type representing a frame and utilities for parsing frames from a byte array.

use bytes::{Buf, Bytes}; // byte array struct and traits for working with buffer implementations

use std::fmt;
use std::io::Cursor;
use std::io::Error as IoError;

/// A frame in spaghetti's network protocol.
#[derive(Debug, PartialEq)]
pub struct Frame {
    pub payload: Bytes, // serialise transaction into Bytes into_frame()
}

impl Frame {
    /// Create new frame.
    pub fn new(payload: Bytes) -> Frame {
        Frame { payload }
    }

    pub fn get_payload(&self) -> Bytes {
        Bytes::copy_from_slice(&self.payload)
    }

    /// Validates if an entire message can be decoded from the read buffer.
    pub fn validate(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        println!("Starting to validate");
        match get_u8(src)? {
            b'$' => {
                // Attempt to get payload length, returns Incomplete if not possible
                println!("matched first bytes");
                let len = get_payload_length(src)?;
                // There should be len + 2 bytes (\r\n) in the buffer
                let n = len + 2;
                // Attempt to skip n, returns Incomplete if not possible
                skip(src, n)
            }
            _ => {
                println!("Invalid");
                // Invalid start byte
                Err(Error::new(ErrorKind::Invalid))
            }
        }
    }

    /// Parse message from validated read buffer.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'$' => {
                let len = get_payload_length(src)?;
                let n = len + 2;
                // check again required data in buffer
                if src.remaining() < n {
                    return Err(Error::new(ErrorKind::Incomplete));
                }

                let start = src.position() as usize;
                let end = start + n - 2;
                let x: &[u8] = &src.get_ref()[start..end];
                let data = Bytes::copy_from_slice(x);

                // advance to after data
                src.advance(n);

                Ok(Frame::new(data))
            }
            _ => {
                // Invalid start byte, this should be unreachable
                unimplemented!()
            }
        }
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::new(ErrorKind::Incomplete));
    }

    Ok(src.get_u8())
}

/// Represents a parsing frame error.
#[derive(Debug, PartialEq)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    /// Create new parse error.
    pub fn new(kind: ErrorKind) -> Error {
        Error { kind }
    }
    // Get parse error type.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorKind {
    /// Not enough data available in read buffer to parse message.
    Incomplete,
    /// Invalid message encoding.
    Invalid,
    /// Remote only sent a partial frame before closing.
    CorruptedFrame,
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let error_msg = match self.kind() {
            ErrorKind::Incomplete => "Not enough data available in read buffer to parse message.",
            ErrorKind::Invalid => "Invalid message encoding.",
            ErrorKind::CorruptedFrame => "Remote connection closed during sending of a frame",
            ErrorKind::Other(s) => s,
        };
        write!(f, "{}", error_msg)
    }
}

impl From<IoError> for Error {
    fn from(error: IoError) -> Self {
        Error::new(ErrorKind::Other(error.to_string()))
    }
}

impl std::error::Error for Error {}

/// Attempt to move the cursor forward `n` bytes.
///
/// If the cursor can move forward less than `n` bytes an `Incomplete` error is returned.
pub fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    println!("Skip");
    if src.remaining() < n {
        Err(Error::new(ErrorKind::Incomplete))
    } else {
        src.advance(n);
        Ok(())
    }
}

/// Calulates the size of the payload in bytes.
pub fn get_payload_length(src: &mut Cursor<&[u8]>) -> Result<usize, Error> {
    println!("Get payload");
    let line = get_line(src)?;
    let decoded: usize = bincode::deserialize(&line[..]).unwrap();
    Ok(decoded)
}

/// Attempt to read a line from the buffer; lines are terminated with `\r\m`.
///
/// Receives an exclusive reference to a cursor which references the underlying read buffer.
/// Returns an appropiate reference to the slice of the read buffer
pub fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    println!("Get line");
    // Get current position of cursor
    let start = src.position() as usize;
    // Get reference to buffer under the cursor to get length
    let end = src.get_ref().len() - 1;
    // Search buffer up to this point
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // Line is found, move to position after this line
            src.set_position((i + 2) as u64);
            // Return reference to line in buffer
            return Ok(&src.get_ref()[start..i]);
        }
    }
    // Not enough data in buffer to read complete line
    Err(Error::new(ErrorKind::Incomplete))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn validate_buffer_with_complete_frame() {
        // buffer
        let mut buf = BytesMut::with_capacity(4 * 1024);
        buf.put(&b"$"[..]);
        let len: usize = 2;
        let encoded: Vec<u8> = bincode::serialize(&len).unwrap();
        buf.put(&encoded[..]);
        buf.put(&b"\r\n"[..]);
        buf.put(&b"ok"[..]);
        buf.put(&b"\r\n"[..]);

        // cursor
        let mut buff = Cursor::new(&buf[..]);

        assert_eq!(validate(&mut buff).unwrap(), ());
    }

    #[test]
    fn validate_buffer_with_partial_frame() {
        // buffer
        let mut buf = BytesMut::with_capacity(4 * 1024);
        buf.put(&b"$"[..]);
        let len: usize = 2;
        let encoded: Vec<u8> = bincode::serialize(&len).unwrap();
        buf.put(&encoded[..]);
        buf.put(&b"\r\n"[..]);
        buf.put(&b"o"[..]);

        // cursor
        let mut buff = Cursor::new(&buf[..]);

        assert_eq!(
            validate(&mut buff).err().unwrap().kind(),
            ErrorKind::Incomplete
        );
    }

    #[test]
    fn validate_buffer_with_incorrect_encoding() {
        // buffer
        let mut buf = BytesMut::with_capacity(4 * 1024);
        buf.put(&b"&"[..]);
        let len: usize = 2;
        let encoded: Vec<u8> = bincode::serialize(&len).unwrap();
        buf.put(&encoded[..]);
        buf.put(&b"\r\n"[..]);
        buf.put(&b"ok"[..]);
        buf.put(&b"\r\n"[..]);

        // cursor
        let mut buff = Cursor::new(&buf[..]);

        assert_eq!(
            validate(&mut buff).err().unwrap().kind(),
            ErrorKind::Invalid
        );
    }

    #[test]
    fn parse_validated_buffer() {
        // buffer
        let mut buf = BytesMut::with_capacity(4 * 1024);
        buf.put(&b"$"[..]);
        let len: usize = 2;
        let encoded: Vec<u8> = bincode::serialize(&len).unwrap();
        buf.put(&encoded[..]);
        buf.put(&b"\r\n"[..]);
        buf.put(&b"ok"[..]);
        buf.put(&b"\r\n"[..]);

        // cursor
        let mut buff = Cursor::new(&buf[..]);

        // expected frame
        let f = Frame::new(Bytes::copy_from_slice(&b"ok"[..]));

        assert_eq!(parse(&mut buff).unwrap(), f);
    }
}
