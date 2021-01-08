//! There are a few ways to handle error types.
//! (1) Define an error type to mask all other error types, convert all types to spaghetti's error
//! type.
//! (2) Box errors which preserve the underlying errors. This is to be avoided in any hot path.
//! (3) Wrap errors with spaghetti's error type, defining an enum of causes

use std::error;
use std::error::Error;
use std::fmt;
use tokio::io::Error as TokioIoError;

// Represents a Spaghetti error.
#[derive(Debug)]
pub enum SpaghettiError {
    /// Not enough data available in read buffer to parse message.
    Incomplete,
    /// Invalid message encoding.
    Invalid,
    /// Remote only sent a partial frame before closing.
    CorruptedFrame,
    /// Tokio errors
    Tokio(TokioIoError),
}

impl fmt::Display for SpaghettiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SpaghettiError::Incomplete => write!(
                f,
                "Not enough data available in read buffer to parse message."
            ),
            SpaghettiError::Invalid => write!(f, "Invalid message encoding."),
            SpaghettiError::CorruptedFrame => {
                write!(f, "Remote connection closed during sending of a frame")
            }
            SpaghettiError::Tokio(..) => write!(f, "Tokio error"),
        }
    }
}

impl error::Error for SpaghettiError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            SpaghettiError::Incomplete => None,
            SpaghettiError::CorruptedFrame => None,
            SpaghettiError::Invalid => None,
            // The cause is the underlying implementation error type.
            // Implicitly cast to the trait object as already implements the Error trait.
            SpaghettiError::Tokio(ref e) => Some(e),
        }
    }
}

// Implement the conversion from tokio::io::Error to Error
impl From<tokio::io::Error> for SpaghettiError {
    fn from(error: TokioIoError) -> Self {
        SpaghettiError::Tokio(error)
    }
}
