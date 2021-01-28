//! There are a few ways to handle error types.
//! (1) Define an error type to mask all other error types, convert all types to spaghetti's error
//! type.
//! (2) Box errors which preserve the underlying errors. This is to be avoided in any hot path.
//! (3) Wrap errors with spaghetti's error type, defining an enum of causes

use crate::common::frame::ParseError;
use std::error;
use std::fmt;

// Represents a Spaghetti error.
#[derive(Debug, PartialEq)]
pub enum SpaghettiError {
    /// Not enough data available in read buffer to parse message.
    Incomplete,
    /// Invalid message encoding.
    Invalid,
    /// Remote only sent a partial frame before closing.
    CorruptedFrame,
    /// Parsing error.
    Parse(ParseError),
    /// Workload not recognised
    IncorrectWorkload,
    /// Table does not exist
    TableNotFound,
    /// Index does not exist
    IndexNotFound,
    /// No primary index on table.
    NoPrimaryIndex,
    /// Column does not exist in table.
    ColumnNotFound,
    /// Unexpected message received.
    UnexpectedMessage,
    /// Connection unexpectedly closed.
    ConnectionUnexpectedlyClosed,
}

impl fmt::Display for SpaghettiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SpaghettiError::*;
        match *self {
            Incomplete => write!(
                f,
                "Not enough data available in read buffer to xbparse message."
            ),
            Invalid => write!(f, "Invalid message encoding."),
            CorruptedFrame => write!(f, "Remote connection closed during sending of a frame"),
            IncorrectWorkload => write!(f, "Workload not recognised"),
            Parse(_) => write!(f, "Parsing error"),
            TableNotFound => write!(f, "Table not found"),
            IndexNotFound => write!(f, "Index not found"),
            NoPrimaryIndex => write!(f, "No primary index on table"),
            ColumnNotFound => write!(f, "Column not found"),
            UnexpectedMessage => write!(f, "Unexpected message"),
            ConnectionUnexpectedlyClosed => write!(f, "Connection unexpectedly closed"),
        }
    }
}

impl error::Error for SpaghettiError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        use SpaghettiError::*;
        match *self {
            ColumnNotFound => Some(self),
            Parse(_) => Some(self),
            _ => unimplemented!(),
        }
    }
}

// Implement the conversion from ParseError to SpaghettiError
impl From<ParseError> for SpaghettiError {
    fn from(error: ParseError) -> Self {
        SpaghettiError::Parse(error)
    }
}
