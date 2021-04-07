//! There are two types errors, (i) fatal errors, and (ii) non-fatal errors.
//! Fatal errors, result in the termination of the database.
//! Non-fatal errors, are reasons for transactions to abort.
use crate::common::frame::ParseError;
use crate::server::scheduler::hit_list::error::HitListError;
use crate::server::scheduler::serialization_graph_testing::error::SerializationGraphTestingError;
use crate::server::scheduler::two_phase_locking::error::TwoPhaseLockingError;

use serde::{Deserialize, Serialize};
use std::error;
use std::fmt;

/// Represents a fatal error.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum FatalError {
    /// Not enough data available in read buffer to parse message.
    Incomplete,

    /// Invalid message encoding.
    Invalid,

    /// Remote only sent a partial frame before closing.
    CorruptedFrame,

    /// Parsing error.
    Parse(ParseError),

    /// Workload not recognised
    IncorrectWorkload(String),

    /// Unexpected message received.
    UnexpectedMessage,

    /// Connection unexpectedly closed.
    ConnectionUnexpectedlyClosed,

    /// Two phase locking error.
    TwoPhaseLocking(TwoPhaseLockingError),

    /// SGT error.
    SerializationGraphTesting(SerializationGraphTestingError),

    /// Access history not initalised.
    NotTrackingAccessHistory,

    /// TCP socket read half unexpectedly closed.
    ReadSocketUnexpectedlyClosed,

    /// TCP socket write half unexpectedly closed.
    WriteHandlerUnexpectedlyClosed,

    /// Thread pool closed.
    ThreadPoolClosed,

    /// Invalid column type.
    InvalidColumnType(String),

    /// Unable to convert values to result string
    UnableToConvertToResultString,

    /// Generator mode recognised
    IncorrectGeneratorMode(String),
}

/// Represents a non-fatal error.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NonFatalError {
    /// Table not found in database.
    TableNotFound(String),

    /// Index not found in database.
    IndexNotFound(String),

    /// Table does not have a primary index.
    NoPrimaryIndex(String),

    /// Row does not exist in index.
    RowNotFound(String, String),

    /// Column does not exist in table.
    ColumnNotFound(String),

    /// Unable to initalise a row.
    UnableToInitialiseRow(String, String, String),

    /// Row already exists in index.
    RowAlreadyExists(String, String),

    /// Row dirty.
    RowDirty(String, String),

    /// Row deleted.
    RowDeleted(String, String),

    /// Unable to convert to spaghetti data type.
    UnableToConvertToDataType(String, String),

    /// Unable to convert from spaghetti data type
    UnableToConvertFromDataType(String, String),

    /// Invalid column type.
    InvalidColumnType(String),

    /// TODO: Replace with hit list.
    NonSerializable,

    /// Two phase locking error.
    TwoPhaseLocking(TwoPhaseLockingError),

    /// SGT error.
    SerializationGraphTesting(SerializationGraphTestingError),

    /// Hit-list error.
    HitList(HitListError),
}

impl fmt::Display for FatalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use FatalError::*;
        match *self {
            Incomplete => write!(
                f,
                "not enough data available in read buffer to parse message."
            ),
            Invalid => write!(f, "invalid message encoding."),
            CorruptedFrame => write!(f, "remote connection closed during sending of a frame"),
            Parse(ref e) => write!(f, "parsing error {:?}", e),
            IncorrectWorkload(ref workload) => write!(f, "workload not recognised: {}", workload),
            UnexpectedMessage => write!(f, "unexpected message"),
            ConnectionUnexpectedlyClosed => write!(f, "connection unexpectedly closed"),
            TwoPhaseLocking(ref e) => write!(f, "{}", e),
            SerializationGraphTesting(ref e) => write!(f, "{}", e),
            NotTrackingAccessHistory => write!(f, "not tracking access history"),
            ReadSocketUnexpectedlyClosed => write!(f, "read socket unexpectedly closed"),
            WriteHandlerUnexpectedlyClosed => write!(
                f,
                "channel between read and write handler unexpectedly closed"
            ),
            ThreadPoolClosed => write!(f, "thread pool is closed"),
            UnableToConvertToResultString => write!(f, "unable to convert values to result string"),
            InvalidColumnType(ref col_type) => write!(f, "invalid: column type {}", col_type),
            IncorrectGeneratorMode(ref mode) => {
                write!(f, "generator mode not recognised: {}", mode)
            }
        }
    }
}

impl fmt::Display for NonFatalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use NonFatalError::*;
        match *self {
            TableNotFound(ref name) => write!(f, "not found: table {}", name),
            IndexNotFound(ref name) => write!(f, "not found: index {}", name),
            NoPrimaryIndex(ref name) => write!(f, "not found: no index on table {}", name),
            ColumnNotFound(ref name) => write!(f, "not found: column {}", name),
            RowNotFound(ref key, ref index) => write!(f, "not found: {} in {}", key, index),
            UnableToInitialiseRow(ref table, ref column, ref value) => write!(
                f,
                "unable to initalise: column {} in table {} with value {}",
                column, table, value
            ),
            HitList(ref e) => write!(f, "{}", e),
            TwoPhaseLocking(ref e) => write!(f, "{}", e),
            SerializationGraphTesting(ref e) => write!(f, "{}", e),
            RowAlreadyExists(ref key, ref index) => {
                write!(f, "already exists: {} in {}", key, index)
            }
            InvalidColumnType(ref col_type) => write!(f, "invalid: column type {}", col_type),
            RowDirty(ref key, ref table) => write!(f, "dirty: {} in table {}", key, table),
            RowDeleted(ref key, ref table) => write!(f, "deleted: {} in table {}", key, table),
            UnableToConvertToDataType(ref value, ref spaghetti_type) => write!(
                f,
                "unable to convert: value {} to type {}",
                value, spaghetti_type
            ),
            UnableToConvertFromDataType(ref spaghetti_type, ref value) => write!(
                f,
                "unable to convert: type {} to type {}",
                spaghetti_type, value
            ),
            NonSerializable => write!(f, "non-serializable behaviour"),
        }
    }
}

impl std::error::Error for NonFatalError {}

impl error::Error for FatalError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        use FatalError::*;
        match *self {
            Parse(ref e) => Some(e),
            TwoPhaseLocking(ref e) => Some(e),
            _ => Some(self),
        }
    }
}

impl From<ParseError> for FatalError {
    fn from(error: ParseError) -> Self {
        FatalError::Parse(error)
    }
}

impl From<TwoPhaseLockingError> for FatalError {
    fn from(error: TwoPhaseLockingError) -> Self {
        FatalError::TwoPhaseLocking(error)
    }
}

impl From<TwoPhaseLockingError> for NonFatalError {
    fn from(error: TwoPhaseLockingError) -> Self {
        NonFatalError::TwoPhaseLocking(error)
    }
}

impl From<SerializationGraphTestingError> for FatalError {
    fn from(error: SerializationGraphTestingError) -> Self {
        FatalError::SerializationGraphTesting(error)
    }
}

impl From<SerializationGraphTestingError> for NonFatalError {
    fn from(error: SerializationGraphTestingError) -> Self {
        NonFatalError::SerializationGraphTesting(error)
    }
}

impl From<HitListError> for NonFatalError {
    fn from(error: HitListError) -> Self {
        NonFatalError::HitList(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpl_error_test() {
        let e1 = FatalError::Incomplete;
        let e2 = FatalError::Invalid;
        let e3 = FatalError::CorruptedFrame;
        let e8 = FatalError::UnexpectedMessage;
        let e9 = FatalError::ConnectionUnexpectedlyClosed;
        let e14 = FatalError::NotTrackingAccessHistory;

        assert_eq!(
            format!("{}", e1),
            format!("not enough data available in read buffer to parse message.")
        );

        assert_eq!(format!("{}", e2), format!("invalid message encoding."));
        assert_eq!(
            format!("{}", e3),
            format!("remote connection closed during sending of a frame")
        );
        assert_eq!(format!("{}", e8), format!("unexpected message"));
        assert_eq!(format!("{}", e9), format!("connection unexpectedly closed"));
        assert_eq!(format!("{}", e14), format!("not tracking access history"));
    }
}
