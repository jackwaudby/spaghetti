use crate::scheduler::owh::error::OptimisedWaitHitError;
use crate::scheduler::sgt::error::SerializationGraphError;
use crate::scheduler::tpl::error::TwoPhaseLockingError;
use crate::scheduler::wh::error::WaitHitError;
use crate::workloads::smallbank::error::SmallBankError;

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

    /// Workload not recognised
    IncorrectWorkload(String),

    /// Unexpected message received.
    UnexpectedMessage,

    /// Connection unexpectedly closed.
    ConnectionUnexpectedlyClosed,

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
    RowDirty(String),

    /// Row deleted.
    RowDeleted(String, String),

    /// Unable to convert to spaghetti data type.
    UnableToConvertToDataType(String, String),

    /// Unable to convert from spaghetti data type
    UnableToConvertFromDataType(String, String),

    /// Invalid column type.
    InvalidColumnType(String),

    /// Manual abort. Used in ACID test.
    NonSerializable,

    /// Smallbank error.
    SmallBankError(SmallBankError),

    SerializationGraph(SerializationGraphError),

    WaitHitError(WaitHitError),

    OptimisedWaitHitError(OptimisedWaitHitError),

    TwoPhaseLockingError(TwoPhaseLockingError),
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
            IncorrectWorkload(ref workload) => write!(f, "workload not recognised: {}", workload),
            UnexpectedMessage => write!(f, "unexpected message"),
            ConnectionUnexpectedlyClosed => write!(f, "connection unexpectedly closed"),
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
            RowAlreadyExists(ref key, ref index) => {
                write!(f, "already exists: {} in {}", key, index)
            }
            InvalidColumnType(ref col_type) => write!(f, "invalid: column type {}", col_type),
            RowDirty(ref tid) => write!(f, "row dirty, modified by: {}", tid),
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
            SmallBankError(ref e) => write!(f, "{}", e),
            SerializationGraph(ref e) => write!(f, "{}", e),
            WaitHitError(ref e) => write!(f, "{}", e),
            OptimisedWaitHitError(ref e) => write!(f, "{}", e),
            TwoPhaseLockingError(ref e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for NonFatalError {}

impl error::Error for FatalError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // use FatalError::*;
        match *self {
            // TwoPhaseLocking(ref e) => Some(e),
            _ => Some(self),
        }
    }
}

impl From<SerializationGraphError> for NonFatalError {
    fn from(error: SerializationGraphError) -> Self {
        NonFatalError::SerializationGraph(error)
    }
}

impl From<SmallBankError> for NonFatalError {
    fn from(error: SmallBankError) -> Self {
        NonFatalError::SmallBankError(error)
    }
}

impl From<WaitHitError> for NonFatalError {
    fn from(error: WaitHitError) -> Self {
        NonFatalError::WaitHitError(error)
    }
}

impl From<OptimisedWaitHitError> for NonFatalError {
    fn from(error: OptimisedWaitHitError) -> Self {
        NonFatalError::OptimisedWaitHitError(error)
    }
}

impl From<TwoPhaseLockingError> for NonFatalError {
    fn from(error: TwoPhaseLockingError) -> Self {
        NonFatalError::TwoPhaseLockingError(error)
    }
}
