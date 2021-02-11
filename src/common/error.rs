use crate::common::frame::ParseError;
use crate::server::scheduler::serialization_graph_testing::error::SerializationGraphTestingError;
use crate::server::scheduler::two_phase_locking::error::TwoPhaseLockingError;

use serde::{Deserialize, Serialize};
use std::error;
use std::fmt;

// Represents a Spaghetti error.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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
    ColumnNotFound(String),
    /// Unexpected message received.
    UnexpectedMessage,
    /// Connection unexpectedly closed.
    ConnectionUnexpectedlyClosed,
    /// Two phase locking error.
    TwoPhaseLocking(TwoPhaseLockingError),
    /// SGT error.
    SerializationGraphTesting(SerializationGraphTestingError),
    /// Row already exists in index.
    RowAlreadyExists,
    /// Row does not exist in index.
    RowDoesNotExist,
    /// Invalid column type.
    InvalidColumnType,
    /// Row dirty.
    RowDirty,
    // Access history not initalised.
    NotTrackingAccessHistory,
}

impl fmt::Display for SpaghettiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SpaghettiError::*;
        match *self {
            Incomplete => write!(
                f,
                "not enough data available in read buffer to parse message."
            ),
            Invalid => write!(f, "invalid message encoding."),
            CorruptedFrame => write!(f, "remote connection closed during sending of a frame"),
            IncorrectWorkload => write!(f, "workload not recognised"),
            Parse(ref e) => write!(f, "parsing error {:?}", e),
            TableNotFound => write!(f, "table not found"),
            IndexNotFound => write!(f, "index not found"),
            NoPrimaryIndex => write!(f, "no primary index on table"),
            ColumnNotFound(ref c) => write!(f, "column not found: {:?}", c),
            UnexpectedMessage => write!(f, "unexpected message"),
            ConnectionUnexpectedlyClosed => write!(f, "connection unexpectedly closed"),
            TwoPhaseLocking(ref e) => write!(f, "{}", e),
            SerializationGraphTesting(ref e) => write!(f, "{}", e),
            RowAlreadyExists => write!(f, "row already exists in index."),
            RowDoesNotExist => write!(f, "row does not exist in index."),
            InvalidColumnType => write!(f, "invalid column type"),
            RowDirty => write!(f, "row already dirty"),
            NotTrackingAccessHistory => write!(f, "not tracking access history"),
        }
    }
}

impl error::Error for SpaghettiError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        use SpaghettiError::*;
        match *self {
            Parse(ref e) => Some(e),
            TwoPhaseLocking(ref e) => Some(e),
            _ => Some(self),
        }
    }
}

impl From<ParseError> for SpaghettiError {
    fn from(error: ParseError) -> Self {
        SpaghettiError::Parse(error)
    }
}

impl From<TwoPhaseLockingError> for SpaghettiError {
    fn from(error: TwoPhaseLockingError) -> Self {
        SpaghettiError::TwoPhaseLocking(error)
    }
}

impl From<SerializationGraphTestingError> for SpaghettiError {
    fn from(error: SerializationGraphTestingError) -> Self {
        SpaghettiError::SerializationGraphTesting(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpl_error_test() {
        let e1 = SpaghettiError::Incomplete;
        let e2 = SpaghettiError::Invalid;
        let e3 = SpaghettiError::CorruptedFrame;
        let e4 = SpaghettiError::IncorrectWorkload;
        let e5 = SpaghettiError::TableNotFound;
        let e6 = SpaghettiError::IndexNotFound;
        let e7 = SpaghettiError::NoPrimaryIndex;
        let e8 = SpaghettiError::UnexpectedMessage;
        let e9 = SpaghettiError::ConnectionUnexpectedlyClosed;
        let e10 = SpaghettiError::RowAlreadyExists;
        let e11 = SpaghettiError::RowDoesNotExist;
        let e12 = SpaghettiError::InvalidColumnType;
        let e13 = SpaghettiError::RowDirty;
        let e14 = SpaghettiError::NotTrackingAccessHistory;

        assert_eq!(
            format!("{}", e1),
            format!("not enough data available in read buffer to parse message.")
        );

        assert_eq!(format!("{}", e2), format!("invalid message encoding."));
        assert_eq!(
            format!("{}", e3),
            format!("remote connection closed during sending of a frame")
        );
        assert_eq!(format!("{}", e4), format!("workload not recognised"));
        assert_eq!(format!("{}", e5), format!("table not found"));
        assert_eq!(format!("{}", e6), format!("index not found"));
        assert_eq!(format!("{}", e7), format!("no primary index on table"));
        assert_eq!(format!("{}", e8), format!("unexpected message"));
        assert_eq!(format!("{}", e9), format!("connection unexpectedly closed"));
        assert_eq!(format!("{}", e10), format!("row already exists in index."));
        assert_eq!(format!("{}", e11), format!("row does not exist in index."));
        assert_eq!(format!("{}", e12), format!("invalid column type"));
        assert_eq!(format!("{}", e13), format!("row already dirty"));
        assert_eq!(format!("{}", e14), format!("not tracking access history"));
    }
}
