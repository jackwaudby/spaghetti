use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// SGT specific error.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SerializationGraphTestingError {
    kind: SerializationGraphTestingErrorKind,
}

impl SerializationGraphTestingError {
    /// Create new SGT error.
    pub fn new(kind: SerializationGraphTestingErrorKind) -> SerializationGraphTestingError {
        SerializationGraphTestingError { kind }
    }
}

/// SGT error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SerializationGraphTestingErrorKind {
    /// No free nodes in the graph.
    NoSpaceInGraph,
    /// Transaction ID field already set on node.
    TransactionIdAlreadySet,
    /// Edge already exists between two nodes.
    EdgeExists,
    /// Self edge
    SelfEdge,
    /// Locking mutex failed
    MutexLockFailed,
    /// Locking rw lock failed
    RwLockFailed,
    /// Serializable error
    SerializableError,
}

impl fmt::Display for SerializationGraphTestingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl Error for SerializationGraphTestingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.kind)
    }
}

impl Error for SerializationGraphTestingErrorKind {}

impl fmt::Display for SerializationGraphTestingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SerializationGraphTestingErrorKind::*;
        let err_msg = match *self {
            NoSpaceInGraph => "no nodes free in graph",
            TransactionIdAlreadySet => "transaction id field already set",
            EdgeExists => "edge already exists between two nodes",
            SelfEdge => "attempted to insert self edge",
            MutexLockFailed => "locking mutex failed",
            RwLockFailed => "locking rw lock failed",
            SerializableError => "Serializable error",
        };
        write!(f, "{}", err_msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpl_error_test() {
        let e1 =
            SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::NoSpaceInGraph);

        assert_eq!(format!("{}", e1), format!("no nodes free in graph"));
    }
}
