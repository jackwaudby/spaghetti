use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// Basic-SGT error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum BasicSerializationGraphTestingError {
    /// Cascading abort.
    CascadingAbort,

    /// Cycle found.
    CycleFound,

    /// No free nodes in the graph.
    NoSpaceInGraph,

    /// Transaction ID field already set on node.
    TransactionIdAlreadySet,

    /// Edge already exists between two nodes.
    EdgeAlreadyExists,

    /// Self edge
    SelfEdge,

    /// Serializable error
    NonSerializable,

    /// Transaction ID not set
    TransactionIdNotSet,

    /// Node expectedly free
    NodeStateUnexpectedlyFree,

    /// Parent node aborted
    ParentAborted,
}

impl Error for BasicSerializationGraphTestingError {}

impl fmt::Display for BasicSerializationGraphTestingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use BasicSerializationGraphTestingError::*;
        let err_msg = match *self {
            CascadingAbort => "cascading abort",
            CycleFound => "cycle found",
            NoSpaceInGraph => "no nodes free in graph",
            TransactionIdAlreadySet => "transaction id field already set",
            EdgeAlreadyExists => "edge already exists between two nodes",
            SelfEdge => "attempted to insert self edge",
            NonSerializable => "Serializable error",
            TransactionIdNotSet => "transaction id not set",
            NodeStateUnexpectedlyFree => "node state unexpectedly free",
            ParentAborted => "parent node aborted",
        };
        write!(f, "{}", err_msg)
    }
}

#[cfg(test)]
mod tests {
    //    use super::*;

    #[test]
    fn sgt_error_test() {
        // let e1 =
        //     SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::NoSpaceInGraph);
        // let e2 = SerializationGraphTestingError::new(
        //     SerializationGraphTestingErrorKind::TransactionIdAlreadySet,
        // );
        // let e3 =
        //     SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::EdgeExists);
        // let e4 = SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::SelfEdge);
        // let e5 = SerializationGraphTestingError::new(
        //     SerializationGraphTestingErrorKind::MutexLockFailed,
        // );
        // let e6 =
        //     SerializationGraphTestingError::new(SerializationGraphTestingErrorKind::RwLockFailed);
        // let e7 = SerializationGraphTestingError::new(
        //     SerializationGraphTestingErrorKind::SerializableError,
        // );
        // let e8 = SerializationGraphTestingError::new(
        //     SerializationGraphTestingErrorKind::TransactionIdNotSet,
        // );
        // assert_eq!(format!("{}", e1), format!("no nodes free in graph"));
        // assert_eq!(
        //     format!("{}", e2),
        //     format!("transaction id field already set")
        // );
        // assert_eq!(
        //     format!("{}", e3),
        //     format!("edge already exists between two nodes")
        // );
        // assert_eq!(format!("{}", e4), format!("attempted to insert self edge"));
        // assert_eq!(format!("{}", e5), format!("locking mutex failed"));

        // assert_eq!(format!("{}", e6), format!("locking rw lock failed"));
        // assert_eq!(format!("{}", e7), format!("Serializable error"));
        // assert_eq!(format!("{}", e8), format!("transaction id not set"));
    }
}
