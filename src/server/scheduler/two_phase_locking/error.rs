use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// 2PL error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TwoPhaseLockingError {
    /// Transaction not registered in list of active transactions.
    NotRegistered(String),

    /// Transaction is already registered in list og active transactions.
    AlreadyRegistered(String),

    /// Read lock request denied.
    ReadLockRequestDenied(String),

    /// Write lock request denied.
    WriteLockRequestDenied(String),

    /// Lock does not exist in lock table.
    LockNotInTable(String),

    /// Lock already exists
    LockAlreadyInTable(String),
}

impl fmt::Display for TwoPhaseLockingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TwoPhaseLockingError::*;
        match *self {
            NotRegistered(ref id) => write!(f, "{} not registered in active transaction table", id),
            AlreadyRegistered(ref id) => {
                write!(f, "{} already registered in active transaction table", id)
            }
            ReadLockRequestDenied(ref key) => write!(f, "read lock for {} denied", key),
            WriteLockRequestDenied(ref key) => write!(f, "write lock for {} denied", key),
            LockNotInTable(ref key) => write!(f, "no lock in table for {} ", key),
            LockAlreadyInTable(ref s) => write!(f, "no lock in table for {} ", s),
        }
    }
}

impl Error for TwoPhaseLockingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpl_error_test() {
        let e1 = TwoPhaseLockingError::AlreadyRegistered("t".to_string());
        let e2 = TwoPhaseLockingError::ReadLockRequestDenied("key".to_string());
        let e3 = TwoPhaseLockingError::WriteLockRequestDenied("key".to_string());
        let e4 = TwoPhaseLockingError::NotRegistered("t".to_string());

        assert_eq!(
            format!("{}", e1),
            format!("t already registered in active transaction table")
        );

        assert_eq!(format!("{}", e2), format!("read lock for key denied"));
        assert_eq!(format!("{}", e3), format!("write lock for key denied"));

        assert_eq!(
            format!("{}", e4),
            format!("t not registered in active transaction table")
        );
    }
}
