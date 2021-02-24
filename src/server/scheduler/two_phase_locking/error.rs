use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// 2PL specific error.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TwoPhaseLockingError {
    kind: TwoPhaseLockingErrorKind,
}

impl TwoPhaseLockingError {
    /// Create new `TwoPhaseLockingError.
    pub fn new(kind: TwoPhaseLockingErrorKind) -> TwoPhaseLockingError {
        TwoPhaseLockingError { kind }
    }
}

/// 2PL error types.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TwoPhaseLockingErrorKind {
    NotRegisteredInActiveTransactions,
    AlreadyRegisteredInActiveTransactions(String),
    LockRequestDenied,
    LockNotInTable(String),
    LockAlreadyInTable(String),
}

impl fmt::Display for TwoPhaseLockingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl Error for TwoPhaseLockingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.kind)
    }
}

impl Error for TwoPhaseLockingErrorKind {}

impl fmt::Display for TwoPhaseLockingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TwoPhaseLockingErrorKind::*;
        match *self {
            AlreadyRegisteredInActiveTransactions(ref s) => {
                write!(f, "{} already registered in active transaction table", s)
            }
            LockRequestDenied => write!(f, "lock request for denied"),
            NotRegisteredInActiveTransactions => {
                write!(f, "transaction not registered in active transaction table")
            }
            LockNotInTable(ref s) => write!(f, "no lock in table for {} ", s),
            LockAlreadyInTable(ref s) => write!(f, "no lock in table for {} ", s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpl_error_test() {
        let e1 = TwoPhaseLockingError::new(
            TwoPhaseLockingErrorKind::AlreadyRegisteredInActiveTransactions("t".to_string()),
        );

        let e2 = TwoPhaseLockingError::new(TwoPhaseLockingErrorKind::LockRequestDenied);
        let e3 =
            TwoPhaseLockingError::new(TwoPhaseLockingErrorKind::NotRegisteredInActiveTransactions);

        assert_eq!(
            format!("{}", e1),
            format!("t already registered in active transaction table")
        );

        assert_eq!(format!("{}", e2), format!("lock request for denied"));

        assert_eq!(
            format!("{}", e3),
            format!("transaction not registered in active transaction table")
        );
    }
}
