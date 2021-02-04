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
    AlreadyRegisteredInActiveTransactions,
    LockRequestDenied,
}

impl fmt::Display for TwoPhaseLockingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TwoPhaseLockingErrorKind::*;
        let err_msg = match self.kind {
            AlreadyRegisteredInActiveTransactions => {
                "Transaction already registered in active transaction table"
            }
            LockRequestDenied => "Lock request denied",
            NotRegisteredInActiveTransactions => {
                "Transaction not registered in active transaction table"
            }
        };
        write!(f, "{}", err_msg)
    }
}

impl Error for TwoPhaseLockingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpl_error_test() {
        let e1 = TwoPhaseLockingError::new(
            TwoPhaseLockingErrorKind::AlreadyRegisteredInActiveTransactions,
        );

        let e2 = TwoPhaseLockingError::new(TwoPhaseLockingErrorKind::LockRequestDenied);
        let e3 =
            TwoPhaseLockingError::new(TwoPhaseLockingErrorKind::NotRegisteredInActiveTransactions);

        assert_eq!(
            format!("{}", e1),
            format!("Transaction already registered in active transaction table")
        );

        assert_eq!(format!("{}", e2), format!("Lock request denied"));

        assert_eq!(
            format!("{}", e3),
            format!("Transaction not registered in active transaction table")
        );
    }
}
