use std::fmt;

/// Primary keys of each table in the SmallBank workload.
#[derive(Debug, PartialEq, Clone, Eq)]
pub enum SmallBankPrimaryKey {
    /// Account (CustomerID int PK)
    Account(u64),

    /// Savings (CustomerID int PK, Balance int)
    Savings(u64),

    /// Checking (CustomerID int PK Balance int)
    Checking(u64),
}

impl fmt::Display for SmallBankPrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SmallBankPrimaryKey::*;
        match &self {
            Account(id) => write!(f, "{}", id),
            Savings(id) => write!(f, "{}", id),
            Checking(id) => write!(f, "{}", id),
        }
    }
}
