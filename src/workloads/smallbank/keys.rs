/// Primary keys of each table in the SmallBank workload.
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub enum SmallBankPrimaryKey {
    /// Account (Name string PK, CustomerID int)
    Account(String),

    /// Savings (CustomerID int PK, Balance int)
    Savings(u64),

    /// Checking (CustomerID int PK Balance int)
    Checking(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smallbank_key_test() {
        assert!(
            SmallBankPrimaryKey::Account("name".to_string)
                == SmallBankPrimaryKey::Account("name".to_string)
        );
        assert!(SmallBankPrimaryKey::Savings(1) != SmallBankPrimaryKey::Checking(1));
    }
}
