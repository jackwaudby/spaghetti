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

// impl std::hash::Hash for SmallBankPrimaryKey {
//     fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
//         use SmallBankPrimaryKey::*;
//         match &self {
//             Account(id) => hasher.write_u64(*id),
//             Savings(id) => hasher.write_u64(*id),
//             Checking(id) => hasher.write_u64(*id),
//         }
//     }
// }

// impl nohash_hasher::IsEnabled for SmallBankPrimaryKey {}
