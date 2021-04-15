/// Primary keys of each table in the TATP workload.
#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash)]
pub enum AcidPrimaryKey {
    /// (p_id)
    Person(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acid_key() {
        assert!(AcidPrimaryKey::Person(1) == AcidPrimaryKey::Person(1));
        assert!(AcidPrimaryKey::Person(1) != AcidPrimaryKey::Person(2));
    }
}
