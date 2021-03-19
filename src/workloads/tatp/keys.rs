/// Primary keys of each table in the TATP workload.
#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash)]
pub enum TatpPrimaryKey {
    /// (s_id)
    Subscriber(u64),

    /// (s_id,ai_type)
    AccessInfo(u64, u64),

    /// (s_id,sf_type)
    SpecialFacility(u64, u64),

    /// (s_id,sf_type,start_time)
    CallForwarding(u64, u64, u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_test() {
        assert!(TatpPrimaryKey::CallForwarding(1, 1, 1) == TatpPrimaryKey::CallForwarding(1, 1, 1));
        assert!(TatpPrimaryKey::SpecialFacility(1, 1) != TatpPrimaryKey::AccessInfo(1, 1));
    }
}
