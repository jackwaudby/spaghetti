#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash, PartialOrd, Ord)]
pub enum TatpPrimaryKey {
    /// (s_id)
    Subscriber(u64),

    /// (s_id, ai_type)
    AccessInfo(u64, u64),

    /// (s_id, sf_type)
    SpecialFacility(u64, u64),

    /// (s_id, sf_type, start_time)
    CallForwarding(u64, u64, u64),
}