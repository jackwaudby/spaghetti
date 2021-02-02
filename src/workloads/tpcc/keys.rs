#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash)]
pub enum TpccPrimaryKey {
    Warehouse(u64),
    Item(u64),
    Stock(u64, u64),
    District(u64, u64),
    Customer(u64, u64, u64),
}
