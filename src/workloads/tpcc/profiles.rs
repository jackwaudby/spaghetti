use serde::{Deserialize, Serialize};
///////////////////////////////////////
/// Transaction Profiles. ///
//////////////////////////////////////
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct NewOrder {
    pub w_id: u64, // home warehouse id
                   // d_id: u64,
                   // c_id: u64,
                   // ol_cnt: u64, // number of lines in order
                   // rbk: u64,
                   // indicates a rollback when = 1
                   // orderlines: Vec<OrderLine>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Payment {}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TpccTransaction {
    NewOrder(NewOrder),
    Payment(Payment),
}
