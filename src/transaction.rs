use crate::frame::Frame;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Transaction {
    GetSubscriberData { s_id: u32 },
    GetNewDestination,
}

impl Transaction {
    pub fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
}

// use serde::{Deserialize, Serialize};

// /// Retrieve one row from the Subscriber table.
// ///
// /// The search key is s_id (primary key).
// /// The value range of s_id is [1,P], where P is the size of the Subscriber table.
// /// All the s_id values in the range [1,P] exist in the table.
// /// For each transaction, s_id is randomly selected from [1,P]. The default is the non-uniform key distribution.
// #[derive(Serialize, Deserialize, PartialEq, Debug)]
// struct GetSubscriberData {
//     /// Subscriber id.
//     s_id: u32,
// }

// impl GetSubscriberData {
//     /// Create a new ؀`GetSubscriberData` transaction.
//     pub fn new() -> GetSubscriberData {
//         // TODO: always return s_id = 1
//         GetSubscriberData { s_id: 1 }
//     }

//     /// Get subscriber id.
//     pub fn s_id(&self) -> u32 {
//         self.s_id
//     }

//     /// Convert transaction into `Frame`.
//     pub fn into_frame(self) -> Frame {}

//     /// Parse a `GetSubscriberData` from a `Frame`
//     // TODO
//     pub fn parse_frame() {}

//     /// Execute stored procedure.
//     ///
//     /// Called by server.
//     pub fn execute() {}
// }

// pub enum Frame {
//     Bulk(Bytes),
//     Error(String),
//     Array(Vec<Frame>),
// }

// impl Frame {}

// fn main() {
//     let t = Transaction::GetAccessData {
//         s_id: 10,
//         ai_type: 1,
//     };

//     let encoded: Vec<u8> = bincode::serialize(&t).unwrap();
//     assert_eq!(encoded.len(), 9);

//     let decoded: Transaction = bincode::deserialize(&encoded[..]).unwrap();
//     assert_eq!(t, decoded);
// }
