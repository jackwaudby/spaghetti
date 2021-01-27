use crate::common::frame::Frame;
use crate::workloads::tatp::TatpTransaction;
use crate::workloads::tpcc::TpccTransaction;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use std::fmt::Debug;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Message {
    CloseConnection,
    ConnectionClosed,
    Response(Response),
    TatpTransaction(TatpTransaction),
    TpccTransaction(TpccTransaction),
}

impl Message {
    pub fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Response {
    pub payload: String,
}
