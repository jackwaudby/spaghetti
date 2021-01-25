use crate::frame::Frame;
use crate::transaction::Transaction;
use crate::workloads::tatp::TatpTransaction;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::any::Any;

use std::fmt::Debug;

pub type Message = Box<dyn Transaction + Sync + Send + 'static>;

impl Debug for Message {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Convert trait object to concrete type.
        match self.as_any().downcast_ref::<TatpTransaction>() {
            Some(tatp) => return write!(f, "{:?}", tatp),
            None => {}
        }

        match self.as_any().downcast_ref::<CloseConnection>() {
            Some(close) => write!(f, "{:?}", close),
            None => write!(f, "Unable to cast to concrete type."),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CloseConnection;

impl Transaction for CloseConnection {
    fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ConnectionClosed;

impl Transaction for ConnectionClosed {
    fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Response {
    pub payload: String,
}

impl Transaction for Response {
    fn into_frame(&self) -> Frame {
        // Serialize transaction
        let s: Bytes = bincode::serialize(&self).unwrap().into();
        // Create frame
        Frame::new(s)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
