use serde::{Deserialize, Serialize};
use std::fmt;

///////////////////////////////////////
/// Transaction Profiles. ///
//////////////////////////////////////
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct GetSubscriberData {
    pub s_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct GetNewDestination {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct GetAccessData {
    pub s_id: u64,
    pub ai_type: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct UpdateSubscriberData {
    pub s_id: u64,
    pub sf_type: u8,
    pub bit_1: u8,
    pub data_a: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct UpdateLocationData {
    pub s_id: u64,
    pub vlr_location: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct InsertCallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
    pub number_x: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct DeleteCallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TatpTransaction {
    GetSubscriberData(GetSubscriberData),
    GetNewDestination(GetNewDestination),
    GetAccessData(GetAccessData),
    UpdateSubscriberData(UpdateSubscriberData),
    UpdateLocationData(UpdateLocationData),
    InsertCallForwarding(InsertCallForwarding),
    DeleteCallForwarding(DeleteCallForwarding),
}

impl fmt::Display for TatpTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TatpTransaction::*;
        match &*self {
            GetAccessData { .. } => write!(f, "GetAccessData"),
            GetNewDestination { .. } => write!(f, "GetNewDestination"),
            GetSubscriberData { .. } => write!(f, "GetSubscriberData"),
            UpdateLocationData { .. } => write!(f, "UpdateLocationData"),
            UpdateSubscriberData { .. } => write!(f, "UpdateSubscriberData"),
            InsertCallForwarding { .. } => write!(f, "InsertCallForwarding"),
            DeleteCallForwarding { .. } => write!(f, "DeleteCallForwarding"),
        }
    }
}
