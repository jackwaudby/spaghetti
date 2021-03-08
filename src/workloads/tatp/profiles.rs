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
        match &*self {
            TatpTransaction::GetSubscriberData(params) => {
                let GetSubscriberData { s_id } = params;
                write!(f, "0,{}", s_id)
            }
            TatpTransaction::GetNewDestination(params) => {
                let GetNewDestination {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                } = params;
                write!(f, "1,{},{},{},{}", s_id, sf_type, start_time, end_time)
            }
            TatpTransaction::GetAccessData(params) => {
                let GetAccessData { s_id, ai_type } = params;
                write!(f, "2,{},{}", s_id, ai_type)
            }
            TatpTransaction::UpdateLocationData(params) => {
                let UpdateLocationData { s_id, vlr_location } = params;
                write!(f, "3,{},{}", s_id, vlr_location)
            }
            TatpTransaction::UpdateSubscriberData(params) => {
                let UpdateSubscriberData {
                    s_id,
                    sf_type,
                    bit_1,
                    data_a,
                } = params;
                write!(f, "4,{},{},{},{}", s_id, sf_type, bit_1, data_a)
            }
            TatpTransaction::InsertCallForwarding(params) => {
                let InsertCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                    end_time,
                    number_x,
                } = params;
                write!(
                    f,
                    "5,{},{},{},{},{}",
                    s_id, sf_type, start_time, end_time, number_x
                )
            }
            TatpTransaction::DeleteCallForwarding(params) => {
                let DeleteCallForwarding {
                    s_id,
                    sf_type,
                    start_time,
                } = params;
                write!(f, "6,{},{},{}", s_id, sf_type, start_time)
            }
        }
    }
}
