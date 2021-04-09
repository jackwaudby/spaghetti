use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::EnumIter;

pub mod helper;

pub mod loader;

pub mod paramgen;

pub mod procedures;

pub mod keys;

lazy_static! {
    pub static ref TATP_SF_MAP: HashMap<u64, u64> = {
        let mut m = HashMap::new();
        m.insert(1, 100000);
        m.insert(2, 200000);
        m.insert(5, 500000);
        m.insert(10, 1000000);
        m.insert(20, 2000000);
        m.insert(50, 5000000);
        m
    };
}

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TatpTransaction {
    GetSubscriberData,
    GetNewDestination,
    GetAccessData,
    UpdateSubscriberData,
    UpdateLocationData,
    InsertCallForwarding,
    DeleteCallForwarding,
}
