use crate::storage::table::Table;

use lazy_static::lazy_static;
use nohash_hasher::IntMap;
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
        m.insert(0, 10);
        m.insert(1, 100000);
        m.insert(2, 200000);
        m.insert(5, 500000);
        m.insert(10, 1000000);
        m.insert(20, 2000000);
        m.insert(50, 5000000);
        m
    };
}

#[derive(Debug)]
pub struct TatpDatabase(IntMap<usize, Table>);

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TatpTransaction {
    GetSubscriberData,
    GetNewDestination,
    GetAccessData,
    UpdateSubscriberData,
    UpdateLocationData,
}

impl TatpDatabase {
    pub fn new(population: usize) -> Self {
        let mut map = IntMap::default();

        // Note; subscriber has 34 columns which are largely redunant
        // Included: s_id; sub_nbr; bit_1; msc_location; vlr_location
        map.insert(0, Table::new(population, 5)); // subscribers
        map.insert(1, Table::new(population * 3, 6)); // access info
        map.insert(2, Table::new(population * 3, 6)); // special facility
        map.insert(3, Table::new(population * 4, 5)); // call forwarding

        TatpDatabase(map)
    }

    pub fn get_table(&self, id: usize) -> &Table {
        self.0.get(&id).unwrap()
    }

    pub fn get_mut_table(&mut self, id: usize) -> &mut Table {
        self.0.get_mut(&id).unwrap()
    }
}
