use crate::storage::table::Table;

use lazy_static::lazy_static;
use nohash_hasher::IntMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::EnumIter;

pub mod loader;

pub mod paramgen;

pub mod procedures;

lazy_static! {
    pub static ref ACID_SF_MAP: HashMap<u64, u64> = {
        let mut m = HashMap::new();
        m.insert(1, 8); // 2 sufficient; otv/fr/g2item require x4
        m
    };
}

#[derive(Debug)]
pub struct AcidDatabase(IntMap<usize, Table>);

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum AcidTransaction {
    G0Write,
    G0Read,
    G1aWrite,
    G1aRead,
    G1cReadWrite,
    ImpRead,
    ImpWrite,
    OtvRead,
    OtvWrite,
    LostUpdateRead,
    LostUpdateWrite,
    G2itemWrite,
    G2itemRead,
}

impl AcidDatabase {
    pub fn new(population: usize) -> Self {
        let mut map = IntMap::default();

        map.insert(0, Table::new(population, 5)); // person
        map.insert(1, Table::new(population, 2)); // person_knows_person

        AcidDatabase(map)
    }

    pub fn get_table(&self, id: usize) -> &Table {
        self.0.get(&id).unwrap()
    }

    pub fn get_mut_table(&mut self, id: usize) -> &mut Table {
        self.0.get_mut(&id).unwrap()
    }
}
