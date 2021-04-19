use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::EnumIter;

pub mod loader;

pub mod paramgen;

pub mod procedures;

pub mod keys;

lazy_static! {
    pub static ref ACID_SF_MAP: HashMap<u64, u64> = {
        let mut m = HashMap::new();
        m.insert(0, 2);
        m.insert(1, 10);
        m
    };
}

#[derive(EnumIter, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum AcidTransaction {
    G0Write,
    G0Read,
    G1aWrite,
    G1aRead,
    G1cReadWrite,
    ImpRead,
    ImpWrite,
    LostUpdateRead,
    LostUpdateWrite,
}
