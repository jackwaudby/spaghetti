use crate::Result;

use csv::WriterBuilder;
use serde::{Deserialize, Serialize};

/// Generate `Person` records.
pub fn persons(persons: u64, sf: u64) -> Result<()> {
    // TODO: see person knows person
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(format!("./data/acid/sf-{}/persons.csv", sf))?;

    for p_id in 0..persons {
        wtr.serialize(Person::new(p_id, 1, 0))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Generate `PersonKnowsPerson` records.
pub fn person_knows_person(persons: u64, sf: u64) -> Result<()> {
    // TODO: desired format;
    // p1.id, p2.id,version.history
    // 0,1,[]
    //
    // Currently does not serialize with headers as vec![1,3,5] is flatten into 1,3,5
    // rather than [1,3,5]
    // Loader will not work with this format
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(format!("./data/acid/sf-{}/person_knows_person.csv", sf))?;

    for p1_id in (0..persons).step_by(2) {
        let p2_id = p1_id + 1;
        let pkp = PersonKnowsPerson::new(p1_id, p2_id);

        wtr.serialize(pkp)?;
    }

    wtr.flush()?;
    Ok(())
}

/// Represents a record in the Person table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Person {
    pub p_id: u64,
    pub version: u64,
    pub num_friends: u64,
    pub version_history: Vec<u64>,
}

/// Represents a record in the Knows table.
#[derive(Debug, Deserialize, Serialize)]
pub struct PersonKnowsPerson {
    pub p1_id: u64,
    pub p2_id: u64,
    pub version_history: Vec<u64>,
}

impl Person {
    /// Create new `Person` record.
    pub fn new(p_id: u64, version: u64, num_friends: u64) -> Self {
        Person {
            p_id,
            version,
            num_friends,
            version_history: vec![],
        }
    }
}

impl PersonKnowsPerson {
    /// Create new `PersonKnowsPerson` record.
    pub fn new(p1_id: u64, p2_id: u64) -> Self {
        PersonKnowsPerson {
            p1_id,
            p2_id,
            version_history: vec![],
        }
    }
}
