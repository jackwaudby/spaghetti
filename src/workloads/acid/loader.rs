use crate::datagen::acid::Person;
use crate::server::storage::datatype::Data;
use crate::server::storage::row::Row;
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::acid::ACID_SF_MAP;
use crate::workloads::{Internal, PrimaryKey};
use crate::Result;

use rand::rngs::StdRng;
use std::sync::Arc;
use tracing::info;

/// Load into the `Person` table from csv files.
pub fn load_person_table(data: &Internal) -> Result<()> {
    info!("Loading person table");

    let person = data.get_table("person")?;
    let person_idx = data.get_index(&person.get_primary_index()?)?;

    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;

    let path = format!("./data/acid/sf-{}/persons.csv", sf);

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let p: Person = result?;
        let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(p.p_id));
        let mut row = Row::new(pk.clone(), Arc::clone(&person), true, true);

        row.init_value("p_id", Data::Uint(p.p_id))?;
        row.init_value("version", Data::Uint(p.version))?;
        row.init_value("num_friends", Data::Uint(p.num_friends))?;
        person_idx.insert(&pk, row)?;
    }
    info!("Loaded {} row(s) into person", person.get_num_rows());
    Ok(())
}

/// Populate the person table.
pub fn populate_person_table(data: &Internal) -> Result<()> {
    let person = data.get_table("person")?;
    let person_idx = data.get_index(&person.get_primary_index()?)?;

    let config = data.get_config();
    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")? as u64;
    let anomaly = config.get_str("anomaly")?;

    let mut persons = *ACID_SF_MAP.get(&sf).unwrap();

    if anomaly.as_str() == "otv" || anomaly.as_str() == "fr" || anomaly.as_str() == "g2item" {
        persons *= 4;
    }

    info!("Populating person table: {}", persons);
    let mut ws_flag = true;
    for p_id in 0..persons {
        let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(p_id));
        let mut row = Row::new(pk.clone(), Arc::clone(&person), true, true);

        row.init_value("p_id", Data::Uint(p_id))?;
        row.init_value("version", Data::Uint(1))?;
        row.init_value("num_friends", Data::Uint(0))?;
        row.init_value("version_history", Data::List(vec![]))?;

        if ws_flag {
            row.init_value("value", Data::Int(70))?;
            ws_flag = false;
        } else {
            row.init_value("value", Data::Int(80))?;
            ws_flag = true;
        }

        person_idx.insert(&pk, row)?;
    }
    info!("Loaded {} row(s) into person", person.get_num_rows());

    Ok(())
}

/// Populate the `Knows` table.
///
/// Inserts distinct person pairs, e.g., p1 -> p2, p3 -> p4.
/// Each person node has only a single edge in or out.
pub fn populate_person_knows_person_table(data: &Internal, _rng: &mut StdRng) -> Result<()> {
    let knows = data.get_table("knows")?;
    let knows_idx = data.get_index(&knows.get_primary_index()?)?;

    let config = data.get_config();
    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")? as u64;

    let persons = *ACID_SF_MAP.get(&sf).unwrap();

    info!("Populating knows table: {}", persons);
    for p1_id in (0..persons).step_by(2) {
        let p2_id = p1_id + 1;
        let pk = PrimaryKey::Acid(AcidPrimaryKey::Knows(p1_id, p2_id));
        let mut row = Row::new(pk.clone(), Arc::clone(&knows), true, true);

        row.init_value("p1_id", Data::Uint(p1_id))?;
        row.init_value("p2_id", Data::Uint(p2_id))?;
        row.init_value("version_history", Data::List(vec![]))?;
        knows_idx.insert(&pk, row)?;
    }

    info!("Loaded {} row(s) into knows", knows.get_num_rows());

    Ok(())
}
