use crate::storage::datatype::Data;
use crate::storage::row::Row;
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::acid::ACID_SF_MAP;
use crate::workloads::Index;
use crate::workloads::PrimaryKey;
use crate::workloads::Table;
use crate::Result;

use config::Config;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Populate the person table.
pub fn populate_person_table(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
) -> Result<()> {
    let person = tables.get_mut("person").unwrap();
    let person_idx = indexes.get_mut("person_idx").unwrap();

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

        person_idx.insert(&pk, row);
    }
    info!("Loaded {} row(s) into person", person.get_num_rows());

    Ok(())
}

/// Populate the `Knows` table.
///
/// Inserts distinct person pairs, e.g., p1 -> p2, p3 -> p4.
/// Each person node has only a single edge in or out.
pub fn populate_person_knows_person_table(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
) -> Result<()> {
    let knows = tables.get_mut("knows").unwrap();
    let knows_idx = indexes.get_mut("knows_idx").unwrap();

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
        knows_idx.insert(&pk, row);
    }

    info!("Loaded {} row(s) into knows", knows.get_num_rows());

    Ok(())
}
