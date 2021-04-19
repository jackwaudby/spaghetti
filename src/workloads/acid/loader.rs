use crate::datagen::acid::Person;
use crate::server::storage::row::Row;
use crate::workloads::acid::keys::AcidPrimaryKey;
use crate::workloads::acid::ACID_SF_MAP;
use crate::workloads::{Internal, PrimaryKey};
use crate::Result;

use rand::rngs::StdRng;
//use rand::seq::IteratorRandom;
// use rand::Rng;

use std::sync::Arc;
use tracing::info;

/// Load into the `Person` table from csv files.
///
/// Schema: (p_id, version)
/// Primary key: p_id
pub fn load_person_table(data: &Internal) -> Result<()> {
    info!("Loading person table");
    let t_name = "person";
    let t = data.get_table(t_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;
    let path = format!("./data/acid/sf-{}/persons.csv", sf); // dir

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let p: Person = result?; // deserialise
        let mut row = Row::new(Arc::clone(&t), &protocol); // empty row
        let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(p.p_id)); // pk

        row.set_primary_key(pk.clone());
        row.init_value("p_id", &p.p_id.to_string())?;
        row.init_value("version", &p.version.to_string())?;
        row.init_value("num_friends", &p.version.to_string())?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} row(s) into person", t.get_num_rows());
    Ok(())
}

/// Populate the `Person` table.
///
/// Schema: (p_id, version)
/// Primary key: p_id
pub fn populate_person_table(data: &Internal, _rng: &mut StdRng) -> Result<()> {
    let t_name = "person";
    let t = data.get_table(t_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")? as u64;
    let persons = *ACID_SF_MAP.get(&sf).unwrap();

    info!("Populating person table: {}", persons);
    for p_id in 0..persons {
        let mut row = Row::new(Arc::clone(&t), &protocol);
        let pk = PrimaryKey::Acid(AcidPrimaryKey::Person(p_id));
        row.set_primary_key(pk.clone());
        row.init_value("p_id", &p_id.to_string())?;
        row.init_value("version", "1")?;
        row.init_value("num_friends", "0")?;
        row.init_value("version_history", "[]")?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} row(s) into person", t.get_num_rows());

    Ok(())
}
