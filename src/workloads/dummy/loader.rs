use crate::storage::datatype::Data;
use crate::workloads::dummy::DummyDatabase;
use crate::Result;

use rand::rngs::StdRng;
use rand::Rng;

use tracing::info;

pub fn populate_tables(
    population: usize,
    database: &mut DummyDatabase,
    rng: &mut StdRng,
) -> Result<()> {
    for offset in 0..population {
        let num = rng.gen_range(0..=100) as u64; // random int

        database.insert_value(0, offset, Data::Uint(num)); //
    }
    info!("Loaded {} rows into table 1", population);

    Ok(())
}
