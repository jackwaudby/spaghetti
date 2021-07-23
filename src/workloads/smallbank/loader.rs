use crate::storage::datatype::Data;
use crate::workloads::smallbank::SmallBankDatabase;
use crate::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE};
use crate::Result;

use rand::rngs::StdRng;
use rand::Rng;

use tracing::info;

pub fn populate_tables(
    population: usize,
    database: &mut SmallBankDatabase,
    rng: &mut StdRng,
) -> Result<()> {
    for cid in 0..population {
        database.insert_value(0, 0, cid, Data::Uint(cid as u64)); // accounts

        let cbal = rng.gen_range(MIN_BALANCE..=MAX_BALANCE) as f64; // checking
        database.insert_value(1, 0, cid, Data::Uint(cid as u64));
        database.insert_value(1, 1, cid, Data::Double(cbal));

        let sbal = rng.gen_range(MIN_BALANCE..=MAX_BALANCE) as f64; // saving
        database.insert_value(2, 0, cid, Data::Uint(cid as u64));
        database.insert_value(2, 1, cid, Data::Double(sbal));
    }
    info!("Loaded {} rows into account", population);
    info!("Loaded {} rows into savings", population);
    info!("Loaded {} rows into checking", population);

    Ok(())
}
