use crate::storage::datatype::Data;
use crate::storage::SmallBankDatabase;
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
        // accounts
        database
            .get_mut_table(0)
            .get_tuple(0, cid)
            .get()
            .init_value(Data::Uint(cid as u64))?;

        // checking
        let cbal = rng.gen_range(MIN_BALANCE..=MAX_BALANCE) as f64;
        database
            .get_mut_table(1)
            .get_tuple(0, cid)
            .get()
            .init_value(Data::Uint(cid as u64))?;
        database
            .get_mut_table(1)
            .get_tuple(1, cid)
            .get()
            .init_value(Data::Double(cbal))?;

        // saving
        let sbal = rng.gen_range(MIN_BALANCE..=MAX_BALANCE) as f64;
        database
            .get_mut_table(2)
            .get_tuple(0, cid)
            .get()
            .init_value(Data::Uint(cid as u64))?;
        database
            .get_mut_table(2)
            .get_tuple(1, cid)
            .get()
            .init_value(Data::Double(sbal))?;
    }
    info!("Loaded {} rows into account", population);
    info!("Loaded {} rows into savings", population);
    info!("Loaded {} rows into checking", population);

    Ok(())
}
