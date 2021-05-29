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
        database.accounts.customer_id[cid]
            .get()
            .init_value(Data::from(cid as u64))?;

        let cbal = rng.gen_range(MIN_BALANCE..=MAX_BALANCE) as f64;
        database.checking.customer_id[cid]
            .get()
            .init_value(Data::from(cid as u64))?;
        database.checking.balance[cid]
            .get()
            .init_value(Data::Double(cbal))?;

        let sbal = rng.gen_range(MIN_BALANCE..=MAX_BALANCE) as f64;
        database.saving.customer_id[cid]
            .get()
            .init_value(Data::from(cid as u64))?;
        database.saving.balance[cid]
            .get()
            .init_value(Data::Double(sbal))?;
    }
    info!("Loaded {} rows into account", population);
    info!("Loaded {} rows into savings", population);
    info!("Loaded {} rows into checking", population);

    Ok(())
}
