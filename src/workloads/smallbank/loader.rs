use crate::storage::datatype::Data;
use crate::storage::row::Row;
use crate::storage::{Database, Table};

use crate::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE, SB_SF_MAP};
use crate::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::Rng;

use tracing::info;

pub fn populate_tables(config: &Config, database: &mut Database, rng: &mut StdRng) -> Result<()> {
    populate_account(config, database)?;
    populate_savings(config, database, rng)?;
    populate_checking(config, database, rng)?;
    Ok(())
}

pub fn populate_account(config: &Config, database: &mut Database) -> Result<()> {
    let sf = config.get_int("scale_factor")? as u64;
    let n_accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;

    for a_id in 0..n_accounts {
        let table: &mut Table = database.get_mut_table(0);
        let mut row = Row::new(&table.get_schema());
        row.init_value(&table.get_schema(), "customer_id", Data::from(a_id as u64))?;
        table.get_mut_rows().push(row);
    }

    info!("Loaded {} rows into account", n_accounts);

    Ok(())
}

/// Populate the `Savings` table.
pub fn populate_savings(config: &Config, database: &mut Database, rng: &mut StdRng) -> Result<()> {
    let sf = config.get_int("scale_factor")? as u64;
    let accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    for customer_id in 0..accounts {
        let table: &mut Table = database.get_mut_table(1);
        let mut row = Row::new(&table.get_schema());
        row.init_value(
            &table.get_schema(),
            "customer_id",
            Data::from(customer_id as u64),
        )?;
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value(&table.get_schema(), "balance", Data::from(balance))?;
        table.get_mut_rows().push(row);
    }
    info!("Loaded {} rows into savings", accounts);
    Ok(())
}

/// Populate the `Checking` table.
pub fn populate_checking(config: &Config, database: &mut Database, rng: &mut StdRng) -> Result<()> {
    let sf = config.get_int("scale_factor")? as u64;
    let accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    for customer_id in 0..accounts {
        let table: &mut Table = database.get_mut_table(2);
        let mut row = Row::new(&table.get_schema());
        row.init_value(
            &table.get_schema(),
            "customer_id",
            Data::from(customer_id as u64),
        )?;
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value(&table.get_schema(), "balance", Data::from(balance))?;
        table.get_mut_rows().push(row);
    }
    info!("Loaded {} rows into savings", accounts);
    Ok(())
}
