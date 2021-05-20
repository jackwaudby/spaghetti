use crate::storage;
use crate::storage::datatype::Data;
use crate::storage::row::Row;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE, SB_SF_MAP};
use crate::workloads::Database;
use crate::workloads::PrimaryKey;
use crate::workloads::Table;
use crate::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

/// Populate tables.
pub fn populate_tables(
    config: &Config,
    tables: &HashMap<String, Arc<Table>>,
    database: &mut Database,
    rng: &mut StdRng,
) -> Result<()> {
    populate_account(config, tables, database)?;
    populate_savings(config, tables, database, rng)?;
    populate_checking(config, tables, database, rng)?;
    Ok(())
}

/// Populate the `Account` table.
pub fn populate_account(
    config: &Config,
    tables: &HashMap<String, Arc<Table>>,
    database: &mut Database,
) -> Result<()> {
    let accounts = tables.get("accounts").unwrap();
    let sf = config.get_int("scale_factor")? as u64;
    let n_accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;

    for a_id in 0..n_accounts {
        database.set_row(a_id, Arc::clone(&accounts));
        database
            .get_row(a_id)
            .get_lock()
            .init_value("customer_id", Data::from(a_id as u64))
            .unwrap();
    }

    info!("Loaded {} rows into account", n_accounts);

    Ok(())
}

/// Populate the `Savings` table.
pub fn populate_savings(
    config: &Config,
    tables: &HashMap<String, Arc<Table>>,
    database: &mut Database,
    rng: &mut StdRng,
) -> Result<()> {
    let savings = tables.get("savings").unwrap();
    let sf = config.get_int("scale_factor")? as u64;
    let accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    for customer_id in 0..accounts {
        let key = storage::calculate_offset(customer_id, 1, accounts);
        database.set_row(key, Arc::clone(&savings));

        let mut row = database.get_row(key).get_lock();
        row.init_value("customer_id", Data::from(customer_id as u64))
            .unwrap();
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", Data::from(balance)).unwrap();
    }
    info!("Loaded {} rows into savings", accounts);
    Ok(())
}

/// Populate the `Checking` table.
pub fn populate_checking(
    config: &Config,
    tables: &HashMap<String, Arc<Table>>,
    database: &mut Database,
    rng: &mut StdRng,
) -> Result<()> {
    let checking = tables.get("checking").unwrap();
    let sf = config.get_int("scale_factor")? as u64;
    let accounts = *SB_SF_MAP.get(&sf).unwrap() as usize;
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    for customer_id in 0..accounts {
        let key = storage::calculate_offset(customer_id, 2, accounts);
        database.set_row(key, Arc::clone(&checking));

        let mut row = database.get_row(key).get_lock();
        row.init_value("customer_id", Data::from(customer_id as u64))
            .unwrap();
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", Data::from(balance)).unwrap();
    }
    info!("Loaded {} rows into savings", accounts);
    Ok(())
}
