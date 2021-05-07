use crate::storage::datatype::Data;
use crate::storage::index::Index;
use crate::storage::row::Row;
use crate::storage::table::Table;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE, SB_SF_MAP};
use crate::workloads::PrimaryKey;
use crate::Result;

use config::Config;
use rand::rngs::StdRng;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Populate tables.
pub fn populate_tables(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
    rng: &mut StdRng,
) -> Result<()> {
    populate_account(Arc::clone(&config), tables, indexes)?;
    populate_savings(Arc::clone(&config), tables, indexes, rng)?;
    populate_checking(config, tables, indexes, rng)?;
    Ok(())
}

/// Populate the `Account` table.
pub fn populate_account(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
) -> Result<()> {
    let accounts = tables.get("accounts").unwrap(); // get handle to table
    let accounts_idx = indexes.get_mut("account_name").unwrap();

    let protocol = config.get_str("protocol")?; // get protocol
    let sf = config.get_int("scale_factor")? as u64; // get sf
    let n_accounts = *SB_SF_MAP.get(&sf).unwrap(); // get accounts

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    for a_id in 0..n_accounts {
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(a_id));
        let mut row = Row::new(
            pk.clone(),
            Arc::clone(&accounts),
            track_access,
            track_delayed,
        );

        row.init_value("customer_id", Data::from(a_id)).unwrap();
        accounts_idx.insert(&pk, row);
    }
    info!("Loaded {} rows into account", accounts.get_num_rows());

    Ok(())
}

/// Populate the `Savings` table.
pub fn populate_savings(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
    rng: &mut StdRng,
) -> Result<()> {
    let savings = tables.get_mut("savings").unwrap();
    let savings_idx = indexes.get_mut("savings_idx").unwrap();

    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")? as u64;

    let accounts = *SB_SF_MAP.get(&sf).unwrap();
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    for customer_id in 0..accounts {
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(customer_id));
        let mut row = Row::new(
            pk.clone(),
            Arc::clone(&savings),
            track_access,
            track_delayed,
        );
        row.init_value("customer_id", Data::from(customer_id))
            .unwrap();
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", Data::from(balance)).unwrap();
        savings_idx.insert(&pk, row);
    }
    info!("Loaded {} rows into savings", savings.get_num_rows());
    Ok(())
}

/// Populate the `Checking` table.
pub fn populate_checking(
    config: Arc<Config>,
    tables: &mut HashMap<String, Arc<Table>>,
    indexes: &mut HashMap<String, Index>,
    rng: &mut StdRng,
) -> Result<()> {
    let checking = tables.get("checking").unwrap();
    let checking_idx = indexes.get_mut("checking_idx").unwrap();

    let protocol = config.get_str("protocol")?;
    let sf = config.get_int("scale_factor")? as u64;

    let accounts = *SB_SF_MAP.get(&sf).unwrap();
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    for customer_id in 0..accounts {
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(customer_id));
        let mut row = Row::new(
            pk.clone(),
            Arc::clone(&checking),
            track_access,
            track_delayed,
        );
        row.init_value("customer_id", Data::from(customer_id))
            .unwrap();
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", Data::from(balance)).unwrap();
        checking_idx.insert(&pk, row);
    }
    info!("Loaded {} rows into savings", checking.get_num_rows());
    Ok(())
}
