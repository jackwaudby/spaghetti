use crate::datagen::smallbank::{Account, Checking, Savings};
use crate::server::storage::row::Row;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE, SB_SF_MAP};
use crate::workloads::{Internal, PrimaryKey};
use crate::Result;

use rand::rngs::StdRng;
use rand::Rng;
use std::sync::Arc;
use tracing::info;

pub fn load_account_table(data: &Internal) -> Result<()> {
    info!("Loading account table");
    let table = data.get_table("accounts")?;
    let index_name = table.get_primary_index()?;
    let index = data.get_index(&index_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;
    let path = format!("./data/smallbank/sf-{}/accounts.csv", sf);

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let s: Account = result?; // deserialise
        let mut row = Row::new(Arc::clone(&table), track_access, track_delayed); // initialise empty row
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(s.name.clone())); // calculate primary key
        row.set_primary_key(pk.clone());
        row.init_value("name", &s.name)?;
        row.init_value("customer_id", &s.customer_id.to_string())?;

        index.insert(&pk, row)?;
    }
    info!("Loaded {} rows into account", table.get_num_rows());
    Ok(())
}

pub fn load_savings_table(data: &Internal) -> Result<()> {
    info!("Loading savings table");
    let table = data.get_table("savings")?;
    let index_name = table.get_primary_index()?;
    let index = data.get_index(&index_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;
    let path = format!("./data/smallbank/sf-{}/savings.csv", sf);

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let s: Savings = result?; // deserialise
        let mut row = Row::new(Arc::clone(&table), track_access, track_delayed); // initialise empty row
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(s.customer_id)); // calculate primary key
        row.set_primary_key(pk.clone());

        row.init_value("customer_id", &s.customer_id.to_string())?;
        row.init_value("balance", &s.balance.to_string())?;

        index.insert(&pk, row)?;
    }
    info!("Loaded {} rows into savings", table.get_num_rows());
    Ok(())
}

pub fn load_checking_table(data: &Internal) -> Result<()> {
    info!("Loading checking table");
    let table = data.get_table("checking")?;
    let index_name = table.get_primary_index()?;
    let index = data.get_index(&index_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;
    let path = format!("./data/smallbank/sf-{}/checking.csv", sf);

    let track_access = match protocol.as_str() {
        "sgt" | "basic-sgt" | "hit" | "opt-hit" => true,
        _ => false,
    };

    let track_delayed = match protocol.as_str() {
        "basic-sgt" => true,
        _ => false,
    };

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        let s: Checking = result?; // deserialise
        let mut row = Row::new(Arc::clone(&table), track_access, track_delayed); // initialise empty row
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(s.customer_id)); // calculate primary key
        row.set_primary_key(pk.clone());

        row.init_value("customer_id", &s.customer_id.to_string())?;
        row.init_value("balance", &s.balance.to_string())?;

        index.insert(&pk, row)?;
    }
    info!("Loaded {} rows into checking", table.get_num_rows());
    Ok(())
}

/// Populate tables.
pub fn populate_tables(data: &Internal, rng: &mut StdRng) -> Result<()> {
    populate_account(data)?;
    populate_savings(data, rng)?;
    populate_checking(data, rng)?;
    Ok(())
}

/// Populate the `Account` table.
pub fn populate_account(data: &Internal) -> Result<()> {
    let accounts = data.get_table("accounts")?; // get handle to table
    let accounts_idx = data.get_index(&accounts.get_primary_index()?)?; // get handle to index

    let protocol = data.config.get_str("protocol")?; // get protocol
    let sf = data.config.get_int("scale_factor")? as u64; // get sf
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
        let name = format!("cust{}", a_id);
        let mut row = Row::new(Arc::clone(&accounts), track_access, track_delayed);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(name.clone()));
        row.set_primary_key(pk.clone());
        row.init_value("name", &name)?;
        row.init_value("customer_id", &a_id.to_string())?;
        accounts_idx.insert(&pk, row)?;
    }
    info!("Loaded {} rows into account", accounts.get_num_rows());

    Ok(())
}

/// Populate the `Savings` table.
pub fn populate_savings(data: &Internal, rng: &mut StdRng) -> Result<()> {
    let savings = data.get_table("savings")?;
    let savings_idx = data.get_index(&savings.get_primary_index()?)?;

    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")? as u64;

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
        let mut row = Row::new(Arc::clone(&savings), track_access, track_delayed);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(customer_id));
        row.set_primary_key(pk.clone());
        row.init_value("customer_id", &customer_id.to_string())?;
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", &balance.to_string())?;
        savings_idx.insert(&pk, row)?;
    }
    info!("Loaded {} rows into savings", savings.get_num_rows());
    Ok(())
}

/// Populate the `Checking` table.
pub fn populate_checking(data: &Internal, rng: &mut StdRng) -> Result<()> {
    let checking = data.get_table("checking")?;
    let checking_idx = data.get_index(&checking.get_primary_index()?)?;

    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")? as u64;

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
        let mut row = Row::new(Arc::clone(&checking), track_access, track_delayed);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(customer_id));
        row.set_primary_key(pk.clone());
        row.init_value("customer_id", &customer_id.to_string())?;
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", &balance.to_string())?;
        checking_idx.insert(&pk, row)?;
    }
    info!("Loaded {} rows into savings", checking.get_num_rows());
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::server::storage::datatype;
    use config::Config;
    use rand::SeedableRng;

    #[test]
    fn populate_account_test() {
        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-smallbank.toml"))
            .unwrap();
        let config = Arc::new(c);

        // Initialise database.
        let c = Arc::clone(&config);
        let internals = Internal::new("./schema/smallbank_schema.txt", c).unwrap();

        // let rng = StdRng::seed_from_u64(1);

        // Populate Accounts.
        populate_account(&internals).unwrap();
        assert_eq!(
            internals.get_table("accounts").unwrap().get_next_row_id(),
            10
        );

        // Get record.
        let index = internals.indexes.get("account_name").unwrap();
        let cols = vec!["name", "customer_id"];
        let res = index
            .read(
                PrimaryKey::SmallBank(SmallBankPrimaryKey::Account("cust1".to_string())),
                &cols,
                "2pl",
                "t1",
            )
            .unwrap()
            .get_values()
            .unwrap();
        assert_eq!(
            datatype::to_result(None, None, None, Some(&cols), Some(&res)).unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"customer_id\":\"1\",\"name\":\"cust1\"}}"
        );
    }

    #[test]
    fn populate_checking_test() {
        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-smallbank.toml"))
            .unwrap();
        let config = Arc::new(c);

        // Initialise database.
        let c = Arc::clone(&config);
        let internals = Internal::new("./schema/smallbank_schema.txt", c).unwrap();

        let mut rng = StdRng::seed_from_u64(2);

        // Populate Accounts.
        populate_checking(&internals, &mut rng).unwrap();
        assert_eq!(
            internals.get_table("checking").unwrap().get_next_row_id(),
            10
        );

        // Get record.
        let index = internals.indexes.get("checking_idx").unwrap();
        let cols = vec!["customer_id", "balance"];
        let res = index
            .read(
                PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(1)),
                &cols,
                "2pl",
                "t1",
            )
            .unwrap()
            .get_values()
            .unwrap();
        assert_eq!(
            datatype::to_result(None, None, None, Some(&cols), Some(&res)).unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"balance\":\"28425308\",\"customer_id\":\"1\"}}"
        );
    }

    #[test]
    fn populate_savings_test() {
        // Initialise configuration.
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-smallbank.toml"))
            .unwrap();
        let config = Arc::new(c);

        // Initialise database.
        let c = Arc::clone(&config);
        let internals = Internal::new("./schema/smallbank_schema.txt", c).unwrap();

        let mut rng = StdRng::seed_from_u64(3);

        // Populate savings.
        populate_savings(&internals, &mut rng).unwrap();
        assert_eq!(
            internals.get_table("savings").unwrap().get_next_row_id(),
            10
        );

        // Get record.
        let index = internals.indexes.get("savings_idx").unwrap();
        let cols = vec!["customer_id", "balance"];
        let res = index
            .read(
                PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(1)),
                &cols,
                "2pl",
                "t1",
            )
            .unwrap()
            .get_values()
            .unwrap();
        assert_eq!(
            datatype::to_result(None, None, None, Some(&cols), Some(&res)).unwrap(),
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"balance\":\"27059721\",\"customer_id\":\"1\"}}"
        );
    }
}
