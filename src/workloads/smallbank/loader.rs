use crate::datagen::smallbank::{Account, Checking, Savings};
use crate::server::storage::row::Row;
use crate::workloads::smallbank::keys::SmallBankPrimaryKey;
use crate::workloads::smallbank::{MAX_BALANCE, MIN_BALANCE, SB_SF_MAP};
use crate::workloads::{Internal, PrimaryKey};
use crate::Result;

use rand::rngs::StdRng;
//use rand::seq::IteratorRandom;
use rand::Rng;

use std::sync::Arc;
use tracing::info;

//////////////////////////////
/// Table Loaders. ///
//////////////////////////////

pub fn load_account_table(data: &Internal) -> Result<()> {
    info!("Loading account table");
    let table_name = "accounts";
    let t = data.get_table(table_name)?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;
    let path = format!("./data/smallbank/sf-{}/accounts.csv", sf);

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        // Deserialise.
        let s: Account = result?;
        // Initialise empty row.
        let mut row = Row::new(Arc::clone(&t), &protocol);
        // Calculate primary key
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(s.name.clone()));
        row.set_primary_key(pk.clone());
        row.init_value("name", &s.name)?;
        row.init_value("customer_id", &s.customer_id.to_string())?;

        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into account", t.get_num_rows());
    Ok(())
}

pub fn load_savings_table(data: &Internal) -> Result<()> {
    info!("Loading savings table");
    let table_name = "savings";
    let t = data.get_table(table_name)?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;
    let path = format!("./data/smallbank/sf-{}/savings.csv", sf);

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        // Deserialise.
        let s: Savings = result?;
        // Initialise empty row.
        let mut row = Row::new(Arc::clone(&t), &protocol);
        // Calculate primary key
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(s.customer_id));
        row.set_primary_key(pk.clone());

        row.init_value("customer_id", &s.customer_id.to_string())?;
        row.init_value("balance", &s.balance.to_string())?;

        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into savings", t.get_num_rows());
    Ok(())
}

pub fn load_checking_table(data: &Internal) -> Result<()> {
    info!("Loading checking table");
    let table_name = "checking";
    let t = data.get_table(table_name)?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")?;
    let path = format!("./data/smallbank/sf-{}/checking.csv", sf);

    let mut rdr = csv::Reader::from_path(&path)?;
    for result in rdr.deserialize() {
        // Deserialise.
        let s: Checking = result?;
        // Initialise empty row.
        let mut row = Row::new(Arc::clone(&t), &protocol);
        // Calculate primary key
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(s.customer_id));
        row.set_primary_key(pk.clone());

        row.init_value("customer_id", &s.customer_id.to_string())?;
        row.init_value("balance", &s.balance.to_string())?;

        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into checking", t.get_num_rows());
    Ok(())
}
///////////////////////////////////////////////
/// Table Generate and Load. ///
///////////////////////////////////////////////

/// Populate tables.
pub fn populate_tables(data: &Internal, rng: &mut StdRng) -> Result<()> {
    populate_account(data)?;
    populate_savings(data, rng)?;
    populate_checking(data, rng)?;
    Ok(())
}

/// Populate the `Account` table.
pub fn populate_account(data: &Internal) -> Result<()> {
    let table_name = "accounts";
    let t = data.get_table(table_name)?; // get handle to table
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?; // get handle to index

    let protocol = data.config.get_str("protocol")?; // get protocol
    let sf = data.config.get_int("scale_factor")? as u64; // get sf
    let accounts = *SB_SF_MAP.get(&sf).unwrap(); // get accounts

    info!("Populating accounts table: {}", accounts);

    for a_id in 0..accounts {
        let name = format!("cust{}", a_id);
        let mut row = Row::new(Arc::clone(&t), &protocol);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Account(name.clone()));
        row.set_primary_key(pk.clone());
        row.init_value("name", &name)?;
        row.init_value("customer_id", &a_id.to_string())?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into account", t.get_num_rows());

    Ok(())
}

/// Populate the `Savings` table.
pub fn populate_savings(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Populating savings table");
    let t = data.get_table("savings")?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")? as u64;
    let accounts = *SB_SF_MAP.get(&sf).unwrap();
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    for customer_id in 0..accounts {
        let mut row = Row::new(Arc::clone(&t), &protocol);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Savings(customer_id));
        row.set_primary_key(pk.clone());
        row.init_value("customer_id", &customer_id.to_string())?;
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", &balance.to_string())?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into savings", t.get_num_rows());
    Ok(())
}

/// Populate the `Checking` table.
pub fn populate_checking(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Populating checking table");
    // Get handle to `Table` and `Index`.
    let table_name = "checking";
    let t = data.get_table(table_name)?;
    let index_name = t.get_primary_index()?;
    let i = data.get_index(&index_name)?;

    // Get protocol.
    let protocol = data.config.get_str("protocol")?;
    let sf = data.config.get_int("scale_factor")? as u64;

    let accounts = *SB_SF_MAP.get(&sf).unwrap();
    let min_bal = MIN_BALANCE;
    let max_bal = MAX_BALANCE;

    for customer_id in 0..accounts {
        let mut row = Row::new(Arc::clone(&t), &protocol);
        let pk = PrimaryKey::SmallBank(SmallBankPrimaryKey::Checking(customer_id));
        row.set_primary_key(pk.clone());
        row.init_value("customer_id", &customer_id.to_string())?;
        let balance = rng.gen_range(min_bal..=max_bal) as f64;
        row.init_value("balance", &balance.to_string())?;
        i.insert(pk, row)?;
    }
    info!("Loaded {} rows into savings", t.get_num_rows());
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
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"balance\":\"21893033\",\"customer_id\":\"1\"}}"
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
            "{\"created\":null,\"updated\":null,\"deleted\":null,\"val\":{\"balance\":\"17353809\",\"customer_id\":\"1\"}}"
        );
    }
}
