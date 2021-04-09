use crate::common::message::{Message, Parameters};
use crate::common::parameter_generation::Generator;
use crate::workloads::smallbank::paramgen::SmallBankGenerator;
use crate::Result;

use csv::Writer;
use rand::rngs::StdRng;
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Generate parameters for SmallBank stored procedures.
pub fn params(
    sf: u64,
    set_seed: bool,
    seed: Option<u64>,
    use_balance_mix: bool,
    hotspot_use_fixed_size: bool,
    transactions: u64,
) -> Result<()> {
    let mut wtr = Writer::from_path(format!("./data/smallbank/sf-{}/params.csv", sf))?;
    let mut gen =
        SmallBankGenerator::new(sf, set_seed, seed, use_balance_mix, hotspot_use_fixed_size);

    for _ in 1..=transactions {
        let message = gen.generate();
        if let Message::Request { parameters, .. } = message {
            if let Parameters::SmallBank(params) = parameters {
                let s = format!("{}", params);
                wtr.write_record(&[s])?;
            }
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate `Account` records.
pub fn accounts(accounts: u64, sf: u64) -> Result<()> {
    let mut wtr = Writer::from_path(format!("./data/smallbank/sf-{}/accounts.csv", sf))?;

    for a in 1..=accounts {
        wtr.serialize(Account::new(format!("cust{}", a), a))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Generate `Savings` records.
pub fn savings(accounts: u64, min: i64, max: i64, rng: &mut StdRng, sf: u64) -> Result<()> {
    let mut wtr = Writer::from_path(format!("./data/smallbank/sf-{}/savings.csv", sf))?;

    for a in 1..=accounts {
        let bal = rng.gen_range(min..max) as f64;
        wtr.serialize(Savings::new(a, bal))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Generate `Checking` records.
pub fn checking(accounts: u64, min: i64, max: i64, rng: &mut StdRng, sf: u64) -> Result<()> {
    let mut wtr = Writer::from_path(format!("./data/smallbank/sf-{}/checking.csv", sf))?;

    for a in 1..=accounts {
        let bal = rng.gen_range(min..max) as f64;
        wtr.serialize(Savings::new(a, bal))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Represent a record in the Account table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Account {
    pub name: String,
    pub customer_id: u64,
}

/// Represent a record in the Savings table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Savings {
    pub customer_id: u64,
    pub balance: f64,
}

/// Represent a record in the Checking table.
#[derive(Debug, Deserialize, Serialize)]
pub struct Checking {
    pub customer_id: u64,
    pub balance: f64,
}

impl Account {
    /// Create new `Account` record.
    pub fn new(name: String, customer_id: u64) -> Account {
        Account { name, customer_id }
    }
}

impl Savings {
    /// Create new `Savings` record.
    pub fn new(customer_id: u64, balance: f64) -> Savings {
        Savings {
            customer_id,
            balance,
        }
    }
}

impl Checking {
    /// Create new `Savings` record.
    pub fn new(customer_id: u64, balance: f64) -> Checking {
        Checking {
            customer_id,
            balance,
        }
    }
}
