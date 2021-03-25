use crate::common::message::{Message, Parameters};
use crate::common::parameter_generation::Generator;
use crate::workloads::smallbank::generator::SmallBankGenerator;
use crate::workloads::smallbank::records::*;

use crate::Result;

use config::Config;
use csv::Writer;
use rand::prelude::IteratorRandom;
use rand::rngs::StdRng;
use rand::Rng;
use std::sync::Arc;

/// Generate parameters.
pub fn params(config: Arc<Config>) -> Result<()> {
    let transactions = config.get_int("transactions")? as u64;
    let mut wtr = Writer::from_path("data/smallbank/params.csv")?;
    let mut gen = SmallBankGenerator::new(config);

    for _ in 1..=transactions {
        let message = gen.generate();
        if let Message::Request { parameters, .. } = message {
            if let Parameters::Tatp(params) = parameters {
                let s = format!("{}", params);
                wtr.write_record(&[s])?;
            }
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate `Account` records.
pub fn accounts(accounts: u64) -> Result<()> {
    let mut wtr = Writer::from_path("data/smallbank/accounts.csv")?;

    for a in 1..=accounts {
        wtr.serialize(Account::new(format!("cust{}", a), a))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Generate `Savings` records.
pub fn savings(accounts: u64, min: i64, max: i64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("data/smallbank/savings.csv")?;

    for a in 1..=accounts {
        let bal = rng.gen_range(min..max) as f64;
        wtr.serialize(Savings::new(a, bal))?;
    }

    wtr.flush()?;
    Ok(())
}

/// Generate `Checking` records.
pub fn checking(accounts: u64, min: i64, max: i64, rng: &mut StdRng) -> Result<()> {
    let mut wtr = Writer::from_path("data/smallbank/checking.csv")?;

    for a in 1..=accounts {
        let bal = rng.gen_range(min..max) as f64;
        wtr.serialize(Savings::new(a, bal))?;
    }

    wtr.flush()?;
    Ok(())
}
