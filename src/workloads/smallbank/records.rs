use serde::{Deserialize, Serialize};

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
