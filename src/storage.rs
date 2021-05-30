use crate::common::ds::atomic_linked_list::AtomicLinkedList;
use crate::scheduler::TransactionInfo;
use crate::storage::row::Tuple;

use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub mod datatype;

pub mod catalog;

pub mod row;

#[derive(Debug)]
pub struct SmallBankDatabase {
    pub accounts: Accounts,
    pub saving: Saving,
    pub checking: Checking,
}

#[derive(Debug)]
pub struct Accounts {
    pub customer_id: Arc<Vec<Tuple>>,
    pub lsns: Vec<AtomicU64>,
    pub rw_tables: Arc<Vec<AtomicLinkedList<Access>>>,
}

#[derive(Debug)]
pub struct Saving {
    pub customer_id: Arc<Vec<Tuple>>,
    pub balance: Arc<Vec<Tuple>>,
    pub lsns: Arc<Vec<AtomicU64>>,
    pub rw_tables: Arc<Vec<AtomicLinkedList<Access>>>,
}

#[derive(Debug)]
pub struct Checking {
    pub customer_id: Arc<Vec<Tuple>>,
    pub balance: Arc<Vec<Tuple>>,
    pub lsns: Arc<Vec<AtomicU64>>,
    pub rw_tables: Arc<Vec<AtomicLinkedList<Access>>>,
}

impl SmallBankDatabase {
    pub fn new(population: usize) -> Self {
        let accounts = Accounts::new(population);
        let saving = Saving::new(population);
        let checking = Checking::new(population);

        SmallBankDatabase {
            accounts,
            saving,
            checking,
        }
    }
}

impl Accounts {
    pub fn new(population: usize) -> Self {
        let mut customer_id = Vec::with_capacity(population);
        let mut lsns = Vec::with_capacity(population);
        let mut rw_tables = Vec::with_capacity(population);

        for _ in 0..population {
            customer_id.push(Tuple::new());
            lsns.push(AtomicU64::new(0));
            rw_tables.push(AtomicLinkedList::new())
        }

        Accounts {
            customer_id: Arc::new(customer_id),
            lsns: lsns,
            rw_tables: Arc::new(rw_tables),
        }
    }
}

impl Saving {
    pub fn new(population: usize) -> Self {
        let mut customer_id = Vec::with_capacity(population);
        let mut balance = Vec::with_capacity(population);
        let mut lsns = Vec::with_capacity(population);
        let mut rw_tables = Vec::with_capacity(population);

        for _ in 0..population {
            customer_id.push(Tuple::new());
            balance.push(Tuple::new());
            lsns.push(AtomicU64::new(0));
            rw_tables.push(AtomicLinkedList::new())
        }

        Saving {
            customer_id: Arc::new(customer_id),
            balance: Arc::new(balance),
            lsns: Arc::new(lsns),
            rw_tables: Arc::new(rw_tables),
        }
    }
}

impl Checking {
    pub fn new(population: usize) -> Self {
        let mut customer_id = Vec::with_capacity(population);
        let mut balance = Vec::with_capacity(population);
        let mut lsns = Vec::with_capacity(population);
        let mut rw_tables = Vec::with_capacity(population);

        for _ in 0..population {
            customer_id.push(Tuple::new());
            balance.push(Tuple::new());
            lsns.push(AtomicU64::new(0));
            rw_tables.push(AtomicLinkedList::new())
        }

        Checking {
            customer_id: Arc::new(customer_id),
            balance: Arc::new(balance),
            lsns: Arc::new(lsns),
            rw_tables: Arc::new(rw_tables),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionInfo),
    Write(TransactionInfo),
}

pub fn access_eq(a: &Access, b: &Access) -> bool {
    matches!(
        (a, b),
        (&Access::Read(..), &Access::Read(..)) | (&Access::Write(..), &Access::Write(..))
    )
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Access::*;
        match &self {
            Read(id) => write!(f, "r-{}", id),
            Write(id) => write!(f, "w-{}", id),
        }
    }
}
