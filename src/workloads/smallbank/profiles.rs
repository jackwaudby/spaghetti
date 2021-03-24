use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents parameters for each transaction.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SmallBankTransactionProfile {
    Balance(Balance),
    DepositChecking(DepositChecking),
    TransactSaving(TransactSaving),
    Amalgamate(Amalgamate),
    WriteCheck(WriteCheck),
    SendPayment(SendPayment),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct Balance {
    pub name: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct DepositChecking {
    pub name: u64,
    pub value: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct TransactSaving {
    pub name: u64,
    pub value: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct Amalgamate {
    pub name_1: u64,
    pub name_2: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct WriteCheck {
    pub name: u64,
    pub value: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct SendPayment {
    pub name_1: u64,
    pub name_2: u64,
    pub amount: u64,
}

impl fmt::Display for SmallBankTransactionProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            SmallBankTransactionProfile::Balance(params) => {
                let Balance { name } = params;
                write!(f, "0,{}", name)
            }
            SmallBankTransactionProfile::DepositChecking(params) => {
                let DepositChecking { name, value } = params;
                write!(f, "1,{},{}", name, value)
            }
            SmallBankTransactionProfile::TransactSaving(params) => {
                let TransactSaving { name, value } = params;
                write!(f, "2,{},{}", name, value)
            }
            SmallBankTransactionProfile::Amalgamate(params) => {
                let Amalgamate { name_1, name_2 } = params;
                write!(f, "3,{},{}", name_1, name_2)
            }
            SmallBankTransactionProfile::WriteCheck(params) => {
                let WriteCheck { name, value } = params;
                write!(f, "4,{},{}", name, value)
            }
            SmallBankTransactionProfile::SendPayment(params) => {
                let SendPayment {
                    name_1,
                    name_2,
                    amount,
                } = params;
                write!(f, "5,{},{},{}", name_1, name_2, amount)
            }
        }
    }
}
