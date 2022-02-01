use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::smallbank::*;
use crate::workloads::IsolationLevel;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt;

pub struct SmallBankGenerator {
    thread_id: u32,
    rng: StdRng,
    pub generated: u32,
    accounts: u32,
    use_balance_mix: bool,
    send_payment_amount: f64,
    deposit_checking_amount: f64,
    transact_savings_amount: f64,
    write_check_amount: f64,
}

impl SmallBankGenerator {
    pub fn new(
        thread_id: u32,
        sf: u64,
        set_seed: bool,
        seed: Option<u64>,
        use_balance_mix: bool,
    ) -> Self {
        let rng: StdRng;

        if set_seed {
            rng = SeedableRng::seed_from_u64(seed.unwrap());
        } else {
            rng = SeedableRng::from_entropy();
        }

        let accounts = *SB_SF_MAP.get(&sf).unwrap() as u32;
        let send_payment_amount = SEND_PAYMENT_AMOUNT;
        let deposit_checking_amount = DEPOSIT_CHECKING_AMOUNT;
        let transact_savings_amount = TRANSACT_SAVINGS_AMOUNT;
        let write_check_amount = WRITE_CHECK_AMOUNT;

        SmallBankGenerator {
            thread_id,
            rng,
            generated: 0,
            accounts,
            use_balance_mix,
            send_payment_amount,
            deposit_checking_amount,
            transact_savings_amount,
            write_check_amount,
        }
    }
}

impl Generator for SmallBankGenerator {
    fn generate(&mut self) -> Message {
        let n: f32 = self.rng.gen();
        let (transaction, parameters) = self.get_params(n);

        let m: f32 = self.rng.gen();
        let isolation = match m {
            x if x < 0.40 => IsolationLevel::ReadUncommitted,
            x if x < 0.99 => IsolationLevel::ReadCommitted,
            _ => IsolationLevel::Serializable,
        };

        Message::Request {
            request_no: (self.thread_id, self.generated),
            transaction: Transaction::SmallBank(transaction),
            parameters: Parameters::SmallBank(parameters),
            isolation,
        }
    }

    fn get_generated(&self) -> u32 {
        self.generated
    }
}

impl SmallBankGenerator {
    /// Get a random transaction profile (type, params)
    fn get_params(&mut self, n: f32) -> (SmallBankTransaction, SmallBankTransactionProfile) {
        self.generated += 1; // increment generated

        // use desired mix
        if self.use_balance_mix {
            self.balance_mix(n)
        } else {
            self.uniform_mix(n)
        }
    }

    /// Uniform transaction mix .
    fn uniform_mix(&mut self, n: f32) -> (SmallBankTransaction, SmallBankTransactionProfile) {
        match n {
            // BALANCE
            x if x < 0.15 => {
                let name = self.get_account();
                let payload = Balance { name };

                (
                    SmallBankTransaction::Balance,
                    SmallBankTransactionProfile::Balance(payload),
                )
            } // // DEPOSIT_CHECKING
            x if x < 0.30 => {
                let name = self.get_account();

                let payload = DepositChecking {
                    name,
                    value: self.deposit_checking_amount,
                };
                (
                    SmallBankTransaction::DepositChecking,
                    SmallBankTransactionProfile::DepositChecking(payload),
                )
            }
            // TRANSACT_SAVING
            x if x < 0.45 => {
                let name = self.get_account();

                let payload = TransactSaving {
                    name,
                    value: self.transact_savings_amount,
                };
                (
                    SmallBankTransaction::TransactSaving,
                    SmallBankTransactionProfile::TransactSaving(payload),
                )
            }
            // AMALGAMATE
            x if x < 0.60 => {
                let (name1, name2) = self.get_accounts();

                let payload = Amalgamate { name1, name2 };
                (
                    SmallBankTransaction::Amalgamate,
                    SmallBankTransactionProfile::Amalgamate(payload),
                )
            }
            // WRITE_CHECK
            x if x < 0.75 => {
                let name = self.get_account();

                let payload = WriteCheck {
                    name,
                    value: self.write_check_amount,
                };
                (
                    SmallBankTransaction::WriteCheck,
                    SmallBankTransactionProfile::WriteCheck(payload),
                )
            }
            // SEND_PAYMENT
            _ => {
                let (name1, name2) = self.get_accounts();

                let payload = SendPayment {
                    name1,
                    name2,
                    value: self.send_payment_amount,
                };
                (
                    SmallBankTransaction::SendPayment,
                    SmallBankTransactionProfile::SendPayment(payload),
                )
            }
        }
    }

    /// Transaction mix with 60% Balance transactions.
    fn balance_mix(&mut self, n: f32) -> (SmallBankTransaction, SmallBankTransactionProfile) {
        match n {
            // BALANCE
            x if x < 0.6 => {
                let name = self.get_account();

                let payload = Balance { name };

                (
                    SmallBankTransaction::Balance,
                    SmallBankTransactionProfile::Balance(payload),
                )
            }
            // DEPOSIT_CHECKING
            x if x < 0.68 => {
                let name = self.get_account();

                let payload = DepositChecking {
                    name,
                    value: self.deposit_checking_amount,
                };
                (
                    SmallBankTransaction::DepositChecking,
                    SmallBankTransactionProfile::DepositChecking(payload),
                )
            }
            // TRANSACT_SAVING
            x if x < 0.76 => {
                let name = self.get_account();

                let payload = TransactSaving {
                    name,
                    value: self.transact_savings_amount,
                };
                (
                    SmallBankTransaction::TransactSaving,
                    SmallBankTransactionProfile::TransactSaving(payload),
                )
            }
            // AMALGAMATE
            x if x < 0.84 => {
                let (name1, name2) = self.get_accounts();

                let payload = Amalgamate { name1, name2 };
                (
                    SmallBankTransaction::Amalgamate,
                    SmallBankTransactionProfile::Amalgamate(payload),
                )
            }
            // WRITE_CHECK
            x if x < 0.92 => {
                let name = self.get_account();

                let payload = WriteCheck {
                    name,
                    value: self.write_check_amount,
                };
                (
                    SmallBankTransaction::WriteCheck,
                    SmallBankTransactionProfile::WriteCheck(payload),
                )
            }
            // SEND_PAYMENT
            _ => {
                let (name1, name2) = self.get_accounts();

                let payload = SendPayment {
                    name1,
                    name2,
                    value: self.send_payment_amount,
                };
                (
                    SmallBankTransaction::SendPayment,
                    SmallBankTransactionProfile::SendPayment(payload),
                )
            }
        }
    }

    pub fn get_account(&mut self) -> u64 {
        if self.accounts <= 100 {
            self.rng.gen_range(0..self.accounts as u64)
        } else {
            let n: f32 = self.rng.gen();
            match n {
                x if x < 0.25 => self.rng.gen_range(0..100 as u64),
                _ => self.rng.gen_range(100..self.accounts as u64),
            }
        }
    }

    pub fn get_accounts(&mut self) -> (u64, u64) {
        let acc1 = self.get_account();
        let mut acc2 = self.get_account();

        while acc1 == acc2 {
            acc2 = self.get_account();
        }
        (acc1, acc2)
    }
}

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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Balance {
    pub name: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DepositChecking {
    pub name: u64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TransactSaving {
    pub name: u64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Amalgamate {
    pub name1: u64,
    pub name2: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct WriteCheck {
    pub name: u64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SendPayment {
    pub name1: u64,
    pub name2: u64,
    pub value: f64,
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
                let Amalgamate { name1, name2 } = params;
                write!(f, "3,{},{}", name1, name2)
            }
            SmallBankTransactionProfile::WriteCheck(params) => {
                let WriteCheck { name, value } = params;
                write!(f, "4,{},{}", name, value)
            }
            SmallBankTransactionProfile::SendPayment(params) => {
                let SendPayment {
                    name1,
                    name2,
                    value,
                } = params;
                write!(f, "5,{},{},{}", name1, name2, value)
            }
        }
    }
}
