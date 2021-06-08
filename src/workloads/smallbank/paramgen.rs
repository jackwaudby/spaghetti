use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::smallbank::*;
use serde::{Deserialize, Serialize};
use std::fmt;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// SmallBank workload transaction generator.
pub struct SmallBankGenerator {
    /// Random number generator.
    rng: StdRng,

    /// Number of transactions generated.
    pub generated: u32,

    /// Number of accounts.
    accounts: u32,

    /// Flag if read-heavy workload should be used.
    use_balance_mix: bool,

    /// Amount that is sent between accounts.
    send_payment_amount: f64,

    /// Amount that is deposited in the checking account.
    deposit_checking_amount: f64,

    /// Amount that is deposited in the savings account.
    transact_savings_amount: f64,

    /// Value of a cheque written against an account.
    write_check_amount: f64,
}

impl SmallBankGenerator {
    /// Create new `SmallBankGenerator`.
    pub fn new(
        sf: u64,
        set_seed: bool,
        seed: Option<u64>,
        use_balance_mix: bool,
    ) -> SmallBankGenerator {
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
    /// Generate a transaction request.
    fn generate(&mut self) -> Message {
        let n: f32 = self.rng.gen();
        let (transaction, parameters) = self.get_params(n);

        Message::Request {
            request_no: self.generated,
            transaction: Transaction::SmallBank(transaction),
            parameters: Parameters::SmallBank(parameters),
        }
    }

    /// Get number of transactions generated.
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
            //            _ => {
            x if x < 0.15 => {
                let name = self.get_name();
                let payload = Balance { name };

                (
                    SmallBankTransaction::Balance,
                    SmallBankTransactionProfile::Balance(payload),
                )
            } // // DEPOSIT_CHECKING
            x if x < 0.30 => {
                let name = self.get_name();

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
                let name = self.get_name();

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
            x if x < 0.46 => {
                let (name1, name2) = self.get_names();

                let payload = Amalgamate { name1, name2 };
                (
                    SmallBankTransaction::Amalgamate,
                    SmallBankTransactionProfile::Amalgamate(payload),
                )
            }
            // WRITE_CHECK
            x if x < 0.75 => {
                let name = self.get_name();

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
                let (name1, name2) = self.get_names();

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
                let name = self.get_name();

                let payload = Balance { name };

                (
                    SmallBankTransaction::Balance,
                    SmallBankTransactionProfile::Balance(payload),
                )
            }
            // DEPOSIT_CHECKING
            x if x < 0.68 => {
                let name = self.get_name();

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
                let name = self.get_name();

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
                let (name1, name2) = self.get_names();

                let payload = Amalgamate { name1, name2 };
                (
                    SmallBankTransaction::Amalgamate,
                    SmallBankTransactionProfile::Amalgamate(payload),
                )
            }
            // WRITE_CHECK
            x if x < 0.92 => {
                let name = self.get_name();

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
                let (name1, name2) = self.get_names();

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

    /// Get customer name.
    pub fn get_name(&mut self) -> u64 {
        if self.accounts == 10 {
            let id: u64 = self.rng.gen_range(0..self.accounts).into();
            return id;
        }

        if self.accounts == 100 {
            let id: u64 = self.rng.gen_range(0..self.accounts).into();
            return id;
        }

        let n: f32 = self.rng.gen();
        match n {
            x if x < 0.25 => {
                let id = self.rng.gen_range(0..100) as u32;
                id as u64
            }
            _ => {
                let id: u64 = self.rng.gen_range(100..self.accounts).into();
                id
            }
        }
    }

    /// Get distinct customer names.
    pub fn get_names(&mut self) -> (u64, u64) {
        let name1 = self.get_name();
        let mut name2 = self.get_name();

        while name1 == name2 {
            name2 = self.get_name();
        }
        (name1, name2)
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
