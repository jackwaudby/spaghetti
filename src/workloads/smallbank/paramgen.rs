use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::smallbank::*;
use serde::{Deserialize, Serialize};
use std::fmt;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tracing::debug;

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
        let contention = match sf {
            0 => "NA",
            1 => "high",
            2 => "mid",
            3 => "low",
            _ => panic!("invalid scale factor"),
        };

        debug!("Parameter generator set seed: {}", set_seed);
        debug!("Balance mix: {}", use_balance_mix);
        debug!("Contention: {}", contention);

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
            x if x < 0.1667 => {
                let name = self.get_name();
                let payload = Balance { name };

                (
                    SmallBankTransaction::Balance,
                    SmallBankTransactionProfile::Balance(payload),
                )
            }
            // DEPOSIT_CHECKING
            x if x < 0.3333 => {
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
            x if x < 0.50 => {
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
            x if x < 0.6667 => {
                let (name1, name2) = self.get_names();

                let payload = Amalgamate { name1, name2 };
                (
                    SmallBankTransaction::Amalgamate,
                    SmallBankTransactionProfile::Amalgamate(payload),
                )
            }
            // WRITE_CHECK
            x if x < 0.8333 => {
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
    pub fn get_name(&mut self) -> String {
        if self.accounts == 100 {
            let id = self.rng.gen_range(0..self.accounts);
            return format!("cust{}", id);
        }

        let n: f32 = self.rng.gen();
        let id = match n {
            x if x < 0.25 => self.rng.gen_range(0..100),

            _ => self.rng.gen_range(100..self.accounts),
        };
        format!("cust{}", id)
    }

    /// Get distinct customer names.
    pub fn get_names(&mut self) -> (String, String) {
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
    pub name: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DepositChecking {
    pub name: String,
    pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TransactSaving {
    pub name: String,
    pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Amalgamate {
    pub name1: String,
    pub name2: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct WriteCheck {
    pub name: String,
    pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SendPayment {
    pub name1: String,
    pub name2: String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config;

    #[test]
    fn generate_test() {
        let mut c = Config::default();
        c.merge(config::File::with_name("./tests/Test-smallbank.toml"))
            .unwrap();
        let mut gen = SmallBankGenerator::new(1, true, Some(1), true);
        assert_eq!(
            (
                SmallBankTransaction::Balance,
                SmallBankTransactionProfile::Balance(Balance {
                    name: "cust82".to_string()
                })
            ),
            gen.get_params(0.1)
        );
    }
}
