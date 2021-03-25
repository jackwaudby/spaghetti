use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::smallbank::profiles::*;
use crate::workloads::smallbank::SmallBankTransaction;

use config::Config;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

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

    /// Use hotspot fixed size.
    hotspot_use_fixed_size: bool,

    /// Hotspot fixed size.
    hotspot_fixed_size: usize,

    /// Hotspot percentage.
    hotspot_percentage: f64,
}

impl SmallBankGenerator {
    /// Create new `SmallBankGenerator`.
    pub fn new(config: Arc<Config>) -> SmallBankGenerator {
        // Initialise rng.
        let set_seed = config.get_bool("set_seed").unwrap();
        let seed = config.get_int("seed").unwrap() as u64;
        let rng: StdRng;
        if set_seed {
            rng = SeedableRng::seed_from_u64(seed);
        } else {
            rng = SeedableRng::from_entropy();
        }
        // Params from config.
        let accounts = config.get_int("accounts").unwrap() as u32;
        let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
        let send_payment_amount = config.get_float("send_payment_amount").unwrap();
        let deposit_checking_amount = config.get_float("deposit_checking_amount").unwrap();
        let transact_savings_amount = config.get_float("transact_savings_amount").unwrap();
        let write_check_amount = config.get_float("write_check_amount").unwrap();
        let hotspot_use_fixed_size = config.get_bool("hotspot_use_fixed_size").unwrap();
        let hotspot_fixed_size = config.get_int("hotspot_fixed_size").unwrap() as usize;
        let hotspot_percentage = config.get_float("hotspot_percentage").unwrap();

        SmallBankGenerator {
            rng,
            generated: 0,
            accounts,
            use_balance_mix,
            send_payment_amount,
            deposit_checking_amount,
            transact_savings_amount,
            write_check_amount,
            hotspot_use_fixed_size,
            hotspot_fixed_size,
            hotspot_percentage,
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
        // Increment generated.
        self.generated += 1;
        // Use desired mix.
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

    /// Split account range into cold and hot.
    ///
    /// Sets the first n accounts as the hotspot.
    fn split_accounts(&self, hotspot_size: usize) -> (Vec<String>, Vec<String>) {
        assert!(self.accounts as usize > hotspot_size);
        let mut hot = vec![];
        let mut cold = vec![];

        for account in 1..=hotspot_size {
            hot.push(format!("cust{}", account));
        }

        for account in hotspot_size + 1..=self.accounts as usize {
            cold.push(format!("cust{}", account));
        }

        (hot, cold)
    }

    /// Calculate size of hotspot.
    pub fn get_hotspot_size(&self) -> usize {
        if self.hotspot_use_fixed_size {
            self.hotspot_fixed_size
        } else {
            (self.accounts as f64 * self.hotspot_percentage) as usize
        }
    }

    /// Get customer name.
    pub fn get_name(&mut self) -> String {
        let hotspot_size = self.get_hotspot_size();
        let (hot, cold) = self.split_accounts(hotspot_size);

        let n: f32 = self.rng.gen();
        match n {
            // Choose from hot.
            x if x < 0.9 => {
                let ind = self.rng.gen_range(0..hotspot_size);
                hot[ind].clone()
            }
            // Choose from cold.
            _ => {
                let cold_size = cold.len();
                let ind = self.rng.gen_range(0..cold_size);
                cold[ind].clone()
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_test() {
        let mut c = Config::default();
        c.merge(config::File::with_name("Test-smallbank.toml"))
            .unwrap();
        let mut gen = SmallBankGenerator::new(Arc::new(c));
        assert_eq!(
            (
                SmallBankTransaction::Balance,
                SmallBankTransactionProfile::Balance(Balance {
                    name: "cust1".to_string()
                })
            ),
            gen.get_params(0.1)
        );
    }
}
