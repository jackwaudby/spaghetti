use crate::common::message::{Message, Parameters, Transaction};
use crate::common::parameter_generation::Generator;
use crate::workloads::smallbank::helper;
use crate::workloads::smallbank::profiles::*;
use crate::workloads::smallbank::SmallBankTransaction;

use config::Config;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

/// SmallBank workload transaction generator.
pub struct SmallBankGenerator {
    /// Rng.
    rng: StdRng,

    /// Number of transactions generated.
    pub generated: u32,

    /// Configuration.
    config: Arc<Config>,
}

impl SmallBankGenerator {
    /// Create new `SmallBankGenerator`.
    pub fn new(config: Arc<Config>) -> SmallBankGenerator {
        let set_seed = config.get_bool("set_seed").unwrap();

        let rng: StdRng;
        if set_seed {
            rng = SeedableRng::seed_from_u64(1);
        } else {
            rng = SeedableRng::from_entropy();
        }
        SmallBankGenerator {
            rng,
            generated: 0,
            config,
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
        self.generated += 1;
        let config = Arc::clone(&self.config);
        match n {
            // BALANCE
            x if x < 0.1667 => {
                let name = helper::get_name(&mut self.rng, config);

                let payload = Balance { name };

                (
                    SmallBankTransaction::Balance,
                    SmallBankTransactionProfile::Balance(payload),
                )
            }
            // DEPOSIT_CHECKING
            x if x < 0.3333 => {
                let name = helper::get_name(&mut self.rng, config);
                let value = self.config.get_float("deposit_checking_amount").unwrap();

                let payload = DepositChecking { name, value };
                (
                    SmallBankTransaction::DepositChecking,
                    SmallBankTransactionProfile::DepositChecking(payload),
                )
            }
            // TRANSACT_SAVING
            x if x < 0.50 => {
                let name = helper::get_name(&mut self.rng, config);
                let value = self.config.get_float("transact_savings_amount").unwrap();

                let payload = TransactSaving { name, value };
                (
                    SmallBankTransaction::TransactSaving,
                    SmallBankTransactionProfile::TransactSaving(payload),
                )
            }
            // AMALGAMATE
            x if x < 0.6667 => {
                let (name1, name2) = helper::get_names(&mut self.rng, config);

                let payload = Amalgamate { name1, name2 };
                (
                    SmallBankTransaction::Amalgamate,
                    SmallBankTransactionProfile::Amalgamate(payload),
                )
            }
            // WRITE_CHECK
            x if x < 0.8333 => {
                let name = helper::get_name(&mut self.rng, config);
                let value = self.config.get_float("write_check_amount").unwrap();

                let payload = WriteCheck { name, value };
                (
                    SmallBankTransaction::WriteCheck,
                    SmallBankTransactionProfile::WriteCheck(payload),
                )
            }
            // SEND_PAYMENT
            _ => {
                let (name1, name2) = helper::get_names(&mut self.rng, config);
                let value = self.config.get_float("send_payment_amount").unwrap();

                let payload = SendPayment {
                    name1,
                    name2,
                    value,
                };
                (
                    SmallBankTransaction::SendPayment,
                    SmallBankTransactionProfile::SendPayment(payload),
                )
            }
        }
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
